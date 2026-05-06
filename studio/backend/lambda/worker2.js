'use strict';

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const REGION = process.env.AWS_REGION || process.env.REGION || 'us-east-1';
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET || '';
const OUTPUT_PREFIX = process.env.OUTPUT_PREFIX || 'trackster-output';

const s3 = new S3Client({ region: REGION });

const OUTPUT_FORMATS = new Set(['BIN', 'JSON', 'CSV']);

exports.handler = async function handler(event) {
  const records = Array.isArray(event?.Records) ? event.Records : [{ body: JSON.stringify(event) }];
  const results = [];

  for (const record of records) {
    const message = parseSqsBody(record.body);
    const result = await processVehicleMessage(message);
    results.push(result);
  }

  return {
    ok: true,
    processed: results.length,
    results
  };
};

async function processVehicleMessage(message) {
  const normalized = normalizeMessage(message);

  const frames = generateFrames({
    compiledDbcs: normalized.compiledDbcs,
    selectedCanIds: normalized.selectedCanIds,
    blockCount: normalized.blockCount,
    vehicleId: normalized.vehicleId,
    startTimestampMs: normalized.startTimestampMs,
    frameIntervalMs: normalized.frameIntervalMs
  });

  const outputFormat = normalized.outputFormat;

  let body;
  let contentType;
  let extension;

  if (outputFormat === 'JSON') {
    body = Buffer.from(JSON.stringify({ frames }, null, 2), 'utf8');
    contentType = 'application/json';
    extension = 'json';
  } else if (outputFormat === 'CSV') {
    body = Buffer.from(toCsv(frames), 'utf8');
    contentType = 'text/csv';
    extension = 'csv';
  } else {
    body = toTracksterBin(frames);
    contentType = 'application/octet-stream';
    extension = 'bin';
  }

  const outputKey = buildOutputKey(normalized, extension);
  let localFilePath = null;

  if (!OUTPUT_BUCKET) {
    const fs = require('fs');
    const path = require('path');

    const localDir = path.join(process.cwd(), 'output');

    if (!fs.existsSync(localDir)) {
      fs.mkdirSync(localDir, { recursive: true });
    }

    localFilePath = path.join(localDir, `${normalized.vehicleId}.${extension}`);
    fs.writeFileSync(localFilePath, body);

    console.log(`Local output saved: ${localFilePath}`);
  } else {
    await s3.send(new PutObjectCommand({
      Bucket: OUTPUT_BUCKET,
      Key: outputKey,
      Body: body,
      ContentType: contentType
    }));
  }

  return {
    ok: true,
    jobId: normalized.jobId,
    vehicleId: normalized.vehicleId,
    outputFormat,
    frameCount: frames.length,
    outputBucket: OUTPUT_BUCKET || null,
    outputKey: OUTPUT_BUCKET ? outputKey : null,
    localFilePath,
    localBytes: OUTPUT_BUCKET ? undefined : body.length
  };
}

function normalizeMessage(message) {
  const outputFormat = String(message.outputFormat || message.format || 'BIN').toUpperCase();

  if (!OUTPUT_FORMATS.has(outputFormat)) {
    throw new Error(`Invalid output format: ${outputFormat}`);
  }

  const compiledDbcs =
    message.compiledDbcs ||
    message.runtimeCompiledDbcs ||
    message.dbcs ||
    (message.compiledDbc ? [message.compiledDbc] : null) ||
    (message.runtimeCompiledDbc ? [message.runtimeCompiledDbc] : null);

  if (!Array.isArray(compiledDbcs) || compiledDbcs.length === 0) {
    throw new Error('Missing compiledDbcs/runtimeCompiledDbcs/compiledDbc in SQS message.');
  }

  for (const dbc of compiledDbcs) {
    validateCompiledDbc(dbc);
  }

  const selectedCanIds = normalizeSelectedCanIds(message.selectedCanIds || message.canIds || []);

  return {
    jobId: String(message.jobId || message.requestId || `job-${Date.now()}`),
    vehicleId: String(message.vehicleId || message.vehicle || 'vehicle-000001'),
    vehicleIndex: Number.isFinite(Number(message.vehicleIndex)) ? Number(message.vehicleIndex) : 0,
    outputFormat,
    compiledDbcs,
    selectedCanIds,
    blockCount: positiveInteger(message.blockCount || message.blocks || message.totalBlocks || 1, 1),
    startTimestampMs: positiveInteger(message.startTimestampMs || Date.now(), Date.now()),
    frameIntervalMs: positiveInteger(message.frameIntervalMs || 10, 10)
  };
}

function validateCompiledDbc(dbc) {
  if (!dbc || typeof dbc !== 'object') {
    throw new Error('Invalid compiled DBC object.');
  }

  if (dbc.v !== 1) {
    throw new Error(`Unsupported compiled DBC version: ${dbc.v}`);
  }

  if (dbc.st && dbc.st !== 'validated') {
    throw new Error(`Compiled DBC is not validated. Current status: ${dbc.st}`);
  }

  if (!dbc.m || typeof dbc.m !== 'object') {
    throw new Error('Compiled DBC is missing message map "m".');
  }
}

function generateFrames(options) {
  const {
    compiledDbcs,
    selectedCanIds,
    blockCount,
    vehicleId,
    startTimestampMs,
    frameIntervalMs
  } = options;

  const selectedSet = selectedCanIds.length > 0 ? new Set(selectedCanIds) : null;
  const frames = [];
  let sequence = 0;

  for (let blockIndex = 0; blockIndex < blockCount; blockIndex++) {
    for (const dbc of compiledDbcs) {
      for (const [canIdHex, message] of Object.entries(dbc.m)) {
        const normalizedCanId = normalizeCanId(canIdHex);

        if (selectedSet && !selectedSet.has(normalizedCanId)) {
          continue;
        }

        const dlc = positiveInteger(message.l || 8, 8);
        const signals = Array.isArray(message.s) ? message.s : [];
        const payload = buildPayload({
          dlc,
          signals,
          blockIndex,
          sequence,
          vehicleId,
          canIdHex: normalizedCanId
        });

        frames.push({
          timestampMs: startTimestampMs + sequence * frameIntervalMs,
          blockIndex,
          sequence,
          vehicleId,
          canId: normalizedCanId,
          dlc,
          data: Array.from(payload)
        });

        sequence++;
      }
    }
  }

  return frames;
}

function buildPayload({ dlc, signals, blockIndex, sequence, vehicleId, canIdHex }) {
  const payload = Buffer.alloc(dlc, 0);

  for (let signalIndex = 0; signalIndex < signals.length; signalIndex++) {
    const signal = signals[signalIndex];

    const startBit = Number(signal[0]);
    const bitLength = Number(signal[1]);
    const byteOrder = Number(signal[2]);
    const signed = Number(signal[3]) === 1;
    const factor = Number(signal[4]);
    const offset = Number(signal[5]);
    const min = Number(signal[6]);
    const max = Number(signal[7]);

    if (!Number.isFinite(startBit) || !Number.isFinite(bitLength) || bitLength <= 0) {
      continue;
    }

    const physicalValue = generatePhysicalValue({
      min,
      max,
      factor,
      offset,
      blockIndex,
      sequence,
      signalIndex,
      vehicleId,
      canIdHex
    });

    const rawValue = physicalToRaw({
      physicalValue,
      factor,
      offset,
      bitLength,
      signed
    });

    packSignal({
      payload,
      rawValue,
      startBit,
      bitLength,
      byteOrder,
      signed
    });
  }

  return payload;
}

function generatePhysicalValue(input) {
  const {
    min,
    max,
    factor,
    offset,
    blockIndex,
    sequence,
    signalIndex,
    vehicleId,
    canIdHex
  } = input;

  if (!Number.isFinite(min) || !Number.isFinite(max) || min === max) {
    return Number.isFinite(offset) ? offset : 0;
  }

  const seed = hashString(`${vehicleId}|${canIdHex}|${signalIndex}`);
  const phase = (seed % 1000) / 1000;
  const wave = (Math.sin((blockIndex + sequence * 0.05 + phase * 10) * 0.15) + 1) / 2;

  const value = min + (max - min) * wave;

  if (Number.isFinite(factor) && factor !== 0) {
    return Math.round(value / factor) * factor;
  }

  return value;
}

function physicalToRaw({ physicalValue, factor, offset, bitLength, signed }) {
  const safeFactor = Number.isFinite(factor) && factor !== 0 ? factor : 1;
  const safeOffset = Number.isFinite(offset) ? offset : 0;

  let raw = Math.round((physicalValue - safeOffset) / safeFactor);

  const minRaw = signed ? -(2 ** (bitLength - 1)) : 0;
  const maxRaw = signed ? (2 ** (bitLength - 1)) - 1 : (2 ** bitLength) - 1;

  raw = Math.max(minRaw, Math.min(maxRaw, raw));

  return BigInt(raw);
}

function packSignal({ payload, rawValue, startBit, bitLength, byteOrder, signed }) {
  const maxBits = BigInt(bitLength);
  let value = rawValue;

  if (signed && rawValue < 0n) {
    value = (1n << maxBits) + rawValue;
  }

  const littleEndian = byteOrder === 1;

  if (littleEndian) {
    packLittleEndian(payload, value, startBit, bitLength);
  } else {
    packBigEndianMotorola(payload, value, startBit, bitLength);
  }
}

function packLittleEndian(payload, value, startBit, bitLength) {
  for (let i = 0; i < bitLength; i++) {
    const bitValue = Number((value >> BigInt(i)) & 1n);
    setPayloadBit(payload, startBit + i, bitValue);
  }
}

function packBigEndianMotorola(payload, value, startBit, bitLength) {
  let bitPosition = startBit;

  for (let i = 0; i < bitLength; i++) {
    const rawBitIndex = bitLength - 1 - i;
    const bitValue = Number((value >> BigInt(rawBitIndex)) & 1n);

    setPayloadBit(payload, bitPosition, bitValue);

    if (bitPosition % 8 === 0) {
      bitPosition += 15;
    } else {
      bitPosition -= 1;
    }
  }
}

function setPayloadBit(payload, bitIndex, bitValue) {
  const byteIndex = Math.floor(bitIndex / 8);
  const bitInByte = bitIndex % 8;

  if (byteIndex < 0 || byteIndex >= payload.length) {
    return;
  }

  const mask = 1 << bitInByte;

  if (bitValue) {
    payload[byteIndex] |= mask;
  } else {
    payload[byteIndex] &= ~mask;
  }
}

function toCsv(frames) {
  const lines = ['timestampMs,blockIndex,sequence,vehicleId,canId,dlc,data'];

  for (const frame of frames) {
    lines.push([
      frame.timestampMs,
      frame.blockIndex,
      frame.sequence,
      frame.vehicleId,
      frame.canId,
      frame.dlc,
      frame.data.map(byte => byte.toString(16).padStart(2, '0')).join(' ')
    ].join(','));
  }

  return `${lines.join('\n')}\n`;
}

function toTracksterBin(frames) {
  const headerSize = 40;
  const recordSize = 28;
  const buffer = Buffer.alloc(headerSize + frames.length * recordSize, 0);

  buffer.write('TSTR', 0, 4, 'ascii');
  buffer.writeUInt16LE(1, 4);
  buffer.writeUInt16LE(1, 6);
  buffer.writeUInt32LE(frames.length, 8);
  buffer.writeBigUInt64LE(BigInt(Date.now()), 12);

  let offset = headerSize;

  for (const frame of frames) {
    const canIdNumber = parseInt(frame.canId, 16);

    buffer.writeUInt32LE(0, offset);
    buffer.writeUInt32LE(frame.blockIndex, offset + 4);
    buffer.writeUInt32LE(frame.timestampMs % 0xffffffff, offset + 8);
    buffer.writeUInt32LE(canIdNumber, offset + 12);
    buffer.writeUInt8(frame.dlc, offset + 16);

    for (let i = 0; i < 8; i++) {
      buffer.writeUInt8(frame.data[i] || 0, offset + 17 + i);
    }

    offset += recordSize;
  }

  return buffer;
}

function parseSqsBody(body) {
  if (typeof body === 'object') {
    return body;
  }

  try {
    return JSON.parse(body);
  } catch (error) {
    throw new Error(`Invalid SQS JSON body: ${error.message}`);
  }
}

function buildOutputKey(normalized, extension) {
  const safeJobId = sanitizeKeyPart(normalized.jobId);
  const safeVehicleId = sanitizeKeyPart(normalized.vehicleId);

  return `${OUTPUT_PREFIX}/${safeJobId}/${safeVehicleId}.${extension}`;
}

function normalizeSelectedCanIds(canIds) {
  if (!Array.isArray(canIds)) {
    return [];
  }

  return canIds.map(normalizeCanId);
}

function normalizeCanId(value) {
  if (typeof value === 'number') {
    return `0x${value.toString(16)}`;
  }

  const text = String(value).trim().toLowerCase();

  if (text.startsWith('0x')) {
    return text;
  }

  const asNumber = Number(text);

  if (Number.isFinite(asNumber)) {
    return `0x${asNumber.toString(16)}`;
  }

  return text;
}

function positiveInteger(value, fallback) {
  const number = Number(value);

  if (!Number.isFinite(number) || number <= 0) {
    return fallback;
  }

  return Math.floor(number);
}

function sanitizeKeyPart(value) {
  return String(value)
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .slice(0, 120);
}

function hashString(value) {
  let hash = 2166136261;

  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }

  return hash >>> 0;
}

if (require.main === module) {
  const fs = require('fs');
  const path = process.argv[2];

  if (!path) {
    console.error('Usage: node worker.js ./local-event.json');
    process.exit(1);
  }

  const event = JSON.parse(fs.readFileSync(path, 'utf8'));

  exports.handler(event)
    .then(result => {
      console.log(JSON.stringify(result, null, 2));
    })
    .catch(error => {
      console.error(error);
      process.exit(1);
    });
}