'use strict';

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const REGION = process.env.AWS_REGION || process.env.REGION || 'us-east-1';
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET || '';

const s3 = new S3Client({ region: REGION });

const GLOBAL_HEADER_SIZE = 40;
const BLOCK_HEADER_SIZE = 32;
const FRAME_FIXED_HEADER_SIZE = 11;

const TRACKSTER_MAGIC = 'TRKS';
const BLOCK_MAGIC = 'BLK1';
const FORMAT_VERSION = 1;

const DEFAULT_BUS = 0;
const DEFAULT_BLOCK_INTERVAL_NS = 10_000_000;

exports.handler = async function handler(event) {
  const records = Array.isArray(event?.Records)
    ? event.Records
    : [{ body: JSON.stringify(event) }];

  const results = [];

  for (const record of records) {
    try {
      const message = parseSqsBody(record.body);
      const result = await processMessage(message);
      results.push(result);
    } catch (error) {
      console.error('[WORKER] Failed to process record.', {
        errorName: error?.name,
        errorMessage: error?.message,
        stack: error?.stack,
      });

      results.push({
        ok: false,
        error: error?.message || String(error),
      });
    }
  }

  return {
    ok: results.every((item) => item.ok),
    results,
  };
};

function parseSqsBody(body) {
  if (typeof body === 'object' && body !== null) {
    return body;
  }

  if (typeof body !== 'string') {
    throw new Error('Invalid SQS body.');
  }

  const parsed = JSON.parse(body);

  if (typeof parsed?.Message === 'string') {
    return JSON.parse(parsed.Message);
  }

  return parsed;
}

async function processMessage(message) {
  const outputBucket = getOutputBucket(message);
  const outputKey = buildOutputKey(message);

  const runtime = extractRuntime(message);
  const blocks = buildSimulationBlocks(message, runtime);
  const binBuffer = buildTracksterBin(blocks, message.blocksSize ?? message.blocks_size);

  await s3.send(new PutObjectCommand({
    Bucket: outputBucket,
    Key: outputKey,
    Body: binBuffer,
    ContentType: 'application/octet-stream',
    Metadata: {
      format: 'trackster-bin',
      version: String(FORMAT_VERSION),
      customerid: String(message.customerId || ''),
      runid: String(message.runId || ''),
      vin: String(message.vin || ''),
      blocks: String(blocks.length),
      frames: String(countFrames(blocks)),
    },
  }));

  console.log('[WORKER] BIN written.', {
    bucket: outputBucket,
    key: outputKey,
    bytes: binBuffer.length,
    blocks: blocks.length,
    frames: countFrames(blocks),
  });

  return {
    ok: true,
    bucket: outputBucket,
    key: outputKey,
    bytes: binBuffer.length,
    blocks: blocks.length,
    frames: countFrames(blocks),
  };
}

function getOutputBucket(message) {
  const bucket =
    message.s3Bucket ||
    message.outputBucket ||
    message.bucket ||
    message.output?.bucket ||
    OUTPUT_BUCKET;

  if (!bucket) {
    throw new Error('Missing output bucket. Expected message.s3Bucket or OUTPUT_BUCKET.');
  }

  return String(bucket).trim();
}

function buildOutputKey(message) {
  const customerId = getRequiredString(message.customerId, 'customerId');
  const runId = getRequiredString(message.runId, 'runId');
  const vin = getRequiredString(message.vin, 'vin');

  const extension = normalizeOutputExtension(message.outputFormat);
  const runFolder = normalizeRunFolder(runId);

  return [
    sanitizePathPart(customerId),
    runFolder,
    `${sanitizeFileName(vin)}.${extension}`,
  ].join('/');
}

function normalizeRunFolder(runId) {
  const digits = String(runId || '').replace(/\D/g, '');

  if (digits.length >= 14) {
    return digits.slice(0, 14);
  }

  if (digits.length > 0) {
    return digits.padEnd(14, '0');
  }

  throw new Error(`Invalid runId for S3 folder: ${runId}`);
}

function normalizeOutputExtension(outputFormat) {
  const value = String(outputFormat || 'BIN').trim().toLowerCase();

  if (value === 'json') return 'json';
  if (value === 'csv') return 'csv';

  return 'bin';
}

function getRequiredString(value, fieldName) {
  const text = String(value || '').trim();

  if (!text) {
    throw new Error(`${fieldName} is required`);
  }

  return text;
}

function sanitizePathPart(value) {
  return String(value || '')
    .trim()
    .replace(/^\/+|\/+$/g, '')
    .replace(/[^a-zA-Z0-9._=-]/g, '_');
}

function sanitizeFileName(value) {
  return String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '_');
}

function countFrames(blocks) {
  return blocks.reduce((sum, block) => sum + block.frames.length, 0);
}

function extractRuntime(message) {
  const runtime =
    message.compiledDbc ||
    message.compiledDbcRuntime ||
    message.runtimeCompiledDbc ||
    message.dbcRuntime ||
    message.runtime;

  if (!runtime) {
    throw new Error('Missing compiled DBC runtime. Expected message.compiledDbc.');
  }

  const messages = normalizeRuntimeMessages(runtime);

  if (!messages.length) {
    throw new Error('Compiled DBC runtime has no messages.');
  }

  return {
    messages,
  };
}

function normalizeRuntimeMessages(runtime) {
  const result = [];

  if (runtime.m && typeof runtime.m === 'object' && !Array.isArray(runtime.m)) {
    for (const [canIdKey, entries] of Object.entries(runtime.m)) {
      const list = Array.isArray(entries) ? entries : [entries];

      for (const entry of list) {
        result.push(normalizeRuntimeEntry(canIdKey, entry));
      }
    }

    return result.filter(Boolean);
  }

  if (Array.isArray(runtime.messages)) {
    return runtime.messages
      .map((item) => normalizeRuntimeEntry(item?.canId ?? item?.id, item))
      .filter(Boolean);
  }

  if (Array.isArray(runtime.m)) {
    return runtime.m
      .map((item) => normalizeRuntimeEntry(item?.canId ?? item?.id, item))
      .filter(Boolean);
  }

  return [];
}

function normalizeRuntimeEntry(canIdKey, entry) {
  if (!entry) {
    return null;
  }

  const frame = entry.frame || entry;
  const canId = parseCanId(entry.canId ?? frame.canId ?? frame.id ?? frame.address ?? canIdKey);
  const name = String(entry.messageName || frame.messageName || frame.name || frame.n || `MSG_${canId}`);
  const dlc = Number(frame.dlc ?? frame.length ?? frame.l ?? 8);
  const bus = Number(frame.bus ?? frame.src ?? DEFAULT_BUS);

  const rawSignals =
    frame.signals ||
    frame.s ||
    frame.signalList ||
    [];

  const signals = Array.isArray(rawSignals)
    ? rawSignals.map(normalizeSignal).filter(Boolean)
    : [];

  if (!Number.isInteger(canId) || canId < 0) {
    throw new Error(`Invalid CAN ID in runtime message: ${canIdKey}`);
  }

  if (!Number.isInteger(dlc) || dlc < 0 || dlc > 64) {
    throw new Error(`Invalid DLC for ${name}: ${dlc}`);
  }

  return {
    canId,
    name,
    dlc,
    bus,
    signals,
  };
}

function normalizeSignal(raw, index) {
  if (!raw) {
    return null;
  }

  if (Array.isArray(raw)) {
    return {
      name: raw[8] ? String(raw[8]) : `signal_${index}`,
      startBit: Number(raw[0]),
      bitLength: Number(raw[1]),
      byteOrder: Number(raw[2]) === 0 ? 'big' : 'little',
      isSigned: Boolean(raw[3]),
      factor: Number(raw[4] ?? 1),
      offset: Number(raw[5] ?? 0),
      min: raw[6],
      max: raw[7],
    };
  }

  return {
    name: raw.name ? String(raw.name) : `signal_${index}`,
    startBit: Number(raw.startBit ?? raw.sb),
    bitLength: Number(raw.bitLength ?? raw.sizeBits ?? raw.bl),
    byteOrder: normalizeByteOrder(raw.byteOrder ?? raw.endianness ?? raw.bo),
    isSigned: Boolean(raw.isSigned ?? raw.signed ?? raw.sg),
    factor: Number(raw.factor ?? raw.f ?? 1),
    offset: Number(raw.offset ?? raw.o ?? 0),
    min: raw.min ?? raw.minRaw,
    max: raw.max ?? raw.maxRaw,
  };
}

function normalizeByteOrder(value) {
  if (value === 0) return 'big';
  if (value === 1) return 'little';

  const text = String(value || '').toLowerCase();

  if (text.includes('big') || text.includes('motorola')) {
    return 'big';
  }

  return 'little';
}

function parseCanId(value) {
  if (typeof value === 'number') {
    return value;
  }

  const text = String(value || '').trim();

  if (text.startsWith('0x') || text.startsWith('0X')) {
    return parseInt(text, 16);
  }

  return parseInt(text, 10);
}

function buildSimulationBlocks(message, runtime) {
  const sourceBlocks =
    message.blocks ||
    message.simulationBlocks ||
    message.payloadBlocks;

  if (Array.isArray(sourceBlocks) && sourceBlocks.length > 0) {
    return sourceBlocks.map((block, index) => buildBlockFromInputBlock(block, index, runtime));
  }

  const blockCount = Number(
    message.numberOfBlocks ||
    message.blockCount ||
    message.blocksCount ||
    message.totalBlocks ||
    1
  );

  if (!Number.isInteger(blockCount) || blockCount <= 0) {
    throw new Error(`Invalid block count: ${blockCount}`);
  }

  const blocks = [];

  for (let blockIndex = 0; blockIndex < blockCount; blockIndex += 1) {
    blocks.push(buildGeneratedBlock(message, blockIndex, runtime));
  }

  return blocks;
}

function buildBlockFromInputBlock(block, blockIndex, runtime) {
  const timestampNs = toBigIntSafe(
    block.timestampNs ??
    block.timestamp ??
    block.startTimestampNs ??
    blockIndex * DEFAULT_BLOCK_INTERVAL_NS
  );

  const rawFrames =
    block.frames ||
    block.canFrames ||
    block.messages ||
    [];

  const frames = rawFrames.map((frame, frameIndex) => {
    return buildFrameFromInputFrame(frame, frameIndex, runtime);
  });

  return {
    timestampNs,
    frames,
  };
}

function buildGeneratedBlock(message, blockIndex, runtime) {
  const epochMs = Number(message.epochMs || 0);
  const intervalSec = Number(message.intervalSec || 1);

  const timestampNs = epochMs > 0
    ? BigInt(Math.floor((epochMs + blockIndex * intervalSec * 1000) * 1_000_000))
    : BigInt(blockIndex * DEFAULT_BLOCK_INTERVAL_NS);

  const selectedMessages = extractSelectedMessages(message, runtime);

  const frames = selectedMessages.map((dbcMessage, frameIndex) => {
    const signalValues = resolveSignalValues(message, dbcMessage, blockIndex, frameIndex);
    const payload = encodeMessagePayload(dbcMessage, signalValues);

    return {
      timestampDeltaNs: frameIndex,
      bus: dbcMessage.bus,
      flags: dbcMessage.dlc > 8 ? 1 : 0,
      canId: dbcMessage.canId,
      dlc: dbcMessage.dlc,
      payload,
    };
  });

  return {
    timestampNs,
    frames,
  };
}

function extractSelectedMessages(message, runtime) {
  const canFrames = Array.isArray(message.canFrames) ? message.canFrames : [];

  if (canFrames.length > 0) {
    const selected = [];

    for (const item of canFrames) {
      const canId = parseCanId(item.canId ?? item.id ?? item.address ?? item);
      const messageName = item.messageName || item.name;

      const found = findRuntimeMessage(runtime, canId, messageName);

      if (!found) {
        throw new Error(`Selected CAN frame not found in runtime: ${canId} / 0x${canId.toString(16).toUpperCase()}`);
      }

      selected.push(found);
    }

    return selected;
  }

  return runtime.messages;
}

function buildFrameFromInputFrame(frame, frameIndex, runtime) {
  const canId = parseCanId(
    frame.canId ??
    frame.id ??
    frame.address
  );

  const messageName = frame.name || frame.messageName;
  const dbcMessage = findRuntimeMessage(runtime, canId, messageName);

  if (!dbcMessage) {
    throw new Error(`Frame CAN ID not found in runtime: ${canId} / 0x${canId.toString(16).toUpperCase()}`);
  }

  const timestampDeltaNs = Number(
    frame.timestampDeltaNs ??
    frame.deltaNs ??
    frame.t ??
    frameIndex
  );

  const bus = Number(frame.bus ?? frame.src ?? dbcMessage.bus ?? DEFAULT_BUS);

  let payload;

  if (frame.payload || frame.data || frame.dat) {
    payload = normalizePayload(frame.payload ?? frame.data ?? frame.dat);
  } else {
    const values = frame.values || frame.signals || {};
    payload = encodeMessagePayload(dbcMessage, values);
  }

  const dlc = Number(frame.dlc ?? payload.length ?? dbcMessage.dlc);

  if (payload.length !== dlc) {
    throw new Error(
      `Payload length does not match DLC for CAN ID ${canId}. DLC=${dlc}, payload=${payload.length}`
    );
  }

  return {
    timestampDeltaNs,
    bus,
    flags: dlc > 8 ? 1 : 0,
    canId,
    dlc,
    payload,
  };
}

function findRuntimeMessage(runtime, canId, messageName) {
  if (messageName) {
    const exact = runtime.messages.find((msg) => {
      return msg.canId === canId && msg.name === String(messageName);
    });

    if (exact) {
      return exact;
    }
  }

  return runtime.messages.find((msg) => msg.canId === canId) || null;
}

function resolveSignalValues(message, dbcMessage, blockIndex, frameIndex) {
  const values = {};

  const globalValues =
    message.signalValues ||
    message.values ||
    {};

  const byMessage =
    globalValues[dbcMessage.name] ||
    globalValues[String(dbcMessage.canId)] ||
    globalValues[`0x${dbcMessage.canId.toString(16).toUpperCase()}`] ||
    {};

  for (const signal of dbcMessage.signals) {
    const key = signal.name || `${signal.startBit}_${signal.bitLength}`;

    if (Object.prototype.hasOwnProperty.call(byMessage, key)) {
      values[key] = byMessage[key];
      continue;
    }

    if (Object.prototype.hasOwnProperty.call(globalValues, key)) {
      values[key] = globalValues[key];
      continue;
    }

    values[key] = generateDefaultPhysicalValue(signal, blockIndex, frameIndex);
  }

  return values;
}

function generateDefaultPhysicalValue(signal, blockIndex, frameIndex) {
  const rawMin = Number(signal.min ?? 0);
  const rawMax = Number(signal.max ?? 0);

  if (Number.isFinite(rawMin) && Number.isFinite(rawMax) && rawMax > rawMin) {
    const span = rawMax - rawMin;
    return rawMin + ((blockIndex + frameIndex) % Math.min(span + 1, 100));
  }

  return 0;
}

function encodeMessagePayload(dbcMessage, signalValues) {
  const payload = Buffer.alloc(dbcMessage.dlc, 0);

  for (const signal of dbcMessage.signals) {
    const signalKey = signal.name || `${signal.startBit}_${signal.bitLength}`;
    const physicalValue = Number(signalValues[signalKey] ?? 0);
    const rawValue = physicalToRaw(signal, physicalValue);

    writeSignalBits(payload, {
      startBit: signal.startBit,
      bitLength: signal.bitLength,
      byteOrder: signal.byteOrder,
      rawValue,
    });
  }

  return payload;
}

function physicalToRaw(signal, physicalValue) {
  const factor = Number(signal.factor || 1);
  const offset = Number(signal.offset || 0);
  const bitLength = Number(signal.bitLength);

  let raw = Math.round((physicalValue - offset) / factor);
  let rawBig = BigInt(raw);

  if (signal.isSigned) {
    const minSigned = -(1n << BigInt(bitLength - 1));
    const maxSigned = (1n << BigInt(bitLength - 1)) - 1n;

    if (rawBig < minSigned) rawBig = minSigned;
    if (rawBig > maxSigned) rawBig = maxSigned;

    if (rawBig < 0n) {
      rawBig = (1n << BigInt(bitLength)) + rawBig;
    }

    return rawBig;
  }

  const maxUnsigned = (1n << BigInt(bitLength)) - 1n;

  if (rawBig < 0n) rawBig = 0n;
  if (rawBig > maxUnsigned) rawBig = maxUnsigned;

  return rawBig;
}

function writeSignalBits(payload, signal) {
  validateSignalBounds(payload, signal.startBit, signal.bitLength, signal.byteOrder);

  if (signal.byteOrder === 'big') {
    writeMotorolaBits(payload, signal.startBit, signal.bitLength, signal.rawValue);
  } else {
    writeIntelBits(payload, signal.startBit, signal.bitLength, signal.rawValue);
  }
}

function validateSignalBounds(payload, startBit, bitLength, byteOrder) {
  if (!Number.isInteger(startBit) || startBit < 0) {
    throw new Error(`Invalid signal start bit: ${startBit}`);
  }

  if (!Number.isInteger(bitLength) || bitLength <= 0 || bitLength > 64) {
    throw new Error(`Invalid signal bit length: ${bitLength}`);
  }

  const payloadBits = payload.length * 8;

  if (startBit >= payloadBits) {
    throw new Error(`Signal start bit out of payload bounds: ${startBit}`);
  }

  if (byteOrder === 'little') {
    const endBit = startBit + bitLength - 1;

    if (endBit >= payloadBits) {
      throw new Error(
        `Little-endian signal exceeds payload bounds. startBit=${startBit}, bitLength=${bitLength}, payloadBits=${payloadBits}`
      );
    }

    return;
  }

  const positions = getMotorolaBitPositions(startBit, bitLength);

  for (const bit of positions) {
    if (bit < 0 || bit >= payloadBits) {
      throw new Error(
        `Motorola signal exceeds payload bounds. startBit=${startBit}, bitLength=${bitLength}, invalidBit=${bit}, payloadBits=${payloadBits}`
      );
    }
  }
}

function writeIntelBits(payload, startBit, bitLength, rawValue) {
  const value = BigInt(rawValue);

  for (let i = 0; i < bitLength; i += 1) {
    const bitValue = Number((value >> BigInt(i)) & 1n);
    const absoluteBit = startBit + i;
    setPayloadBit(payload, absoluteBit, bitValue);
  }
}

function writeMotorolaBits(payload, startBit, bitLength, rawValue) {
  const value = BigInt(rawValue);
  const positions = getMotorolaBitPositions(startBit, bitLength);

  for (let i = 0; i < bitLength; i += 1) {
    const sourceBitIndex = BigInt(bitLength - 1 - i);
    const bitValue = Number((value >> sourceBitIndex) & 1n);
    setPayloadBit(payload, positions[i], bitValue);
  }
}

function getMotorolaBitPositions(startBit, bitLength) {
  const positions = [];
  let bit = Number(startBit);

  for (let i = 0; i < bitLength; i += 1) {
    positions.push(bit);

    if (bit % 8 === 0) {
      bit += 15;
    } else {
      bit -= 1;
    }
  }

  return positions;
}

function setPayloadBit(payload, absoluteBit, value) {
  const byteIndex = Math.floor(absoluteBit / 8);
  const bitIndex = absoluteBit % 8;

  if (byteIndex < 0 || byteIndex >= payload.length) {
    throw new Error(`Bit index out of payload bounds: ${absoluteBit}`);
  }

  if (value) {
    payload[byteIndex] |= (1 << bitIndex);
  } else {
    payload[byteIndex] &= ~(1 << bitIndex);
  }
}

function normalizePayload(value) {
  if (Buffer.isBuffer(value)) {
    return Buffer.from(value);
  }

  if (Array.isArray(value)) {
    return Buffer.from(value.map((item) => Number(item) & 0xff));
  }

  if (typeof value === 'string') {
    const cleaned = value
      .replace(/^0x/i, '')
      .replace(/[^a-fA-F0-9]/g, '');

    if (cleaned.length % 2 !== 0) {
      throw new Error(`Invalid hex payload: ${value}`);
    }

    return Buffer.from(cleaned, 'hex');
  }

  throw new Error('Invalid payload format.');
}

function buildTracksterBin(blocks, requestedBlockSize) {
  const targetBlockSize = Number(requestedBlockSize || 0);

  const normalizedBlocks = blocks.map((block, blockIndex) => {
    const frames = block.frames || [];
    const frameBuffers = frames.map((frame) => buildFrameRecord(frame));
    const blockPayload = Buffer.concat(frameBuffers);

    const payloadBytes = blockPayload.length;
    const minimumBlockSize = BLOCK_HEADER_SIZE + payloadBytes;

    const blockSizeBytes = targetBlockSize > 0
      ? targetBlockSize
      : minimumBlockSize;

    if (!Number.isInteger(blockSizeBytes) || blockSizeBytes < minimumBlockSize) {
      throw new Error(
        `Invalid blocksSize for block ${blockIndex}. ` +
        `blocksSize=${blockSizeBytes}, minimumRequired=${minimumBlockSize}`
      );
    }

    const fillerBytes = blockSizeBytes - minimumBlockSize;

    return {
      blockIndex,
      timestampNs: toBigIntSafe(block.timestampNs || 0),
      frameCount: frames.length,
      payloadBytes,
      blockSizeBytes,
      payload: blockPayload,
      filler: Buffer.alloc(fillerBytes, 0),
    };
  });

  const totalFrameCount = normalizedBlocks.reduce(
    (sum, block) => sum + block.frameCount,
    0
  );

  const totalPayloadBytes = normalizedBlocks.reduce(
    (sum, block) => sum + block.blockSizeBytes,
    0
  );

  const globalHeader = buildGlobalHeader({
    blockCount: normalizedBlocks.length,
    totalFrameCount,
    totalPayloadBytes,
  });

  const blockBuffers = normalizedBlocks.map((block) => {
    const blockHeader = buildBlockHeader(block);

    return Buffer.concat([
      blockHeader,
      block.payload,
      block.filler,
    ]);
  });

  return Buffer.concat([
    globalHeader,
    ...blockBuffers,
  ]);
}

function buildGlobalHeader({
  blockCount,
  totalFrameCount,
  totalPayloadBytes,
}) {
  const buffer = Buffer.alloc(GLOBAL_HEADER_SIZE);

  buffer.write(TRACKSTER_MAGIC, 0, 4, 'ascii');

  buffer.writeUInt16LE(FORMAT_VERSION, 4);
  buffer.writeUInt8(1, 6);
  buffer.writeUInt8(0, 7);

  buffer.writeUInt16LE(GLOBAL_HEADER_SIZE, 8);
  buffer.writeUInt16LE(BLOCK_HEADER_SIZE, 10);
  buffer.writeUInt16LE(FRAME_FIXED_HEADER_SIZE, 12);
  buffer.writeUInt16LE(0, 14);

  buffer.writeUInt32LE(blockCount, 16);
  buffer.writeUInt32LE(totalFrameCount, 20);

  buffer.writeBigUInt64LE(BigInt(Date.now()), 24);

  buffer.writeUInt32LE(totalPayloadBytes, 32);
  buffer.writeUInt32LE(0, 36);

  return buffer;
}

function buildBlockHeader(block) {
  const buffer = Buffer.alloc(BLOCK_HEADER_SIZE);

  buffer.write(BLOCK_MAGIC, 0, 4, 'ascii');

  buffer.writeUInt32LE(block.blockIndex, 4);
  buffer.writeBigUInt64LE(toBigIntSafe(block.timestampNs), 8);
  buffer.writeUInt32LE(block.frameCount, 16);
  buffer.writeUInt32LE(block.payloadBytes, 20);
  buffer.writeUInt32LE(block.blockSizeBytes, 24);
  buffer.writeUInt32LE(0, 28);

  return buffer;
}

function buildFrameRecord(frame) {
  const canId = Number(frame.canId);
  const bus = Number(frame.bus || 0);
  const flags = Number(frame.flags || 0);
  const timestampDeltaNs = Number(frame.timestampDeltaNs || 0);

  const payload = Buffer.isBuffer(frame.payload)
    ? frame.payload
    : Buffer.from(frame.payload || []);

  const dlc = Number(frame.dlc ?? payload.length);

  if (!Number.isInteger(canId) || canId < 0) {
    throw new Error(`Invalid CAN ID: ${frame.canId}`);
  }

  if (!Number.isInteger(bus) || bus < 0 || bus > 255) {
    throw new Error(`Invalid CAN bus: ${frame.bus}`);
  }

  if (!Number.isInteger(flags) || flags < 0 || flags > 255) {
    throw new Error(`Invalid frame flags for CAN ID ${canId}: ${flags}`);
  }

  if (!Number.isInteger(timestampDeltaNs) || timestampDeltaNs < 0 || timestampDeltaNs > 0xffffffff) {
    throw new Error(`Invalid timestamp delta for CAN ID ${canId}: ${timestampDeltaNs}`);
  }

  if (!Number.isInteger(dlc) || dlc < 0 || dlc > 64) {
    throw new Error(`Invalid DLC for CAN ID ${canId}: ${dlc}`);
  }

  if (payload.length !== dlc) {
    throw new Error(
      `Payload length does not match DLC for CAN ID ${canId}. ` +
      `DLC=${dlc}, payload=${payload.length}`
    );
  }

  const buffer = Buffer.alloc(FRAME_FIXED_HEADER_SIZE + dlc);

  buffer.writeUInt32LE(canId, 0);
  buffer.writeUInt32LE(timestampDeltaNs, 4);
  buffer.writeUInt8(bus, 8);
  buffer.writeUInt8(dlc, 9);
  buffer.writeUInt8(flags, 10);

  payload.copy(buffer, FRAME_FIXED_HEADER_SIZE);

  return buffer;
}

function toBigIntSafe(value) {
  if (typeof value === 'bigint') {
    return value;
  }

  if (value === undefined || value === null || value === '') {
    return 0n;
  }

  return BigInt(Math.trunc(Number(value)));
}