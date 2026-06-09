import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

const s3 = new S3Client({});

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS,GET,POST'
};

const TRACKSTER_MAGIC = 'TRKS';
const BLOCK_MAGIC = 'BLK1';

const GLOBAL_HEADER_SIZE = 40;
const BLOCK_HEADER_SIZE = 32;
const FRAME_FIXED_HEADER_SIZE = 11;

const FRAME_FLAG_CAN_FD = 0x01;
const FRAME_FLAG_EXTENDED_ID = 0x02;

const SIGNAL_FIELD = {
  START_BIT: 0,
  BIT_LENGTH: 1,
  BYTE_ORDER: 2,
  SIGNED: 3,
  FACTOR: 4,
  OFFSET: 5,
  MIN: 6,
  MAX: 7,
  NAME: 8,
  MULTIPLEXOR: 9,
  MULTIPLEX_VALUE: 10
};

const CAN_FD_DLC_TO_BYTES = new Map([
  [0, 0],
  [1, 1],
  [2, 2],
  [3, 3],
  [4, 4],
  [5, 5],
  [6, 6],
  [7, 7],
  [8, 8],
  [9, 12],
  [10, 16],
  [11, 20],
  [12, 24],
  [13, 32],
  [14, 48],
  [15, 64]
]);

export const handler = async (event) => {
  try {
    if (event?.requestContext?.http?.method === 'OPTIONS' || event?.httpMethod === 'OPTIONS') {
      return jsonResponse(200, {
        ok: true
      });
    }

    const request = parseRequestBody(event);

    validateRequest(request);

    const [manifest, binBuffer] = await Promise.all([
      readJsonFromS3(request.bucket, request.manifestKey),
      readBufferFromS3(request.bucket, request.binKey)
    ]);

    const requestedSignals =
      resolveRequestedSignalsFromManifest(
        manifest,
        request.signals
      );

    const frames =
      parseTracksterBinFrames(binBuffer);

    const response =
      buildSignalPlotDataResponse(
        frames,
        requestedSignals
      );

    return jsonResponse(200, response);

  } catch (error) {
    console.error('Signal Plotter data Lambda failed.', {
      errorName: error?.name,
      errorMessage: error?.message,
      stack: error?.stack
    });

    return jsonResponse(500, {
      message: error instanceof Error ? error.message : 'Unexpected Signal Plotter data Lambda error.'
    });
  }
};

function parseRequestBody(event) {
  if (!event?.body) {
    return {};
  }

  const bodyText =
    event.isBase64Encoded
      ? Buffer.from(event.body, 'base64').toString('utf8')
      : event.body;

  return JSON.parse(bodyText);
}

function validateRequest(request) {
  if (!request || typeof request !== 'object') {
    throw new Error('Invalid request body.');
  }

  if (!request.bucket || typeof request.bucket !== 'string') {
    throw new Error('Missing required field: bucket.');
  }

  if (!request.binKey || typeof request.binKey !== 'string') {
    throw new Error('Missing required field: binKey.');
  }

  if (!request.manifestKey || typeof request.manifestKey !== 'string') {
    throw new Error('Missing required field: manifestKey.');
  }

  if (!Array.isArray(request.signals) || request.signals.length === 0) {
    throw new Error('Missing required field: signals.');
  }
}

async function readJsonFromS3(bucket, key) {
  const buffer =
    await readBufferFromS3(bucket, key);

  return JSON.parse(buffer.toString('utf8'));
}

async function readBufferFromS3(bucket, key) {
  const response =
    await s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key
      })
    );

  if (!response.Body) {
    throw new Error(`Empty S3 response body for s3://${bucket}/${key}`);
  }

  const byteArray =
    await response.Body.transformToByteArray();

  return Buffer.from(byteArray);
}

function resolveRequestedSignalsFromManifest(manifest, requestedSignals) {
  const resolvedFrames =
    getResolvedCanFramesFromManifest(manifest);

  const resolvedSignals = [];

  for (const requestedSignal of requestedSignals) {
    const requestedCanId =
      normalizeCanId(requestedSignal.canId);

    const frame =
      resolvedFrames.find(candidate => {
        const candidateCanId =
          normalizeCanId(candidate.canId);

        const candidateMessageName =
          String(candidate.messageName || candidate.frame?.n || '');

        return (
          candidateCanId === requestedCanId &&
          candidateMessageName === requestedSignal.messageName
        );
      });

    if (!frame) {
      console.warn('Requested CAN frame was not found in manifest.', requestedSignal);
      continue;
    }

    const rawSignal =
      (frame.frame?.s || []).find(signal =>
        String(signal[SIGNAL_FIELD.NAME]) === requestedSignal.signalName
      );

    if (!rawSignal) {
      console.warn('Requested signal was not found in manifest frame.', requestedSignal);
      continue;
    }

    resolvedSignals.push({
      id: requestedSignal.id,
      canId: normalizeCanId(frame.canId),
      messageName: requestedSignal.messageName,
      signalName: requestedSignal.signalName,
      unit: '',
      rawSignal
    });
  }

  return resolvedSignals;
}

function getResolvedCanFramesFromManifest(manifest) {
  const resolvedFrames =
    manifest?.dbc?.resolvedCanFrames || [];

  if (Array.isArray(resolvedFrames) && resolvedFrames.length > 0) {
    return resolvedFrames;
  }

  const canFrames =
    manifest?.dbc?.canFrames || [];

  return Array.isArray(canFrames)
    ? canFrames
    : [];
}

function parseTracksterBinFrames(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    throw new Error('BIN input is not a buffer.');
  }

  if (buffer.length < GLOBAL_HEADER_SIZE) {
    throw new Error(`BIN file is smaller than Trackster global header size (${GLOBAL_HEADER_SIZE} bytes).`);
  }

  const magic =
    buffer.toString('ascii', 0, 4);

  if (magic !== TRACKSTER_MAGIC) {
    throw new Error(`Invalid Trackster BIN magic. Expected ${TRACKSTER_MAGIC}, received ${magic}.`);
  }

  const globalHeaderSize =
    buffer.readUInt16LE(8);

  const blockHeaderSize =
    buffer.readUInt16LE(10);

  const frameFixedHeaderSize =
    buffer.readUInt16LE(12);

  const blockCount =
    buffer.readUInt32LE(16);

  if (globalHeaderSize !== GLOBAL_HEADER_SIZE) {
    throw new Error(`Unsupported Trackster global header size: ${globalHeaderSize}.`);
  }

  if (blockHeaderSize !== BLOCK_HEADER_SIZE) {
    throw new Error(`Unsupported Trackster block header size: ${blockHeaderSize}.`);
  }

  if (frameFixedHeaderSize !== FRAME_FIXED_HEADER_SIZE) {
    throw new Error(`Unsupported Trackster frame fixed header size: ${frameFixedHeaderSize}.`);
  }

  const frames = [];
  let offset = globalHeaderSize;

  for (let blockIndex = 0; blockIndex < blockCount; blockIndex += 1) {
    if (offset + blockHeaderSize > buffer.length) {
      throw new Error(`Unexpected end of BIN while reading block header ${blockIndex}.`);
    }

    const blockMagic =
      buffer.toString('ascii', offset, offset + 4);

    if (blockMagic !== BLOCK_MAGIC) {
      throw new Error(`Invalid block magic at block ${blockIndex}. Expected ${BLOCK_MAGIC}, received ${blockMagic}.`);
    }

    const storedBlockIndex =
      buffer.readUInt32LE(offset + 4);

    const blockTimestampNs =
      buffer.readBigUInt64LE(offset + 8);

    const frameCount =
      buffer.readUInt32LE(offset + 16);

    const payloadBytes =
      buffer.readUInt32LE(offset + 20);

    const blockSizeBytes =
      buffer.readUInt32LE(offset + 24);

    const blockPayloadStart =
      offset + blockHeaderSize;

    const blockPayloadEnd =
      blockPayloadStart + payloadBytes;

    const nextBlockOffset =
      offset + blockSizeBytes;

    if (blockPayloadEnd > buffer.length) {
      throw new Error(`Block payload exceeds BIN size at block ${blockIndex}.`);
    }

    if (nextBlockOffset > buffer.length) {
      throw new Error(`Block size exceeds BIN size at block ${blockIndex}.`);
    }

    let frameOffset = blockPayloadStart;

    for (let frameIndex = 0; frameIndex < frameCount; frameIndex += 1) {
      if (frameOffset + frameFixedHeaderSize > blockPayloadEnd) {
        throw new Error(`Unexpected end of block payload while reading frame ${frameIndex} in block ${blockIndex}.`);
      }

      const canId =
        buffer.readUInt32LE(frameOffset);

      const timestampDeltaNs =
        buffer.readUInt32LE(frameOffset + 4);

      const bus =
        buffer.readUInt8(frameOffset + 8);

      const dlcCode =
        buffer.readUInt8(frameOffset + 9);

      const flags =
        buffer.readUInt8(frameOffset + 10);

      const payloadLength =
        decodeDlcLength(dlcCode);

      const payloadStart =
        frameOffset + frameFixedHeaderSize;

      const payloadEnd =
        payloadStart + payloadLength;

      if (payloadEnd > blockPayloadEnd) {
        throw new Error(
          `Frame payload exceeds block payload. block=${blockIndex}, frame=${frameIndex}, dlc=${dlcCode}, payloadLength=${payloadLength}.`
        );
      }

      const timestampNs =
        blockTimestampNs + BigInt(timestampDeltaNs);

      frames.push({
        blockIndex: storedBlockIndex,
        frameIndex,
        timestampNs,
        timestampSeconds: Number(timestampNs) / 1_000_000_000,
        canId: normalizeCanId(canId),
        bus,
        dlcCode,
        flags,
        isCanFd: (flags & FRAME_FLAG_CAN_FD) !== 0,
        isExtendedId: (flags & FRAME_FLAG_EXTENDED_ID) !== 0,
        payload: buffer.subarray(payloadStart, payloadEnd)
      });

      frameOffset = payloadEnd;
    }

    offset = nextBlockOffset;
  }

  return frames;
}

function buildSignalPlotDataResponse(frames, requestedSignals) {
  if (requestedSignals.length === 0) {
    return {
      timeAxisSeconds: [],
      signals: []
    };
  }

  const valuesBySignalId =
    new Map();

  for (const signal of requestedSignals) {
    valuesBySignalId.set(
      signal.id,
      new Map()
    );
  }

  for (const frame of frames) {
    const matchingSignals =
      requestedSignals.filter(signal =>
        signal.canId === frame.canId
      );

    if (matchingSignals.length === 0) {
      continue;
    }

    for (const signal of matchingSignals) {
      const value =
        decodeSignalPhysicalValue(
          frame.payload,
          signal.rawSignal
        );

      valuesBySignalId
        .get(signal.id)
        .set(frame.timestampSeconds, value);
    }
  }

  const timestamps =
    getCommonTimestamps(
      requestedSignals,
      valuesBySignalId
    );

  const timeAxisSeconds =
    normalizeTimestampsToRelativeSeconds(timestamps);

  const signals =
    requestedSignals.map(signal => {
      const valueMap =
        valuesBySignalId.get(signal.id);

      return {
        id: signal.id,
        canId: signal.canId,
        messageName: signal.messageName,
        signalName: signal.signalName,
        unit: signal.unit,
        values: timestamps.map(timestamp =>
          roundNumber(valueMap.get(timestamp), 6)
        )
      };
    });

  return {
    timeAxisSeconds,
    signals
  };
}

function getCommonTimestamps(requestedSignals, valuesBySignalId) {
  if (requestedSignals.length === 0) {
    return [];
  }

  const firstMap =
    valuesBySignalId.get(requestedSignals[0].id);

  if (!firstMap) {
    return [];
  }

  return [...firstMap.keys()]
    .filter(timestamp =>
      requestedSignals.every(signal =>
        valuesBySignalId.get(signal.id)?.has(timestamp)
      )
    )
    .sort((left, right) => left - right);
}

function normalizeTimestampsToRelativeSeconds(timestamps) {
  if (timestamps.length === 0) {
    return [];
  }

  const base =
    timestamps[0];

  return timestamps.map(timestamp =>
    roundNumber(timestamp - base, 6)
  );
}

function decodeSignalPhysicalValue(payload, rawSignal) {
  const startBit =
    Number(rawSignal[SIGNAL_FIELD.START_BIT]);

  const bitLength =
    Number(rawSignal[SIGNAL_FIELD.BIT_LENGTH]);

  const byteOrder =
    Number(rawSignal[SIGNAL_FIELD.BYTE_ORDER]);

  const signed =
    Number(rawSignal[SIGNAL_FIELD.SIGNED]) === 1;

  const factor =
    finiteNumberOrDefault(rawSignal[SIGNAL_FIELD.FACTOR], 1);

  const offset =
    finiteNumberOrDefault(rawSignal[SIGNAL_FIELD.OFFSET], 0);

  if (!Number.isInteger(startBit) || !Number.isInteger(bitLength) || bitLength <= 0) {
    throw new Error(`Invalid signal layout: startBit=${startBit}, bitLength=${bitLength}`);
  }

  const rawValue =
    byteOrder === 1
      ? extractLittleEndianUnsigned(payload, startBit, bitLength)
      : extractBigEndianUnsigned(payload, startBit, bitLength);

  const signedValue =
    signed
      ? signExtend(rawValue, bitLength)
      : rawValue;

  return signedValue * factor + offset;
}

function extractLittleEndianUnsigned(payload, startBit, bitLength) {
  let value = 0n;

  for (let bitIndex = 0; bitIndex < bitLength; bitIndex += 1) {
    const absoluteBit =
      startBit + bitIndex;

    const bit =
      getBitByDbcIndex(payload, absoluteBit);

    if (bit) {
      value |= 1n << BigInt(bitIndex);
    }
  }

  return Number(value);
}

function extractBigEndianUnsigned(payload, startBit, bitLength) {
  let value = 0n;
  let dbcBit = startBit;

  for (let bitIndex = 0; bitIndex < bitLength; bitIndex += 1) {
    const bit =
      getBitByDbcIndex(payload, dbcBit);

    value =
      (value << 1n) | BigInt(bit);

    dbcBit =
      getPreviousMotorolaDbcBit(dbcBit);
  }

  return Number(value);
}

function getPreviousMotorolaDbcBit(dbcBit) {
  return dbcBit % 8 === 0
    ? dbcBit + 15
    : dbcBit - 1;
}

function getBitByDbcIndex(payload, dbcBit) {
  const byteIndex =
    Math.floor(dbcBit / 8);

  const bitIndex =
    dbcBit % 8;

  if (byteIndex < 0 || byteIndex >= payload.length) {
    return 0;
  }

  return (payload[byteIndex] >> bitIndex) & 1;
}

function signExtend(value, bitLength) {
  const signBit =
    2 ** (bitLength - 1);

  const fullRange =
    2 ** bitLength;

  return (value & signBit)
    ? value - fullRange
    : value;
}

function decodeDlcLength(dlcCode) {
  const length =
    CAN_FD_DLC_TO_BYTES.get(dlcCode);

  if (typeof length !== 'number') {
    throw new Error(`Invalid CAN DLC value: ${dlcCode}`);
  }

  return length;
}

function normalizeCanId(value) {
  if (typeof value === 'number') {
    return `0x${value.toString(16).toLowerCase()}`;
  }

  const text =
    String(value || '').trim().toLowerCase();

  if (!text) {
    return '';
  }

  if (text.startsWith('0x')) {
    const parsedHex =
      parseInt(text.slice(2), 16);

    return Number.isFinite(parsedHex)
      ? `0x${parsedHex.toString(16).toLowerCase()}`
      : '';
  }

  const parsedDecimal =
    parseInt(text, 10);

  return Number.isFinite(parsedDecimal)
    ? `0x${parsedDecimal.toString(16).toLowerCase()}`
    : '';
}

function finiteNumberOrDefault(value, fallback) {
  const parsed =
    Number(value);

  return Number.isFinite(parsed)
    ? parsed
    : fallback;
}

function roundNumber(value, decimals) {
  if (!Number.isFinite(value)) {
    return 0;
  }

  const factor =
    10 ** decimals;

  return Math.round(value * factor) / factor;
}

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      ...CORS_HEADERS,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  };
}