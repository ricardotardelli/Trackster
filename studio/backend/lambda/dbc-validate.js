import {
  S3Client,
  PutObjectCommand
} from '@aws-sdk/client-s3';

const s3 = new S3Client({});
const BUCKET_NAME = 'trackster-customer-dbc';

function isValidCustomerId(customerId) {
  return /^[A-Za-z0-9]{8}$/.test(customerId);
}

function isValidDbcFileName(fileName) {
  return /^[A-Za-z0-9._-]+\.dbc$/i.test(fileName);
}

function buildJsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  };
}

function getJsonFileName(dbcFileName) {
  return dbcFileName.replace(/\.dbc$/i, '.json');
}

function normalizeHexId(hexId) {
  if (typeof hexId !== 'string' || !hexId.trim()) {
    return null;
  }

  return hexId.startsWith('0x') || hexId.startsWith('0X')
    ? hexId.toLowerCase()
    : `0x${hexId.toLowerCase()}`;
}

function normalizeSignalName(signal, index) {
  const name = String(signal?.name || '').trim();

  if (name) {
    return name;
  }

  return `signal_${index}`;
}

function normalizeMuxType(signal) {
  const value =
    signal?.muxType ??
    signal?.multiplexType ??
    signal?.multiplexing ??
    null;

  if (value === undefined || value === null || value === '') {
    return null;
  }

  const text = String(value).trim().toLowerCase();

  if (
    text === 'm' ||
    text === 'mux' ||
    text === 'multiplexor' ||
    text === 'multiplexer'
  ) {
    return 'multiplexor';
  }

  if (
    text === 'multiplexed' ||
    /^m\d+$/i.test(text)
  ) {
    return 'multiplexed';
  }

  return text;
}

function normalizeMuxValue(signal) {
  const value =
    signal?.muxValue ??
    signal?.multiplexValue ??
    null;

  if (value === undefined || value === null || value === '') {
    return null;
  }

  const numeric = Number(value);

  if (Number.isFinite(numeric)) {
    return numeric;
  }

  const text = String(value).trim().toLowerCase();

  if (/^m\d+$/.test(text)) {
    return Number(text.slice(1));
  }

  return null;
}

function buildCompiledJson(fileName, parserReport) {
  const hasErrors =
    Array.isArray(parserReport?.errors) && parserReport.errors.length > 0;

  const messages = {};

  if (Array.isArray(parserReport?.data)) {
    for (const message of parserReport.data) {
      const messageId = normalizeHexId(message?.hexId);

      if (!messageId) {
        continue;
      }

      messages[messageId] = {
        l: Number(message.sizeBytes ?? 8),
        n: String(message.name || ''),
        tx: String(message.transmitter || ''),
        s: Array.isArray(message.signals)
          ? message.signals.map((signal, index) => [
              Number(signal.startBit ?? 0),
              Number(signal.sizeBits ?? 0),
              signal.endianness === 'Big Endian' ? 0 : 1,
              signal.isSigned ? 1 : 0,
              Number(signal.factor ?? 1),
              Number(signal.offset ?? 0),
              Number(signal.range?.min ?? 0),
              Number(signal.range?.max ?? 0),
              normalizeSignalName(signal, index),
              normalizeMuxType(signal),
              normalizeMuxValue(signal),
              String(signal.unit || '')
            ])
          : []
      };
    }
  }

  return {
    v: 2,
    src: fileName,
    st: hasErrors ? 'rejected' : 'validated',
    ts: new Date().toISOString(),
    f: [
      'sb',
      'bl',
      'bo',
      'sg',
      'f',
      'o',
      'min',
      'max',
      'n',
      'mx',
      'mv',
      'u'
    ],
    m: messages
  };
}

export const handler = async (event) => {
  try {
    const customerId = event?.queryStringParameters?.customerId;

    if (!customerId || !isValidCustomerId(customerId)) {
      return buildJsonResponse(400, {
        error: 'Invalid customerId. Must be 8 alphanumeric characters.'
      });
    }

    const body = JSON.parse(event.body || '{}');
    const fileName = body.fileName;
    const parserReport = body.parserReport;

    if (!fileName || !isValidDbcFileName(fileName)) {
      return buildJsonResponse(400, {
        error: 'Invalid fileName. Must be a .dbc file.'
      });
    }

    if (!parserReport || typeof parserReport !== 'object') {
      return buildJsonResponse(400, {
        error: 'Missing parserReport object.'
      });
    }

    const compiledJson = buildCompiledJson(fileName, parserReport);
    const jsonKey = `dbc-files/${customerId}/${getJsonFileName(fileName)}`;

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: jsonKey,
        Body: JSON.stringify(compiledJson),
        ContentType: 'application/json'
      })
    );

    return buildJsonResponse(200, {
      fileName,
      status: compiledJson.st
    });
  } catch (error) {
    console.error('ERROR:', error);

    return buildJsonResponse(500, {
      error: 'Internal server error',
      details: error instanceof Error ? error.message : String(error)
    });
  }
};