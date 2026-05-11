import {
  S3Client,
  GetObjectCommand
} from '@aws-sdk/client-s3';

const s3 = new S3Client({});

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization'
};

const MAX_BIN_FILES = 50;

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      ...CORS_HEADERS,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  };
}

function isValidClientId(value) {
  return /^[A-Za-z0-9]{8}$/.test(String(value || ''));
}

function isValidRunId(value) {
  return /^[0-9]{14}$/.test(String(value || ''));
}

function isValidBinFileName(value) {
  return /^[A-Za-z0-9._-]+\.bin$/i.test(String(value || ''));
}

function sanitizeBucket(value) {
  return String(value || '').trim();
}

function parseOptionalInteger(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number.parseInt(String(value), 10);

  if (!Number.isFinite(parsed)) {
    return null;
  }

  return parsed;
}

function buildRunFolder(clientId, runId) {
  return `${clientId}/${runId}`;
}

function buildManifestKey(clientId, runId) {
  return `${buildRunFolder(clientId, runId)}/run-manifest.json`;
}

function buildBinKey(clientId, runId, fileName) {
  return `${buildRunFolder(clientId, runId)}/${fileName}`;
}

async function streamToBuffer(stream) {
  if (!stream) {
    return Buffer.alloc(0);
  }

  if (typeof stream.transformToByteArray === 'function') {
    const bytes = await stream.transformToByteArray();

    return Buffer.from(bytes);
  }

  return await new Promise((resolve, reject) => {
    const chunks = [];

    stream.on('data', chunk => {
      chunks.push(Buffer.from(chunk));
    });

    stream.on('error', reject);

    stream.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
  });
}

async function getObjectBuffer(bucket, key) {
  const response = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key
    })
  );

  const buffer = await streamToBuffer(response.Body);

  return {
    buffer,

    contentType:
      response.ContentType ||
      'application/octet-stream',

    contentLength:
      response.ContentLength ||
      buffer.length,

    lastModified:
      response.LastModified?.toISOString?.() || null,

    eTag:
      response.ETag || null
  };
}

function parseRequest(event) {
  const method =
    event?.requestContext?.http?.method ||
    event?.httpMethod ||
    '';

  if (method.toUpperCase() === 'OPTIONS') {
    return {
      isOptions: true
    };
  }

  const qs = event?.queryStringParameters || {};

  const binFiles =
    typeof qs.binFiles === 'string'
      ? qs.binFiles
          .split(',')
          .map(v => v.trim())
          .filter(Boolean)
      : [];

  return {
    isOptions: false,

    action:
      String(qs.action || '').trim(),

    bucket:
      sanitizeBucket(qs.bucket),

    clientId:
      String(qs.clientId || '').trim(),

    runId:
      String(qs.runId || qs.timestamp || '').trim(),

    binFiles,

    pageStart:
      parseOptionalInteger(qs.pageStart),

    pageEnd:
      parseOptionalInteger(qs.pageEnd),

    pageSize:
      parseOptionalInteger(qs.pageSize),

    blockStart:
      parseOptionalInteger(qs.blockStart),

    blockEnd:
      parseOptionalInteger(qs.blockEnd)
  };
}

function validateBaseRequest(request) {
  if (!request.bucket) {
    throw new Error('bucket is required.');
  }

  if (!isValidClientId(request.clientId)) {
    throw new Error(
      'clientId must contain exactly 8 alphanumeric characters.'
    );
  }

  if (!isValidRunId(request.runId)) {
    throw new Error(
      'runId must contain exactly 14 digits.'
    );
  }
}

function buildPagingMetadata(request) {
  return {
    requested: {
      pageStart: request.pageStart,
      pageEnd: request.pageEnd,
      pageSize: request.pageSize,
      blockStart: request.blockStart,
      blockEnd: request.blockEnd
    },

    applied: false,

    note:
      'Paging parameters were received but BIN slicing is not applied by this Lambda version.'
  };
}

async function getRunManifest(request) {
  const key = buildManifestKey(
    request.clientId,
    request.runId
  );

  const object = await getObjectBuffer(
    request.bucket,
    key
  );

  const manifestText = object.buffer.toString('utf8');

  let manifest;

  try {
    manifest = JSON.parse(manifestText);
  }
  catch {
    throw new Error(
      `Invalid JSON manifest at ${key}`
    );
  }

  return {
    type: 'run-manifest',

    bucket:
      request.bucket,

    clientId:
      request.clientId,

    runId:
      request.runId,

    key,

    sizeBytes:
      object.contentLength,

    contentType:
      'application/json',

    lastModified:
      object.lastModified,

    eTag:
      object.eTag,

    manifest
  };
}

async function getBinFiles(request) {
  if (
    !Array.isArray(request.binFiles) ||
    request.binFiles.length === 0
  ) {
    throw new Error(
      'binFiles must contain at least one file.'
    );
  }

  if (request.binFiles.length > MAX_BIN_FILES) {
    throw new Error(
      `Maximum BIN files exceeded (${MAX_BIN_FILES}).`
    );
  }

  const files = [];

  const paging =
    buildPagingMetadata(request);

  for (const fileName of request.binFiles) {
    if (!isValidBinFileName(fileName)) {
      throw new Error(
        `Invalid BIN file name: ${fileName}`
      );
    }

    const key = buildBinKey(
      request.clientId,
      request.runId,
      fileName
    );

    const object = await getObjectBuffer(
      request.bucket,
      key
    );

    files.push({
      fileName,

      key,

      sizeBytes:
        object.contentLength,

      returnedSizeBytes:
        object.buffer.length,

      contentType:
        object.contentType,

      lastModified:
        object.lastModified,

      eTag:
        object.eTag,

      paging,

      contentBase64:
        object.buffer.toString('base64')
    });
  }

  return {
    type: 'bin-files',

    bucket:
      request.bucket,

    clientId:
      request.clientId,

    runId:
      request.runId,

    runFolder:
      buildRunFolder(
        request.clientId,
        request.runId
      ),

    paging,

    files
  };
}

export const handler = async (event) => {
  try {
    const request = parseRequest(event);

    if (request.isOptions) {
      return {
        statusCode: 204,
        headers: CORS_HEADERS,
        body: ''
      };
    }

    validateBaseRequest(request);

    if (request.action === 'get-run-manifest') {
      const result = await getRunManifest(request);

      return buildResponse(200, result);
    }

    if (request.action === 'get-bin-files') {
      const result = await getBinFiles(request);

      return buildResponse(200, result);
    }

    return buildResponse(400, {
      error: 'Invalid action.',
      allowedActions: [
        'get-run-manifest',
        'get-bin-files'
      ]
    });

  }
  catch (error) {
    console.error(
      'Decoder API error:',
      error
    );

    return buildResponse(500, {
      error: 'Internal server error',
      details:
        error instanceof Error
          ? error.message
          : String(error)
    });
  }
};