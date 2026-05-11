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

const TRACKSTER_HEADER_BYTES = 40;
const TRACKSTER_BLOCK_HEADER_BYTES = 32;
const TRACKSTER_FRAME_FIXED_HEADER_BYTES = 11;

const BLOCK_MAGIC = 'BLK1';

const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 200;

const FRAME_PAYLOAD_LENGTH_CANDIDATE_OFFSETS = [
  6,
  5,
  10,
  4
];

const BLOCK_FRAME_COUNT_CANDIDATE_OFFSETS = [
  16,
  20,
  24,
  12
];

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

function buildRunFolder(clientId, runId) {
  return `${clientId}/${runId}`;
}

function buildManifestKey(clientId, runId) {
  return `${buildRunFolder(clientId, runId)}/run-manifest.json`;
}

function buildBinKey(clientId, runId, fileName) {
  return `${buildRunFolder(clientId, runId)}/${fileName}`;
}

function parsePositiveInteger(value, fallbackValue) {
  const parsed = Number.parseInt(String(value || ''), 10);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallbackValue;
  }

  return parsed;
}

function parseNonNegativeInteger(value, fallbackValue) {
  const parsed = Number.parseInt(String(value || ''), 10);

  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallbackValue;
  }

  return parsed;
}

function clamp(value, min, max) {
  return Math.min(
    Math.max(value, min),
    max
  );
}

function readUInt32LESafe(buffer, offset) {
  if (offset < 0 || offset + 4 > buffer.length) {
    return null;
  }

  return buffer.readUInt32LE(offset);
}

function hasBlockMagic(buffer, offset) {
  if (offset < 0 || offset + 4 > buffer.length) {
    return false;
  }

  return buffer
    .subarray(offset, offset + 4)
    .toString('ascii') === BLOCK_MAGIC;
}

async function streamToBuffer(stream) {
  if (!stream) {
    return Buffer.alloc(0);
  }

  if (typeof stream.transformToByteArray === 'function') {
    const bytes =
      await stream.transformToByteArray();

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
  const response =
    await s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key
      })
    );

  const buffer =
    await streamToBuffer(response.Body);

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

  const qs =
    event?.queryStringParameters || {};

  const binFiles =
    typeof qs.binFiles === 'string'
      ? qs.binFiles
          .split(',')
          .map(v => v.trim())
          .filter(Boolean)
      : [];

  const rawPageSize =
    parsePositiveInteger(
      qs.pageSize,
      DEFAULT_PAGE_SIZE
    );

  const pageSize =
    clamp(
      rawPageSize,
      1,
      MAX_PAGE_SIZE
    );

  const pageStart =
    parsePositiveInteger(
      qs.pageStart,
      null
    );

  const pageEnd =
    parsePositiveInteger(
      qs.pageEnd,
      pageStart
    );

  const blockStart =
    parseNonNegativeInteger(
      qs.blockStart,
      null
    );

  const blockEnd =
    parseNonNegativeInteger(
      qs.blockEnd,
      null
    );

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

    pageStart,
    pageEnd,
    pageSize,
    blockStart,
    blockEnd
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

function shouldPaginateBin(request) {
  return (
    Number.isInteger(request.pageStart) ||
    Number.isInteger(request.pageEnd) ||
    Number.isInteger(request.blockStart) ||
    Number.isInteger(request.blockEnd)
  );
}

function scanFrames(buffer, frameStartOffset, frameCount) {
  for (const payloadLengthOffset of FRAME_PAYLOAD_LENGTH_CANDIDATE_OFFSETS) {
    let cursor = frameStartOffset;
    let isValid = true;
    let payloadBytes = 0;

    for (let frameIndex = 0; frameIndex < frameCount; frameIndex += 1) {
      const lengthOffset =
        cursor + payloadLengthOffset;

      if (
        lengthOffset < cursor ||
        lengthOffset >= buffer.length
      ) {
        isValid = false;
        break;
      }

      const payloadLength =
        buffer.readUInt8(lengthOffset);

      if (payloadLength > 64) {
        isValid = false;
        break;
      }

      const nextCursor =
        cursor +
        TRACKSTER_FRAME_FIXED_HEADER_BYTES +
        payloadLength;

      if (nextCursor > buffer.length) {
        isValid = false;
        break;
      }

      payloadBytes += payloadLength;
      cursor = nextCursor;
    }

    if (!isValid) {
      continue;
    }

    if (
      cursor === buffer.length ||
      hasBlockMagic(buffer, cursor)
    ) {
      return {
        valid: true,
        endOffset: cursor,
        payloadBytes,
        payloadLengthOffset
      };
    }
  }

  return {
    valid: false,
    endOffset: frameStartOffset,
    payloadBytes: 0,
    payloadLengthOffset: null
  };
}

function inferBlockFrameCount(buffer, blockOffset) {
  const frameStartOffset =
    blockOffset + TRACKSTER_BLOCK_HEADER_BYTES;

  for (const frameCountOffset of BLOCK_FRAME_COUNT_CANDIDATE_OFFSETS) {
    const frameCount =
      readUInt32LESafe(
        buffer,
        blockOffset + frameCountOffset
      );

    if (
      frameCount === null ||
      frameCount < 0 ||
      frameCount > 500000
    ) {
      continue;
    }

    const scan =
      scanFrames(
        buffer,
        frameStartOffset,
        frameCount
      );

    if (scan.valid) {
      return {
        valid: true,
        frameCount,
        frameCountOffset,
        endOffset: scan.endOffset,
        payloadBytes: scan.payloadBytes,
        payloadLengthOffset: scan.payloadLengthOffset
      };
    }
  }

  return {
    valid: false,
    frameCount: 0,
    frameCountOffset: null,
    endOffset: blockOffset,
    payloadBytes: 0,
    payloadLengthOffset: null
  };
}

function parseTracksterBlockRanges(buffer) {
  if (buffer.length < TRACKSTER_HEADER_BYTES) {
    throw new Error(
      `Invalid Trackster BIN. File is smaller than ${TRACKSTER_HEADER_BYTES} bytes.`
    );
  }

  const ranges = [];
  let cursor = TRACKSTER_HEADER_BYTES;
  let totalFrameCount = 0;
  let totalPayloadBytes = 0;

  while (cursor < buffer.length) {
    if (!hasBlockMagic(buffer, cursor)) {
      throw new Error(
        `Invalid Trackster BIN. Expected BLK1 at offset ${cursor}.`
      );
    }

    const blockInfo =
      inferBlockFrameCount(
        buffer,
        cursor
      );

    if (!blockInfo.valid) {
      throw new Error(
        `Unable to parse block at offset ${cursor}.`
      );
    }

    const blockIndex =
      ranges.length;

    ranges.push({
      blockIndex,
      startOffset: cursor,
      endOffset: blockInfo.endOffset,
      byteLength: blockInfo.endOffset - cursor,
      frameCount: blockInfo.frameCount,
      payloadBytes: blockInfo.payloadBytes
    });

    totalFrameCount += blockInfo.frameCount;
    totalPayloadBytes += blockInfo.payloadBytes;

    cursor = blockInfo.endOffset;
  }

  return {
    ranges,
    totalBlockCount: ranges.length,
    totalFrameCount,
    totalPayloadBytes
  };
}

function resolveRequestedBlockRange(request, totalBlockCount) {
  if (totalBlockCount <= 0) {
    return {
      blockStart: 0,
      blockEnd: -1,
      pageStart: 1,
      pageEnd: 1,
      pageSize: request.pageSize
    };
  }

  if (
    Number.isInteger(request.blockStart) ||
    Number.isInteger(request.blockEnd)
  ) {
    const safeBlockStart =
      clamp(
        request.blockStart ?? 0,
        0,
        totalBlockCount - 1
      );

    const safeBlockEnd =
      clamp(
        request.blockEnd ?? safeBlockStart,
        safeBlockStart,
        totalBlockCount - 1
      );

    const pageStart =
      Math.floor(safeBlockStart / request.pageSize) + 1;

    const pageEnd =
      Math.floor(safeBlockEnd / request.pageSize) + 1;

    return {
      blockStart: safeBlockStart,
      blockEnd: safeBlockEnd,
      pageStart,
      pageEnd,
      pageSize: request.pageSize
    };
  }

  const totalPages =
    Math.max(
      1,
      Math.ceil(totalBlockCount / request.pageSize)
    );

  const safePageStart =
    clamp(
      request.pageStart ?? 1,
      1,
      totalPages
    );

  const safePageEnd =
    clamp(
      request.pageEnd ?? safePageStart,
      safePageStart,
      totalPages
    );

  const blockStart =
    (safePageStart - 1) * request.pageSize;

  const blockEnd =
    Math.min(
      (safePageEnd * request.pageSize) - 1,
      totalBlockCount - 1
    );

  return {
    blockStart,
    blockEnd,
    pageStart: safePageStart,
    pageEnd: safePageEnd,
    pageSize: request.pageSize
  };
}

function patchFirstUInt32Value(buffer, oldValue, newValue) {
  if (
    !Number.isInteger(oldValue) ||
    !Number.isInteger(newValue) ||
    oldValue < 0 ||
    newValue < 0 ||
    oldValue > 0xFFFFFFFF ||
    newValue > 0xFFFFFFFF
  ) {
    return false;
  }

  for (
    let offset = 4;
    offset <= TRACKSTER_HEADER_BYTES - 4;
    offset += 4
  ) {
    const current =
      buffer.readUInt32LE(offset);

    if (current === oldValue) {
      buffer.writeUInt32LE(newValue, offset);
      return true;
    }
  }

  return false;
}

function buildPagedBinBuffer(originalBuffer, parsedRanges, requestedRange) {
  const selectedRanges =
    parsedRanges.ranges.slice(
      requestedRange.blockStart,
      requestedRange.blockEnd + 1
    );

  const selectedBlockBuffers =
    selectedRanges.map(range =>
      originalBuffer.subarray(
        range.startOffset,
        range.endOffset
      )
    );

  const selectedFrameCount =
    selectedRanges.reduce(
      (total, range) => total + range.frameCount,
      0
    );

  const selectedPayloadBytes =
    selectedRanges.reduce(
      (total, range) => total + range.payloadBytes,
      0
    );

  const header =
    Buffer.from(
      originalBuffer.subarray(
        0,
        TRACKSTER_HEADER_BYTES
      )
    );

  patchFirstUInt32Value(
    header,
    parsedRanges.totalBlockCount,
    selectedRanges.length
  );

  patchFirstUInt32Value(
    header,
    parsedRanges.totalFrameCount,
    selectedFrameCount
  );

  patchFirstUInt32Value(
    header,
    parsedRanges.totalPayloadBytes,
    selectedPayloadBytes
  );

  const pagedBuffer =
    Buffer.concat([
      header,
      ...selectedBlockBuffers
    ]);

  return {
    buffer: pagedBuffer,
    selectedBlockCount: selectedRanges.length,
    selectedFrameCount,
    selectedPayloadBytes,
    selectedFirstBlockIndex:
      selectedRanges[0]?.blockIndex ?? null,
    selectedLastBlockIndex:
      selectedRanges[selectedRanges.length - 1]?.blockIndex ?? null
  };
}

async function getRunManifest(request) {
  const key =
    buildManifestKey(
      request.clientId,
      request.runId
    );

  const object =
    await getObjectBuffer(
      request.bucket,
      key
    );

  const manifestText =
    object.buffer.toString('utf8');

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

  for (const fileName of request.binFiles) {
    if (!isValidBinFileName(fileName)) {
      throw new Error(
        `Invalid BIN file name: ${fileName}`
      );
    }

    const key =
      buildBinKey(
        request.clientId,
        request.runId,
        fileName
      );

    const object =
      await getObjectBuffer(
        request.bucket,
        key
      );

    let outputBuffer =
      object.buffer;

    let paging = null;

    if (shouldPaginateBin(request)) {
      const parsedRanges =
        parseTracksterBlockRanges(
          object.buffer
        );

      const requestedRange =
        resolveRequestedBlockRange(
          request,
          parsedRanges.totalBlockCount
        );

      const paged =
        buildPagedBinBuffer(
          object.buffer,
          parsedRanges,
          requestedRange
        );

      outputBuffer =
        paged.buffer;

      paging = {
        enabled: true,

        pageStart:
          requestedRange.pageStart,

        pageEnd:
          requestedRange.pageEnd,

        pageSize:
          requestedRange.pageSize,

        blockStart:
          requestedRange.blockStart,

        blockEnd:
          requestedRange.blockEnd,

        totalBlocks:
          parsedRanges.totalBlockCount,

        totalFrames:
          parsedRanges.totalFrameCount,

        selectedBlocks:
          paged.selectedBlockCount,

        selectedFrames:
          paged.selectedFrameCount,

        selectedPayloadBytes:
          paged.selectedPayloadBytes,

        selectedFirstBlockIndex:
          paged.selectedFirstBlockIndex,

        selectedLastBlockIndex:
          paged.selectedLastBlockIndex
      };
    }

    files.push({
      fileName,

      key,

      sizeBytes:
        object.contentLength,

      returnedSizeBytes:
        outputBuffer.length,

      contentType:
        object.contentType,

      lastModified:
        object.lastModified,

      eTag:
        object.eTag,

      paging,

      contentBase64:
        outputBuffer.toString('base64')
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

    files
  };
}

export const handler = async (event) => {
  try {
    const request =
      parseRequest(event);

    if (request.isOptions) {
      return {
        statusCode: 204,
        headers: CORS_HEADERS,
        body: ''
      };
    }

    validateBaseRequest(request);

    if (request.action === 'get-run-manifest') {
      const result =
        await getRunManifest(request);

      return buildResponse(200, result);
    }

    if (request.action === 'get-bin-files') {
      const result =
        await getBinFiles(request);

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