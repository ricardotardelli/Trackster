import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand
} from '@aws-sdk/client-s3';

const s3 = new S3Client({});
const BUCKET_NAME = 'trackster-customer-dbc';

function isValidCustomerId(customerId) {
  return /^[A-Za-z0-9]{8}$/.test(customerId);
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

async function bodyToString(body) {
  if (!body) {
    return '';
  }

  if (typeof body.transformToString === 'function') {
    return await body.transformToString();
  }

  const chunks = [];

  for await (const chunk of body) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks).toString('utf-8');
}

function getFileName(key) {
  return key.split('/').pop();
}

function removeExtension(fileName, extension) {
  return fileName.replace(new RegExp(`\\.${extension}$`, 'i'), '');
}

async function readS3Text(key) {
  const result = await s3.send(
    new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key
    })
  );

  if (!result.Body) {
    throw new Error(`S3 object body not found: ${key}`);
  }

  return await bodyToString(result.Body);
}

async function listAllObjects(prefix) {
  const objects = [];
  let continuationToken;

  do {
    const result = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET_NAME,
        Prefix: prefix,
        ContinuationToken: continuationToken
      })
    );

    objects.push(...(result.Contents || []));
    continuationToken = result.NextContinuationToken;
  } while (continuationToken);

  return objects;
}

function normalizeCanId(rawId) {
  const numericId = Number(rawId);

  if (!Number.isFinite(numericId)) {
    return String(rawId);
  }

  return `0x${numericId.toString(16)}`;
}

function parseDbcFrameCatalog(dbcFile, content) {
  const frames = [];
  const lines = content.split(/\r?\n/);

  for (const line of lines) {
    const messageMatch = line.match(
      /^BO_\s+(\d+)\s+([A-Za-z0-9_]+)\s*:\s+\d+\s+\S+/
    );

    if (!messageMatch) {
      continue;
    }

    frames.push({
      dbcFile,
      canId: normalizeCanId(messageMatch[1]),
      messageName: messageMatch[2]
    });
  }

  return frames;
}

async function mapWithConcurrency(items, limit, mapper) {
  const results = [];
  let index = 0;

  async function worker() {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;
      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    () => worker()
  );

  await Promise.all(workers);

  return results;
}

export const handler = async (event) => {
  try {
    const customerId = event?.queryStringParameters?.customerId ?? null;

    if (!customerId || !isValidCustomerId(customerId)) {
      return buildJsonResponse(400, {
        error: 'Invalid customerId. Must be 8 alphanumeric characters.'
      });
    }

    const prefix = `dbc-files/${customerId}/`;
    const objects = await listAllObjects(prefix);

    const dbcKeysByBaseName = new Map();
    const jsonObjects = [];

    for (const object of objects) {
      if (!object.Key) {
        continue;
      }

      const fileName = getFileName(object.Key);
      const lowerKey = object.Key.toLowerCase();

      if (lowerKey.endsWith('.dbc')) {
        const baseName = removeExtension(fileName, 'dbc').toLowerCase();
        dbcKeysByBaseName.set(baseName, object.Key);
      }

      if (lowerKey.endsWith('.json')) {
        jsonObjects.push(object);
      }
    }

    const validatedEntriesRaw = await mapWithConcurrency(
      jsonObjects,
      8,
      async (object) => {
        const jsonKey = object.Key;
        const jsonFileName = getFileName(jsonKey);
        const baseName = removeExtension(jsonFileName, 'json').toLowerCase();

        const rawJson = await readS3Text(jsonKey);
        const parsedJson = JSON.parse(rawJson);

        if (parsedJson?.st !== 'validated') {
          return null;
        }

        const dbcKey = dbcKeysByBaseName.get(baseName);

        if (!dbcKey) {
          return null;
        }

        return {
          dbcFile: `${removeExtension(jsonFileName, 'json')}.dbc`,
          dbcKey
        };
      }
    );

    const validatedEntries = validatedEntriesRaw.filter(Boolean);

    const frameGroups = await mapWithConcurrency(
      validatedEntries,
      4,
      async (entry) => {
        const content = await readS3Text(entry.dbcKey);
        return parseDbcFrameCatalog(entry.dbcFile, content);
      }
    );

    const frames = frameGroups.flat();

    frames.sort((a, b) => {
      const dbcCompare = a.dbcFile.localeCompare(b.dbcFile);

      if (dbcCompare !== 0) {
        return dbcCompare;
      }

      return a.canId.localeCompare(b.canId);
    });

    return buildJsonResponse(200, {
      frames
    });
  } catch (error) {
    console.error('ERROR:', error);

    return buildJsonResponse(500, {
      error: 'Internal server error',
      details: error instanceof Error ? error.message : String(error)
    });
  }
};
