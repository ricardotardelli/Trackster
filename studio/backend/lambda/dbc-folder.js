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

  return await new Promise((resolve, reject) => {
    const chunks = [];

    body.on('data', (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });

    body.on('error', reject);

    body.on('end', () => {
      resolve(Buffer.concat(chunks).toString('utf-8'));
    });
  });
}

function getFileName(key) {
  return key.split('/').pop();
}

function removeExtension(fileName, extension) {
  return fileName.replace(new RegExp(`\\.${extension}$`, 'i'), '');
}

async function readJsonStatus(jsonKey) {
  const result = await s3.send(
    new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: jsonKey
    })
  );

  const rawBody = await bodyToString(result.Body);
  const json = JSON.parse(rawBody);

  if (json?.st !== 'validated' && json?.st !== 'rejected') {
    throw new Error(`Invalid status field in ${jsonKey}. Expected st = validated or rejected.`);
  }

  return json.st;
}

export const handler = async (event) => {
  try {
    const customerId = event?.queryStringParameters?.customerId;

    if (!customerId || !isValidCustomerId(customerId)) {
      return buildJsonResponse(400, {
        error: 'Invalid customerId. Must be 8 alphanumeric characters.'
      });
    }

    const prefix = `dbc-files/${customerId}/`;

    const listResult = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET_NAME,
        Prefix: prefix
      })
    );

    const objects = listResult.Contents || [];

    const jsonKeysByBaseName = new Map();

    for (const object of objects) {
      if (!object.Key || !object.Key.toLowerCase().endsWith('.json')) {
        continue;
      }

      const fileName = getFileName(object.Key);
      const baseName = removeExtension(fileName, 'json').toLowerCase();

      jsonKeysByBaseName.set(baseName, object.Key);
    }

    const dbcObjects = objects.filter((object) => {
      return object.Key && object.Key.toLowerCase().endsWith('.dbc');
    });

    const files = await Promise.all(
      dbcObjects.map(async (object) => {
        const fileName = getFileName(object.Key);
        const baseName = removeExtension(fileName, 'dbc').toLowerCase();

        const jsonKey = jsonKeysByBaseName.get(baseName);

        let status = 'pending';

        if (jsonKey) {
          status = await readJsonStatus(jsonKey);
        }

        return {
          name: fileName,
          sizeBytes: object.Size ?? 0,
          lastModified: object.LastModified
            ? new Date(object.LastModified).toISOString()
            : new Date().toISOString(),
          status
        };
      })
    );

    files.sort((a, b) => a.name.localeCompare(b.name));

    return buildJsonResponse(200, {
      folderName: customerId,
      files
    });
  } catch (error) {
    console.error('ERROR:', error);

    return buildJsonResponse(500, {
      error: 'Internal server error',
      details: error instanceof Error ? error.message : String(error)
    });
  }
};