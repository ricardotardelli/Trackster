import {
  S3Client,
  DeleteObjectsCommand,
  ListObjectsV2Command,
  HeadObjectCommand
} from '@aws-sdk/client-s3';

const s3 = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

const MANIFEST_FILE_NAME = process.env.MANIFEST_FILE_NAME || 'run-manifest.json';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS,POST'
};

export const handler = async (event) => {
  try {
    if (isOptionsRequest(event)) {
      return response(200, { success: true });
    }

    const body = parseBody(event.body);

    const clientId = normalizeClientId(body.clientId);
    const bucketName = normalizeBucketName(body.bucketName);
    const requestedKeys = normalizeKeys(body.keys);
    const deleteManifestWhenEmpty = body.deleteManifestWhenEmpty !== false;

    validateRequest(clientId, bucketName, requestedKeys);

    const existingBinKeys = [];
    const missingBinKeys = [];

    for (const key of requestedKeys) {
      const exists = await objectExists(bucketName, key);

      if (exists) {
        existingBinKeys.push(key);
      } else {
        missingBinKeys.push(key);
      }
    }

    await deleteKeys(bucketName, existingBinKeys);

    const affectedFolders = Array.from(
      new Set(existingBinKeys.map(getFolderPrefix))
    );

    const manifestsToDelete = [];
    const folderMarkersToDelete = [];

    if (deleteManifestWhenEmpty) {
      for (const folderPrefix of affectedFolders) {
        const remainingBinCount = await countRemainingBinFiles(bucketName, folderPrefix);

        if (remainingBinCount === 0) {
          const manifestKey = `${folderPrefix}${MANIFEST_FILE_NAME}`;

          if (await objectExists(bucketName, manifestKey)) {
            manifestsToDelete.push(manifestKey);
          }

          if (await objectExists(bucketName, folderPrefix)) {
            folderMarkersToDelete.push(folderPrefix);
          }
        }
      }

      await deleteKeys(bucketName, manifestsToDelete);
      await deleteKeys(bucketName, folderMarkersToDelete);
    }

    return response(200, {
      success: true,
      bucketName,
      requestedBinFiles: requestedKeys.length,
      deletedBinFiles: existingBinKeys.length,
      missingBinFiles: missingBinKeys.length,
      deletedManifests: manifestsToDelete.length,
      deletedFolderMarkers: folderMarkersToDelete.length,
      affectedFolders,
      deletedBinKeys: existingBinKeys,
      deletedManifestKeys: manifestsToDelete,
      deletedFolderMarkerKeys: folderMarkersToDelete,
      missingBinKeys
    });
  } catch (error) {
    console.error('BIN delete failed:', error);

    return response(error.statusCode || 500, {
      success: false,
      message: error.message || 'Unexpected error while deleting BIN files.'
    });
  }
};

function isOptionsRequest(event) {
  return event.requestContext?.http?.method === 'OPTIONS' || event.httpMethod === 'OPTIONS';
}

function parseBody(rawBody) {
  if (!rawBody) {
    return {};
  }

  if (typeof rawBody === 'object') {
    return rawBody;
  }

  try {
    return JSON.parse(rawBody);
  } catch {
    throw httpError(400, 'Invalid JSON body.');
  }
}

function normalizeClientId(clientId) {
  if (typeof clientId !== 'string') {
    return '';
  }

  return clientId.trim().replace(/^\/+|\/+$/g, '');
}

function normalizeBucketName(bucketName) {
  if (typeof bucketName !== 'string') {
    return '';
  }

  return bucketName.trim();
}

function normalizeKeys(keys) {
  if (!Array.isArray(keys)) {
    return [];
  }

  return Array.from(new Set(
    keys
      .filter((key) => typeof key === 'string')
      .map((key) => key.trim().replace(/^\/+/g, ''))
      .filter((key) => key.length > 0)
  ));
}

function validateRequest(clientId, bucketName, keys) {
  if (!clientId) {
    throw httpError(400, 'clientId is required.');
  }

  if (!bucketName) {
    throw httpError(400, 'bucketName is required.');
  }

  if (!isValidBucketName(bucketName)) {
    throw httpError(400, `Invalid bucketName: ${bucketName}`);
  }

  if (keys.length === 0) {
    throw httpError(400, 'At least one BIN key is required.');
  }

  for (const key of keys) {
    if (!key.toLowerCase().endsWith('.bin')) {
      throw httpError(400, `Only .bin files can be deleted. Invalid key: ${key}`);
    }

    if (key.includes('..')) {
      throw httpError(400, `Invalid key path: ${key}`);
    }

    if (!key.startsWith(`${clientId}/`)) {
      throw httpError(403, `Key is outside the client folder: ${key}`);
    }
  }
}

function isValidBucketName(bucketName) {
  return /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucketName);
}

function getFolderPrefix(key) {
  const lastSlashIndex = key.lastIndexOf('/');

  if (lastSlashIndex < 0) {
    return '';
  }

  return key.slice(0, lastSlashIndex + 1);
}

async function objectExists(bucketName, key) {
  try {
    await s3.send(new HeadObjectCommand({
      Bucket: bucketName,
      Key: key
    }));

    return true;
  } catch (error) {
    const statusCode = error?.$metadata?.httpStatusCode;

    if (
      statusCode === 404 ||
      error.name === 'NotFound' ||
      error.name === 'NoSuchKey'
    ) {
      return false;
    }

    throw error;
  }
}

async function countRemainingBinFiles(bucketName, folderPrefix) {
  let continuationToken;
  let count = 0;

  do {
    const result = await s3.send(new ListObjectsV2Command({
      Bucket: bucketName,
      Prefix: folderPrefix,
      ContinuationToken: continuationToken
    }));

    for (const item of result.Contents || []) {
      if (item.Key && item.Key.toLowerCase().endsWith('.bin')) {
        count += 1;
      }
    }

    continuationToken = result.IsTruncated
      ? result.NextContinuationToken
      : undefined;
  } while (continuationToken);

  return count;
}

async function deleteKeys(bucketName, keys) {
  const uniqueKeys = Array.from(new Set(keys)).filter(Boolean);

  if (uniqueKeys.length === 0) {
    return;
  }

  for (let index = 0; index < uniqueKeys.length; index += 1000) {
    const batch = uniqueKeys.slice(index, index + 1000);

    await s3.send(new DeleteObjectsCommand({
      Bucket: bucketName,
      Delete: {
        Objects: batch.map((Key) => ({ Key })),
        Quiet: true
      }
    }));
  }
}

function response(statusCode, body) {
  return {
    statusCode,
    headers: corsHeaders,
    body: JSON.stringify(body)
  };
}

function httpError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}