import {
  S3Client,
  DeleteObjectCommand
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

export const handler = async (event) => {
  try {
    const customerId = event?.queryStringParameters?.customerId;
    const fileName = event?.queryStringParameters?.fileName;

    if (!customerId || !isValidCustomerId(customerId)) {
      return buildJsonResponse(400, {
        error: 'Invalid customerId. Must be 8 alphanumeric characters.'
      });
    }

    if (!fileName || !isValidDbcFileName(fileName)) {
      return buildJsonResponse(400, {
        error: 'Invalid fileName. Must be a .dbc file.'
      });
    }

    const dbcKey = `dbc-files/${customerId}/${fileName}`;
    const jsonKey = `dbc-files/${customerId}/${getJsonFileName(fileName)}`;

    await s3.send(
      new DeleteObjectCommand({
        Bucket: BUCKET_NAME,
        Key: dbcKey
      })
    );

    await s3.send(
      new DeleteObjectCommand({
        Bucket: BUCKET_NAME,
        Key: jsonKey
      })
    );

    return buildJsonResponse(200, {
      deleted: true,
      fileName
    });
  } catch (error) {
    console.error('ERROR:', error);

    return buildJsonResponse(500, {
      error: 'Internal server error',
      details: error instanceof Error ? error.message : String(error)
    });
  }
};