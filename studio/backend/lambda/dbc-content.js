import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

const s3 = new S3Client({});
const BUCKET_NAME = 'trackster-customer-dbc';

function isValidCustomerId(customerId) {
  return /^[A-Za-z0-9]{8}$/.test(customerId);
}

function isValidFileName(fileName) {
  return (
    typeof fileName === 'string' &&
    fileName.length > 0 &&
    !fileName.includes('/') &&
    !fileName.includes('\\') &&
    fileName.toLowerCase().endsWith('.dbc')
  );
}

function buildTextResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      'Content-Type': 'text/plain; charset=utf-8'
    },
    body
  };
}

async function streamToString(stream) {
  const chunks = [];

  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks).toString('utf-8');
}

export const handler = async (event) => {
  try {
    const customerId = event?.queryStringParameters?.customerId ?? null;
    const fileName = event?.queryStringParameters?.fileName ?? null;

    if (!customerId || !isValidCustomerId(customerId)) {
      return buildTextResponse(400, 'Invalid customerId. Must be 8 alphanumeric characters.');
    }

    if (!fileName || !isValidFileName(fileName)) {
      return buildTextResponse(400, 'Invalid fileName.');
    }

    const key = `dbc-files/${customerId}/${fileName}`;

    const result = await s3.send(
      new GetObjectCommand({
        Bucket: BUCKET_NAME,
        Key: key
      })
    );

    if (!result.Body) {
      return buildTextResponse(404, 'File content not found.');
    }

    const content = await streamToString(result.Body);

    return buildTextResponse(200, content);
  } catch (error) {
    console.error('ERROR:', error);

    return buildTextResponse(
      500,
      error instanceof Error ? error.message : 'Internal server error'
    );
  }
};