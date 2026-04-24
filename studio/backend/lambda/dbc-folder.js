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

async function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () =>
      resolve(Buffer.concat(chunks).toString('utf-8'))
    );
  });
}

export const handler = async (event) => {
  try {
    const customerId = event?.queryStringParameters?.customerId;

    if (!customerId || !isValidCustomerId(customerId)) {
      return buildJsonResponse(400, {
        error: 'Invalid customerId'
      });
    }

    const prefix = `dbc-files/${customerId}/`;

    const result = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET_NAME,
        Prefix: prefix
      })
    );

    const objects = result.Contents || [];

    // Map JSON: baseName -> key
    const jsonMap = new Map(
      objects
        .filter((o) => o.Key && o.Key.toLowerCase().endsWith('.json'))
        .map((o) => {
          const fileName = o.Key.split('/').pop();
          const base = fileName.replace(/\.json$/i, '').toLowerCase();
          return [base, o.Key];
        })
    );

    const files = await Promise.all(
      objects
        .filter((o) => o.Key && o.Key.toLowerCase().endsWith('.dbc'))
        .map(async (item) => {
          const name = item.Key.split('/').pop();
          const baseName = name.replace(/\.dbc$/i, '').toLowerCase();

          let status = 'pending';
          const jsonKey = jsonMap.get(baseName);

          if (jsonKey) {
            try {
              const obj = await s3.send(
                new GetObjectCommand({
                  Bucket: BUCKET_NAME,
                  Key: jsonKey
                })
              );

              const body = await streamToString(obj.Body);
              console.log('JSON RAW:', jsonKey, body);

              const json = JSON.parse(body);

              const st = json?.st || json?.status;

              if (st === 'validated' || st === 'rejected') {
                status = st;
              } else {
                console.error('INVALID STATUS FIELD:', json);
              }

            } catch (err) {
              console.error('ERROR reading JSON:', jsonKey, err);
              status = 'pending';
            }
          }

          return {
            name,
            sizeBytes: item.Size ?? 0,
            lastModified: item.LastModified
              ? new Date(item.LastModified).toISOString()
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
    console.error('FATAL ERROR:', error);

    return buildJsonResponse(500, {
      error: 'Internal server error'
    });
  }
};