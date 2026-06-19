import pg from 'pg';
import {
  LambdaClient,
  InvokeCommand
} from '@aws-sdk/client-lambda';

const { Client } = pg;

const lambdaClient = new LambdaClient({
  region: process.env.AWS_REGION || process.env.LAMBDA_REGION || 'us-east-1'
});

const defaultHeaders = {
  'Access-Control-Allow-Origin': process.env.ALLOWED_ORIGIN || '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS,POST'
};

function response(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}

function getMethod(event) {
  return (
    event?.httpMethod ||
    event?.requestContext?.http?.method ||
    event?.requestContext?.httpMethod ||
    ''
  ).toUpperCase();
}

function getAuthenticatedUsername(event) {
  const claims =
    event?.requestContext?.authorizer?.claims ||
    event?.requestContext?.authorizer?.jwt?.claims ||
    {};

  return (
    claims['cognito:username'] ||
    claims.username ||
    null
  );
}

function parseBody(event) {
  if (!event.body) {
    return {};
  }

  if (event.isBase64Encoded) {
    return JSON.parse(Buffer.from(event.body, 'base64').toString('utf8'));
  }

  return JSON.parse(event.body);
}

async function createDbClient() {
  const client = new Client({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT || 5432),
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
  });

  await client.connect();
  return client;
}

async function deleteUserFromIdentityProviderIfExists(username) {
  const functionName = process.env.IDENTITY_DELETE_FUNCTION_NAME;

  if (!functionName) {
    throw new Error('Missing required environment variable: IDENTITY_DELETE_FUNCTION_NAME');
  }

  const command = new InvokeCommand({
    FunctionName: functionName,
    InvocationType: 'RequestResponse',
    Payload: Buffer.from(JSON.stringify({ username }))
  });

  const result = await lambdaClient.send(command);

  if (result.FunctionError) {
    const payloadText = result.Payload
      ? Buffer.from(result.Payload).toString('utf8')
      : '';

    console.error('Identity delete function error:', payloadText);

    throw new Error('Unable to delete external login account.');
  }

  const payloadText = result.Payload
    ? Buffer.from(result.Payload).toString('utf8')
    : '{}';

  const payload = JSON.parse(payloadText);

  if (!payload.success) {
    throw new Error(payload.message || 'Unable to delete external login account.');
  }

  return {
    deleted: !!payload.externalLoginDeleted,
    wasMissing: !!payload.externalLoginWasMissing
  };
}

export const handler = async (event) => {
  const method = getMethod(event);

  if (method === 'OPTIONS') {
    return response(200, { success: true });
  }

  if (method !== 'POST') {
    return response(405, {
      success: false,
      message: 'Method not allowed.',
      receivedMethod: method || null
    });
  }

  let client;

  try {
    const authenticatedUsername = getAuthenticatedUsername(event);

    if (!authenticatedUsername) {
      return response(401, {
        success: false,
        message: 'Authenticated username not found in token.'
      });
    }

    const body = parseBody(event);
    const targetUsername = String(body.username || '').trim();

    if (!targetUsername) {
      return response(400, {
        success: false,
        message: 'Missing required field: username.'
      });
    }

    if (targetUsername === authenticatedUsername) {
      return response(400, {
        success: false,
        message: 'You cannot delete your own user.'
      });
    }

    client = await createDbClient();

    await client.query('BEGIN');

    const requesterResult = await client.query(
      `
      SELECT
        u.id,
        u.username,
        u.email,
        u.full_name,
        u.status AS user_status,
        gr.role_code AS global_role_code,
        cu.client_id,
        cr.role_code AS client_role_code,
        cu.status AS client_user_status
      FROM trackster_users u
      LEFT JOIN trackster_roles gr
        ON gr.id = u.global_role_id
      LEFT JOIN trackster_client_users cu
        ON cu.user_id = u.id
      LEFT JOIN trackster_roles cr
        ON cr.id = cu.role_id
      WHERE u.username = $1
      LIMIT 1
      `,
      [authenticatedUsername]
    );

    if (requesterResult.rowCount === 0) {
      await client.query('ROLLBACK');

      return response(403, {
        success: false,
        message: 'Authenticated user was not found in Trackster database.'
      });
    }

    const requester = requesterResult.rows[0];

    const requesterIsTracksterAdmin =
      requester.global_role_code === 'trackster_admin';

    const requesterIsClientAdmin =
      requester.client_role_code === 'client_admin';

    if (!requesterIsTracksterAdmin && !requesterIsClientAdmin) {
      await client.query('ROLLBACK');

      return response(403, {
        success: false,
        message: 'Access denied. Only administrators can delete users.'
      });
    }

    const targetResult = await client.query(
      `
      SELECT
        u.id,
        u.username,
        u.email,
        u.full_name,
        u.status AS user_status,
        gr.role_code AS global_role_code,
        cu.client_id,
        cr.role_code AS client_role_code,
        cu.status AS client_user_status
      FROM trackster_users u
      LEFT JOIN trackster_roles gr
        ON gr.id = u.global_role_id
      LEFT JOIN trackster_client_users cu
        ON cu.user_id = u.id
      LEFT JOIN trackster_roles cr
        ON cr.id = cu.role_id
      WHERE u.username = $1
      LIMIT 1
      `,
      [targetUsername]
    );

    if (targetResult.rowCount === 0) {
      await client.query('ROLLBACK');

      return response(404, {
        success: false,
        message: 'Target user not found.'
      });
    }

    const target = targetResult.rows[0];

    if (
      requesterIsClientAdmin &&
      requester.client_id !== target.client_id
    ) {
      await client.query('ROLLBACK');

      return response(403, {
        success: false,
        message: 'Access denied. Administrators can only delete users from their own client.'
      });
    }

    if (
      requesterIsClientAdmin &&
      target.global_role_code === 'trackster_admin'
    ) {
      await client.query('ROLLBACK');

      return response(403, {
        success: false,
        message: 'Access denied. This user cannot be deleted by a client administrator.'
      });
    }

    if (target.client_role_code === 'client_admin') {
      const remainingAdminsResult = await client.query(
        `
        SELECT COUNT(*)::int AS total
        FROM trackster_client_users cu
        JOIN trackster_roles r
          ON r.id = cu.role_id
        WHERE cu.client_id = $1
          AND r.role_code = 'client_admin'
          AND cu.status = 'active'
          AND cu.user_id <> $2
        `,
        [target.client_id, target.id]
      );

      const remainingAdmins = remainingAdminsResult.rows[0].total;

      if (remainingAdmins < 1) {
        await client.query('ROLLBACK');

        return response(400, {
          success: false,
          message: 'Cannot delete the last active client administrator for this client.'
        });
      }
    }

    const identityDeleteResult = await deleteUserFromIdentityProviderIfExists(target.username);

    await client.query(
      `
      DELETE FROM trackster_client_users
      WHERE user_id = $1
      `,
      [target.id]
    );

    await client.query(
      `
      DELETE FROM trackster_users
      WHERE id = $1
      `,
      [target.id]
    );

    await client.query('COMMIT');

    const finalMessage = identityDeleteResult.wasMissing
      ? 'User was removed from the application database. The external login account was already unavailable.'
      : 'User deleted successfully.';

    return response(200, {
      success: true,
      message: finalMessage,
      externalLoginDeleted: identityDeleteResult.deleted,
      externalLoginWasMissing: identityDeleteResult.wasMissing,
      deletedUser: {
        username: target.username,
        email: target.email,
        fullName: target.full_name,
        globalRole: target.global_role_code,
        clientRole: target.client_role_code,
        clientId: target.client_id
      }
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch {}
    }

    console.error('delete-trackster-user error:', error);

    return response(500, {
      success: false,
      message: error?.message || 'Internal server error while deleting user.'
    });
  } finally {
    if (client) {
      await client.end();
    }
  }
};