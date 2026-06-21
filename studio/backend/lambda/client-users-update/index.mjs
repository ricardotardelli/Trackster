import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import pg from 'pg';

const { Client } = pg;

const lambdaClient = new LambdaClient({
  region: process.env.AWS_REGION || process.env.LAMBDA_REGION || 'us-east-1'
});

const defaultHeaders = {
  'Access-Control-Allow-Origin': process.env.ALLOWED_ORIGIN || '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS,POST',
  'Content-Type': 'application/json'
};

const validRoles = ['trackster_admin', 'client_admin', 'client_user'];
const validStatuses = ['active', 'inactive', 'suspended'];

export const handler = async (event) => {
  if (event.requestContext?.http?.method === 'OPTIONS' || event.httpMethod === 'OPTIONS') {
    return buildResponse(200, { success: true });
  }

  if (event.requestContext?.http?.method !== 'POST' && event.httpMethod !== 'POST') {
    return buildResponse(405, {
      success: false,
      message: 'Method not allowed.'
    });
  }

  let body;

  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return buildResponse(400, {
      success: false,
      message: 'Invalid JSON body.'
    });
  }

  const username = normalizeString(body.username);
  const clientId = normalizeString(body.clientId);
  const fullName = normalizeString(body.fullName);
  const email = normalizeString(body.email).toLowerCase();
  const role = normalizeString(body.role);
  const status = normalizeStatus(body.status);

  if (!username || !clientId || !fullName || !email || !role || !status) {
    return buildResponse(400, {
      success: false,
      message: 'username, clientId, fullName, email, role and status are required.'
    });
  }

  if (!email.includes('@')) {
    return buildResponse(400, {
      success: false,
      message: 'Invalid email.'
    });
  }

  if (!validRoles.includes(role)) {
    return buildResponse(400, {
      success: false,
      message: 'Invalid role.'
    });
  }

  if (!validStatuses.includes(status)) {
    return buildResponse(400, {
      success: false,
      message: 'Invalid status.'
    });
  }

  const identityUpdateUserLambdaName = normalizeString(process.env.IDENTITY_UPDATE_USER_LAMBDA_NAME);

  if (!identityUpdateUserLambdaName) {
    return buildResponse(500, {
      success: false,
      message: 'IDENTITY_UPDATE_USER_LAMBDA_NAME environment variable is not configured.'
    });
  }

  const dbClient = new Client({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT || 5432),
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    ssl: process.env.DB_SSL === 'true'
      ? { rejectUnauthorized: false }
      : undefined
  });

  let transactionStarted = false;

  try {
    await dbClient.connect();
    await dbClient.query('BEGIN');
    transactionStarted = true;

    const userResult = await dbClient.query(
      `
      SELECT
        u.id AS user_id,
        u.username,
        u.email,
        u.full_name,
        u.status,
        u.role_id AS current_role_id,
        r.role_code AS current_role,
        c.client_id
      FROM trackster_users u
      INNER JOIN trackster_client_users cu
        ON cu.user_id = u.id
      INNER JOIN trackster_clients c
        ON c.client_id = cu.client_id
      INNER JOIN trackster_roles r
        ON r.id = u.role_id
      WHERE LOWER(u.username) = LOWER($1)
        AND c.client_id = $2
      LIMIT 1
      `,
      [username, clientId]
    );

    if (userResult.rowCount === 0) {
      await rollbackIfNeeded(dbClient, transactionStarted);

      return buildResponse(404, {
        success: false,
        message: 'User was not found for the selected client.'
      });
    }

    const existingUser = userResult.rows[0];
    const userId = existingUser.user_id;
    const currentRole = existingUser.current_role;

    const roleResult = await dbClient.query(
      `
      SELECT id, role_code
      FROM trackster_roles
      WHERE role_code = $1
      LIMIT 1
      `,
      [role]
    );

    if (roleResult.rowCount === 0) {
      await rollbackIfNeeded(dbClient, transactionStarted);

      return buildResponse(400, {
        success: false,
        message: 'Role was not found.'
      });
    }

    const nextRoleId = roleResult.rows[0].id;

    if (
      currentRole === 'client_admin'
      && (role !== 'client_admin' || status !== 'active')
    ) {
      const activeClientAdminsResult = await dbClient.query(
        `
        SELECT COUNT(*)::int AS total
        FROM trackster_client_users cu
        INNER JOIN trackster_users u
          ON u.id = cu.user_id
        INNER JOIN trackster_roles r
          ON r.id = u.role_id
        WHERE cu.client_id = $1
          AND r.role_code = 'client_admin'
          AND u.status = 'active'
          AND u.id <> $2
        `,
        [clientId, userId]
      );

      const remainingActiveClientAdmins = activeClientAdminsResult.rows[0]?.total || 0;

      if (remainingActiveClientAdmins === 0) {
        await rollbackIfNeeded(dbClient, transactionStarted);

        return buildResponse(409, {
          success: false,
          message: 'The selected client must keep at least one active client administrator.'
        });
      }
    }

    await dbClient.query(
      `
      UPDATE trackster_users
      SET
        email = $1,
        full_name = $2,
        role_id = $3,
        status = $4,
        updated_at = NOW()
      WHERE id = $5
      `,
      [
        email,
        fullName,
        nextRoleId,
        status,
        userId
      ]
    );

    await dbClient.query(
      `
      UPDATE trackster_client_users
      SET updated_at = NOW()
      WHERE client_id = $1
        AND user_id = $2
      `,
      [
        clientId,
        userId
      ]
    );

    const identityResponse = await updateExternalLoginAccount({
      lambdaName: identityUpdateUserLambdaName,
      username,
      email,
      fullName,
      status
    });

    if (!identityResponse.success) {
      await rollbackIfNeeded(dbClient, transactionStarted);

      return buildResponse(502, {
        success: false,
        message: identityResponse.message || 'Unable to update external login account.',
        externalLoginUpdated: false
      });
    }

    await dbClient.query('COMMIT');
    transactionStarted = false;

    return buildResponse(200, {
      success: true,
      message: 'User updated successfully.',
      externalLoginUpdated: true,
      updatedUser: {
        username,
        email,
        fullName,
        clientRole: role === 'trackster_admin' ? null : role,
        globalRole: role === 'trackster_admin' ? 'trackster_admin' : null,
        role,
        clientId,
        status
      }
    });
  } catch (error) {
    console.error('client-user-update error:', error);

    await rollbackIfNeeded(dbClient, transactionStarted);

    return buildResponse(500, {
      success: false,
      message: error?.message || 'Unable to update user.'
    });
  } finally {
    try {
      await dbClient.end();
    } catch (closeError) {
      console.error('Unable to close database connection.', closeError);
    }
  }
};

async function updateExternalLoginAccount(payload) {
  const result = await lambdaClient.send(
    new InvokeCommand({
      FunctionName: payload.lambdaName,
      InvocationType: 'RequestResponse',
      Payload: Buffer.from(JSON.stringify({
        username: payload.username,
        email: payload.email,
        fullName: payload.fullName,
        status: payload.status
      }))
    })
  );

  const rawPayload = result.Payload
    ? Buffer.from(result.Payload).toString('utf-8')
    : '{}';

  let parsedPayload;

  try {
    parsedPayload = JSON.parse(rawPayload || '{}');
  } catch {
    return {
      success: false,
      message: 'Invalid response from identity update Lambda.'
    };
  }

  if (result.FunctionError) {
    return {
      success: false,
      message: parsedPayload?.message || 'Identity update Lambda failed.'
    };
  }

  return parsedPayload;
}

async function rollbackIfNeeded(dbClient, transactionStarted) {
  if (!transactionStarted) {
    return;
  }

  try {
    await dbClient.query('ROLLBACK');
  } catch (rollbackError) {
    console.error('Rollback failed.', rollbackError);
  }
}

function normalizeString(value) {
  return String(value || '').trim();
}

function normalizeStatus(value) {
  return normalizeString(value).toLowerCase();
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}