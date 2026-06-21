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

function normalizeString(value) {
  return String(value || '').trim();
}

function normalizeRole(value) {
  const role = normalizeString(value);

  if (role === 'client_admin') {
    return 'client_admin';
  }

  return 'client_user';
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

async function createUserInIdentityProvider(identityPayload) {
  const functionName = process.env.IDENTITY_CREATE_FUNCTION_NAME;

  if (!functionName) {
    throw new Error('Missing required environment variable: IDENTITY_CREATE_FUNCTION_NAME');
  }

  const command = new InvokeCommand({
    FunctionName: functionName,
    InvocationType: 'RequestResponse',
    Payload: Buffer.from(JSON.stringify(identityPayload))
  });

  const result = await lambdaClient.send(command);

  if (result.FunctionError) {
    const resultPayloadText = result.Payload
      ? Buffer.from(result.Payload).toString('utf8')
      : '';

    console.error('Identity create function error:', resultPayloadText);

    throw new Error('Unable to create external login account.');
  }

  const resultPayloadText = result.Payload
    ? Buffer.from(result.Payload).toString('utf8')
    : '{}';

  const parsedIdentityResponse = JSON.parse(resultPayloadText);

  if (!parsedIdentityResponse.success) {
    throw new Error(parsedIdentityResponse.message || 'Unable to create external login account.');
  }

  return parsedIdentityResponse;
}

async function deleteUserFromIdentityProviderIfExists(username) {
  const functionName = process.env.IDENTITY_DELETE_FUNCTION_NAME;

  if (!functionName) {
    return {
      deleted: false,
      cleanupSkipped: true
    };
  }

  const command = new InvokeCommand({
    FunctionName: functionName,
    InvocationType: 'RequestResponse',
    Payload: Buffer.from(JSON.stringify({ username }))
  });

  const result = await lambdaClient.send(command);

  if (result.FunctionError) {
    const resultPayloadText = result.Payload
      ? Buffer.from(result.Payload).toString('utf8')
      : '';

    console.error('Identity cleanup function error:', resultPayloadText);

    return {
      deleted: false,
      cleanupFailed: true
    };
  }

  const resultPayloadText = result.Payload
    ? Buffer.from(result.Payload).toString('utf8')
    : '{}';

  const parsedIdentityResponse = JSON.parse(resultPayloadText);

  return {
    deleted: !!parsedIdentityResponse.externalLoginDeleted,
    wasMissing: !!parsedIdentityResponse.externalLoginWasMissing
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
  let externalLoginCreated = false;
  let createdUsername = '';

  try {
    const authenticatedUsername = getAuthenticatedUsername(event);

    if (!authenticatedUsername) {
      return response(401, {
        success: false,
        message: 'Authenticated username not found in token.'
      });
    }

    const body = parseBody(event);

    const username = normalizeString(body.username);
    const email = normalizeString(body.email).toLowerCase();
    const fullName = normalizeString(body.fullName);
    const requestedClientId = normalizeString(body.clientId);
    const requestedRole = normalizeRole(body.role);
    const temporaryPassword = normalizeString(body.temporaryPassword);

    if (!username) {
      return response(400, {
        success: false,
        message: 'Missing required field: username.'
      });
    }

    if (!email) {
      return response(400, {
        success: false,
        message: 'Missing required field: email.'
      });
    }

    if (!email.includes('@')) {
      return response(400, {
        success: false,
        message: 'Invalid email.'
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
        r.role_code AS role_code,
        cu.client_id,
        c.status AS client_status
      FROM trackster_users u
      INNER JOIN trackster_roles r
        ON r.id = u.role_id
      LEFT JOIN trackster_client_users cu
        ON cu.user_id = u.id
      LEFT JOIN trackster_clients c
        ON c.client_id = cu.client_id
      WHERE LOWER(u.username) = LOWER($1)
      ORDER BY
        CASE WHEN c.status = 'active' THEN 0 ELSE 1 END,
        cu.created_at ASC
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
      requester.role_code === 'trackster_admin';

    const requesterIsClientAdmin =
      requester.role_code === 'client_admin';

    if (!requesterIsTracksterAdmin && !requesterIsClientAdmin) {
      await client.query('ROLLBACK');

      return response(403, {
        success: false,
        message: 'Access denied. Only administrators can create users.'
      });
    }

    if (
      requester.user_status !== 'active' ||
      (requesterIsClientAdmin && requester.client_status !== 'active')
    ) {
      await client.query('ROLLBACK');

      return response(403, {
        success: false,
        message: 'Your administrator account is inactive.'
      });
    }

    const targetClientId = requesterIsTracksterAdmin
      ? requestedClientId
      : requester.client_id;

    if (!targetClientId) {
      await client.query('ROLLBACK');

      return response(400, {
        success: false,
        message: 'Missing required field: clientId.'
      });
    }

    if (
      requesterIsClientAdmin &&
      requestedClientId &&
      requestedClientId !== requester.client_id
    ) {
      await client.query('ROLLBACK');

      return response(403, {
        success: false,
        message: 'Access denied. Administrators can only create users for their own client.'
      });
    }

    const clientResult = await client.query(
      `
      SELECT
        id,
        client_id,
        company_name,
        status
      FROM trackster_clients
      WHERE client_id = $1
      LIMIT 1
      `,
      [targetClientId]
    );

    if (clientResult.rowCount === 0) {
      await client.query('ROLLBACK');

      return response(404, {
        success: false,
        message: 'Client not found.'
      });
    }

    const targetClient = clientResult.rows[0];

    if (targetClient.status !== 'active') {
      await client.query('ROLLBACK');

      return response(400, {
        success: false,
        message: 'Cannot create users for an inactive client.'
      });
    }

    const existingUserResult = await client.query(
      `
      SELECT
        id,
        username
      FROM trackster_users
      WHERE LOWER(username) = LOWER($1)
      LIMIT 1
      `,
      [username]
    );

    if (existingUserResult.rowCount > 0) {
      await client.query('ROLLBACK');

      return response(409, {
        success: false,
        message: 'A user with this username already exists.'
      });
    }

    const roleResult = await client.query(
      `
      SELECT
        id,
        role_code
      FROM trackster_roles
      WHERE role_code = $1
      LIMIT 1
      `,
      [requestedRole]
    );

    if (roleResult.rowCount === 0) {
      await client.query('ROLLBACK');

      return response(400, {
        success: false,
        message: 'Requested role was not found in Trackster database.'
      });
    }

    const userRole = roleResult.rows[0];

    const identityCreateResult = await createUserInIdentityProvider({
      username,
      email,
      fullName,
      clientId: targetClient.client_id,
      temporaryPassword
    });

    externalLoginCreated = true;
    createdUsername = username;

    const insertedUserResult = await client.query(
      `
      INSERT INTO trackster_users (
        username,
        email,
        full_name,
        role_id,
        status,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        'active',
        NOW(),
        NOW()
      )
      RETURNING
        id,
        username,
        email,
        full_name,
        status
      `,
      [
        username,
        email,
        fullName,
        userRole.id
      ]
    );

    const insertedUser = insertedUserResult.rows[0];

    await client.query(
      `
      INSERT INTO trackster_client_users (
        client_id,
        user_id,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        NOW(),
        NOW()
      )
      `,
      [
        targetClient.client_id,
        insertedUser.id
      ]
    );

    await client.query('COMMIT');

    return response(200, {
      success: true,
      message: 'User created successfully.',
      externalLoginCreated: !!identityCreateResult.externalLoginCreated,
      temporaryPasswordCreated: !!identityCreateResult.temporaryPasswordCreated,
      createdUser: {
        username: insertedUser.username,
        email: insertedUser.email,
        fullName: insertedUser.full_name,
        globalRole: userRole.role_code === 'trackster_admin' ? 'trackster_admin' : null,
        clientRole: userRole.role_code !== 'trackster_admin' ? userRole.role_code : null,
        role: userRole.role_code,
        clientId: targetClient.client_id,
        status: insertedUser.status
      }
    });
  } catch (error) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch {}
    }

    if (externalLoginCreated && createdUsername) {
      try {
        await deleteUserFromIdentityProviderIfExists(createdUsername);
      } catch (cleanupError) {
        console.error('Unable to cleanup external login after database failure:', cleanupError);
      }
    }

    console.error('create-trackster-user error:', error);

    return response(500, {
      success: false,
      message: error?.message || 'Internal server error while creating user.'
    });
  } finally {
    if (client) {
      await client.end();
    }
  }
};