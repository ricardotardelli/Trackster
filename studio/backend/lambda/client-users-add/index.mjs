import {
  AdminAddUserToGroupCommand,
  AdminCreateUserCommand,
  AdminDeleteUserCommand,
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  CognitoIdentityProviderClient,
  GetGroupCommand
} from '@aws-sdk/client-cognito-identity-provider';

const USER_POOL_ID = process.env.COGNITO_USER_POOL_ID || '';
const REGION = process.env.AWS_REGION || process.env.REGION || 'eu-west-1';
const TRACKSTER_ADMINS_GROUP = 'trackster-admins';
const CLIENT_ID_PATTERN = /^\d{8}$/;

const cognito = new CognitoIdentityProviderClient({ region: REGION });

const headers = {
  'Access-Control-Allow-Origin': process.env.ALLOWED_ORIGIN || '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS,POST',
  'Content-Type': 'application/json'
};

export const handler = async (event) => {
  const method = getMethod(event);

  console.log('Trackster add client user request', {
    method,
    path: event?.rawPath || event?.path,
    requestId: event?.requestContext?.requestId || null
  });

  if (method === 'OPTIONS') return reply(200, { success: true });
  if (method !== 'POST') {
    return reply(405, {
      success: false,
      message: 'Method not allowed.',
      receivedMethod: method || null
    });
  }

  let createdUsername = '';

  try {
    if (!USER_POOL_ID) {
      throw new Error('Missing required environment variable COGNITO_USER_POOL_ID.');
    }

    const authenticatedUsername = getAuthenticatedUsername(event);
    if (!authenticatedUsername) {
      return reply(401, {
        success: false,
        message: 'Authenticated username not found in token.'
      });
    }

    const body = parseBody(event);
    const username = clean(body.username);
    const email = clean(body.email).toLowerCase();
    const fullName = clean(body.fullName);
    const requestedClientId = clean(body.clientId);
    const requestedRole = normalizeRole(body.role);
    const temporaryPassword = clean(body.temporaryPassword);

    const validationError = validateInput({
      username,
      email,
      fullName,
      requestedClientId,
      requestedRole
    });

    if (validationError) {
      return reply(400, { success: false, message: validationError });
    }

    const requester = await getRequester(authenticatedUsername);
    if (!requester) {
      return reply(403, {
        success: false,
        message: 'Authenticated user was not found in Cognito.'
      });
    }

    if (!requester.active) {
      return reply(403, {
        success: false,
        message: 'Your administrator account is inactive.'
      });
    }

    const authorization = await resolveTargetClient(requester, requestedClientId);
    if (!authorization.success) {
      return reply(authorization.statusCode, {
        success: false,
        message: authorization.message
      });
    }

    const clientId = authorization.clientId;
    const client = await getClientContext(clientId);

    if (!client.exists) {
      return reply(404, { success: false, message: 'Client not found.' });
    }

    if (client.status !== 'active') {
      return reply(400, {
        success: false,
        message: 'Cannot create users for an inactive client.'
      });
    }

    const groupName = requestedRole === 'client_admin'
      ? `${clientId}-admins`
      : `${clientId}-users`;

    if (!(await getGroupOrNull(groupName))) {
      return reply(404, {
        success: false,
        message: 'The target Cognito group was not found.'
      });
    }

    const createInput = {
      UserPoolId: USER_POOL_ID,
      Username: username,
      UserAttributes: [
        { Name: 'email', Value: email },
        { Name: 'name', Value: fullName }
      ],
      DesiredDeliveryMediums: ['EMAIL']
    };

    if (temporaryPassword) {
      createInput.TemporaryPassword = temporaryPassword;
    }

    const created = await cognito.send(new AdminCreateUserCommand(createInput));
    createdUsername = created.User?.Username || username;

    await cognito.send(new AdminAddUserToGroupCommand({
      UserPoolId: USER_POOL_ID,
      Username: createdUsername,
      GroupName: groupName
    }));

    return reply(200, {
      success: true,
      message: 'User created successfully.',
      externalLoginCreated: true,
      temporaryPasswordCreated: Boolean(temporaryPassword),
      createdUser: {
        username: createdUsername,
        email,
        fullName,
        globalRole: null,
        clientRole: requestedRole,
        role: requestedRole,
        clientId,
        status: 'active'
      }
    });
  } catch (error) {
    if (createdUsername) {
      try {
        await cognito.send(new AdminDeleteUserCommand({
          UserPoolId: USER_POOL_ID,
          Username: createdUsername
        }));
      } catch (cleanupError) {
        console.error('Unable to cleanup Cognito user after failure', cleanupError);
      }
    }

    console.error('Unable to create Trackster client user', {
      name: error?.name,
      message: error?.message,
      stack: error?.stack
    });

    if (error?.name === 'UsernameExistsException') {
      return reply(409, {
        success: false,
        message: 'A user with this username already exists.',
        cognitoError: error?.name
      });
    }

    if (error?.name === 'AliasExistsException') {
      return reply(409, {
        success: false,
        message: 'This email is already verified as an alias for another user.',
        cognitoError: error?.name
      });
    }

    if (error?.name === 'InvalidPasswordException' || error?.name === 'InvalidParameterException') {
      return reply(400, {
        success: false,
        message: error?.message || 'Invalid Cognito user data.'
      });
    }

    return reply(500, {
      success: false,
      message: error?.message || 'Internal server error while creating user.'
    });
  }
};

function getMethod(event) {
  return String(
    event?.httpMethod ||
    event?.requestContext?.http?.method ||
    event?.requestContext?.httpMethod ||
    ''
  ).toUpperCase();
}

function getAuthenticatedUsername(event) {
  const claims =
    event?.requestContext?.authorizer?.jwt?.claims ||
    event?.requestContext?.authorizer?.claims ||
    {};

  return clean(
    claims['cognito:username'] ||
    claims.username ||
    claims.preferred_username ||
    claims.email ||
    ''
  );
}

function parseBody(event) {
  if (!event?.body) return {};
  if (typeof event.body === 'object') return event.body;

  const text = event.isBase64Encoded
    ? Buffer.from(event.body, 'base64').toString('utf8')
    : event.body;

  return JSON.parse(text);
}

function clean(value) {
  return String(value || '').trim();
}

function normalizeRole(value) {
  const role = clean(value).toLowerCase();
  if (role === 'client_admin' || role === 'client_user') return role;
  return '';
}

function validateInput({ username, email, fullName, requestedClientId, requestedRole }) {
  if (!username) return 'Missing required field: username.';
  if (!email) return 'Missing required field: email.';
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return 'Invalid email.';
  if (!fullName) return 'Missing required field: fullName.';
  if (!requestedRole) return 'Invalid role. Expected client_admin or client_user.';
  if (requestedClientId && !CLIENT_ID_PATTERN.test(requestedClientId)) {
    return 'clientId must contain exactly 8 digits.';
  }
  return '';
}

async function getRequester(username) {
  try {
    const [user, groups] = await Promise.all([
      cognito.send(new AdminGetUserCommand({ UserPoolId: USER_POOL_ID, Username: username })),
      listGroupsForUser(username)
    ]);

    return {
      active: user.Enabled === true && user.UserStatus !== 'ARCHIVED',
      groups
    };
  } catch (error) {
    if (error?.name === 'UserNotFoundException') return null;
    throw error;
  }
}

async function listGroupsForUser(username) {
  const names = [];
  let nextToken;

  do {
    const result = await cognito.send(new AdminListGroupsForUserCommand({
      UserPoolId: USER_POOL_ID,
      Username: username,
      Limit: 60,
      NextToken: nextToken
    }));

    for (const group of result.Groups || []) {
      if (group.GroupName) names.push(group.GroupName);
    }

    nextToken = result.NextToken;
  } while (nextToken);

  return names;
}

async function resolveTargetClient(requester, requestedClientId) {
  if (requester.groups.includes(TRACKSTER_ADMINS_GROUP)) {
    if (!requestedClientId) {
      return {
        success: false,
        statusCode: 400,
        message: 'Missing required field: clientId.'
      };
    }

    return { success: true, clientId: requestedClientId };
  }

  const clientIds = requester.groups
    .map((group) => clean(group).match(/^(\d{8})-admins$/)?.[1] || '')
    .filter(Boolean);

  if (!clientIds.length) {
    return {
      success: false,
      statusCode: 403,
      message: 'Access denied. Only administrators can create users.'
    };
  }

  if (requestedClientId && !clientIds.includes(requestedClientId)) {
    return {
      success: false,
      statusCode: 403,
      message: 'Access denied. Administrators can only create users for their own client.'
    };
  }

  if (!requestedClientId && clientIds.length > 1) {
    return {
      success: false,
      statusCode: 400,
      message: 'Missing required field: clientId.'
    };
  }

  const clientId = requestedClientId || clientIds[0];
  const client = await getClientContext(clientId);

  if (!client.exists || client.status !== 'active') {
    return {
      success: false,
      statusCode: 403,
      message: 'Your administrator account is inactive.'
    };
  }

  return { success: true, clientId };
}

async function getClientContext(clientId) {
  const [adminGroup, userGroup] = await Promise.all([
    getGroupOrNull(`${clientId}-admins`),
    getGroupOrNull(`${clientId}-users`)
  ]);

  if (!adminGroup && !userGroup) return { exists: false, status: '' };

  const status =
    parseGroupStatus(adminGroup?.Description) ||
    parseGroupStatus(userGroup?.Description) ||
    'active';

  return { exists: true, status };
}

function parseGroupStatus(description) {
  const raw = clean(description);
  if (!raw) return '';

  try {
    const parsed = JSON.parse(raw);
    const status = clean(parsed?.status).toLowerCase();
    if (status === 'inactive' || status === 'suspended') return status;
    return 'active';
  } catch {
    return 'active';
  }
}

async function getGroupOrNull(groupName) {
  try {
    return await cognito.send(new GetGroupCommand({
      UserPoolId: USER_POOL_ID,
      GroupName: groupName
    }));
  } catch (error) {
    if (error?.name === 'ResourceNotFoundException') return null;
    throw error;
  }
}

function reply(statusCode, body) {
  return {
    statusCode,
    headers,
    body: JSON.stringify(body)
  };
}
