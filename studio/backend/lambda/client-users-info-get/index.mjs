import {
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  CognitoIdentityProviderClient,
  GetGroupCommand,
  ListUsersInGroupCommand
} from "@aws-sdk/client-cognito-identity-provider";

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";
const userPoolId = process.env.COGNITO_USER_POOL_ID || "";
const region = process.env.AWS_REGION || process.env.REGION || "eu-west-1";

const TRACKSTER_ADMINS_GROUP = "trackster-admins";
const CLIENT_ID_PATTERN = /^\d{8}$/;

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,GET",
  "Content-Type": "application/json"
};

const cognitoClient = new CognitoIdentityProviderClient({ region });

export const handler = async (event) => {
  console.log("Trackster admin list client users request", {
    method: event?.requestContext?.http?.method || event?.httpMethod,
    path: event?.rawPath || event?.path,
    queryStringParameters: event?.queryStringParameters || {}
  });

  const method = event?.requestContext?.http?.method || event?.httpMethod;

  if (method === "OPTIONS") {
    return buildResponse(200, { success: true });
  }

  if (method !== "GET") {
    return buildResponse(405, {
      success: false,
      error: "Method not allowed."
    });
  }

  try {
    validateEnvironment();

    const username = getAuthenticatedUsername(event);

    if (!username) {
      return buildResponse(401, {
        success: false,
        error: "Authenticated username was not found in token claims."
      });
    }

    const authenticatedUser = await getAuthenticatedUserContext(username);

    if (!authenticatedUser) {
      return buildResponse(404, {
        success: false,
        error: "Authenticated user was not found in Cognito."
      });
    }

    if (authenticatedUser.status !== "active") {
      return buildResponse(403, {
        success: false,
        error: "Authenticated user is not active."
      });
    }

    const requestedClientId = String(
      event?.queryStringParameters?.clientId || ""
    ).trim();

    const authorizationResult = await resolveAuthorizedClientId(
      authenticatedUser,
      requestedClientId
    );

    if (!authorizationResult.success) {
      return buildResponse(authorizationResult.statusCode, {
        success: false,
        error: authorizationResult.error
      });
    }

    const users = await listClientUsers(authorizationResult.clientId);

    return buildResponse(200, {
      success: true,
      clientId: authorizationResult.clientId,
      users
    });
  } catch (error) {
    console.error("Unable to list Trackster client users", {
      name: error?.name,
      message: error?.message,
      stack: error?.stack
    });

    return buildResponse(500, {
      success: false,
      error: "Unable to list client users."
    });
  }
};

function validateEnvironment() {
  if (!userPoolId) {
    throw new Error(
      "Missing required environment variable COGNITO_USER_POOL_ID."
    );
  }
}

function getAuthenticatedUsername(event) {
  const httpApiClaims = event?.requestContext?.authorizer?.jwt?.claims;
  const restApiClaims = event?.requestContext?.authorizer?.claims;
  const claims = httpApiClaims || restApiClaims || {};

  return String(
    claims["cognito:username"] ||
    claims.username ||
    claims.preferred_username ||
    claims.email ||
    ""
  ).trim();
}

async function getAuthenticatedUserContext(username) {
  try {
    const [userResponse, groupNames] = await Promise.all([
      cognitoClient.send(
        new AdminGetUserCommand({
          UserPoolId: userPoolId,
          Username: username
        })
      ),
      listGroupsForUser(username)
    ]);

    return {
      username: userResponse.Username || username,
      email: getAttributeValue(userResponse.UserAttributes, "email"),
      fullName: getFullName(userResponse.UserAttributes),
      status: isUserActive(userResponse) ? "active" : "inactive",
      groupNames
    };
  } catch (error) {
    if (error?.name === "UserNotFoundException") {
      return null;
    }

    throw error;
  }
}

async function listGroupsForUser(username) {
  const groupNames = [];
  let nextToken;

  do {
    const response = await cognitoClient.send(
      new AdminListGroupsForUserCommand({
        UserPoolId: userPoolId,
        Username: username,
        Limit: 60,
        NextToken: nextToken
      })
    );

    for (const group of response.Groups || []) {
      if (group.GroupName) {
        groupNames.push(group.GroupName);
      }
    }

    nextToken = response.NextToken;
  } while (nextToken);

  return groupNames;
}

async function resolveAuthorizedClientId(
  authenticatedUser,
  requestedClientId
) {
  const isTracksterAdmin = authenticatedUser.groupNames.includes(
    TRACKSTER_ADMINS_GROUP
  );

  if (isTracksterAdmin) {
    if (!requestedClientId) {
      return {
        success: false,
        statusCode: 400,
        error: "clientId is required for trackster_admin users."
      };
    }

    if (!CLIENT_ID_PATTERN.test(requestedClientId)) {
      return {
        success: false,
        statusCode: 400,
        error: "clientId must contain exactly 8 digits."
      };
    }

    const clientContext = await getClientContext(requestedClientId);

    if (!clientContext.exists) {
      return {
        success: false,
        statusCode: 404,
        error: "Client was not found."
      };
    }

    return {
      success: true,
      clientId: requestedClientId
    };
  }

  const clientAdminIds = authenticatedUser.groupNames
    .map(extractClientIdFromAdminGroup)
    .filter(Boolean);

  if (clientAdminIds.length === 0) {
    return {
      success: false,
      statusCode: 403,
      error:
        "Only trackster_admin or active client_admin users can list client users."
    };
  }

  let authorizedClientId;

  if (requestedClientId) {
    if (!CLIENT_ID_PATTERN.test(requestedClientId)) {
      return {
        success: false,
        statusCode: 400,
        error: "clientId must contain exactly 8 digits."
      };
    }

    if (!clientAdminIds.includes(requestedClientId)) {
      return {
        success: false,
        statusCode: 403,
        error: "client_admin users cannot list users from another client."
      };
    }

    authorizedClientId = requestedClientId;
  } else if (clientAdminIds.length === 1) {
    authorizedClientId = clientAdminIds[0];
  } else {
    return {
      success: false,
      statusCode: 400,
      error:
        "clientId is required when the authenticated user administers more than one client."
    };
  }

  const clientContext = await getClientContext(authorizedClientId);

  if (!clientContext.exists || clientContext.status !== "active") {
    return {
      success: false,
      statusCode: 403,
      error:
        "Only trackster_admin or active client_admin users can list client users."
    };
  }

  return {
    success: true,
    clientId: authorizedClientId
  };
}

function extractClientIdFromAdminGroup(groupName) {
  const match = String(groupName || "").match(/^(\d{8})-admins$/);
  return match ? match[1] : "";
}

async function getClientContext(clientId) {
  const adminGroupName = `${clientId}-admins`;
  const userGroupName = `${clientId}-users`;

  const [adminGroup, userGroup] = await Promise.all([
    getGroupOrNull(adminGroupName),
    getGroupOrNull(userGroupName)
  ]);

  if (!adminGroup && !userGroup) {
    return {
      exists: false,
      status: ""
    };
  }

  const adminMetadata = parseClientDescription(adminGroup?.Description);
  const userMetadata = parseClientDescription(userGroup?.Description);

  return {
    exists: true,
    status: normalizeClientStatus(
      adminMetadata.status ||
      userMetadata.status ||
      "active"
    )
  };
}

async function getGroupOrNull(groupName) {
  try {
    return await cognitoClient.send(
      new GetGroupCommand({
        UserPoolId: userPoolId,
        GroupName: groupName
      })
    );
  } catch (error) {
    if (error?.name === "ResourceNotFoundException") {
      return null;
    }

    throw error;
  }
}

function parseClientDescription(description) {
  const rawDescription = String(description || "").trim();

  if (!rawDescription) {
    return { status: "" };
  }

  try {
    const parsed = JSON.parse(rawDescription);

    if (
      parsed &&
      typeof parsed === "object" &&
      !Array.isArray(parsed)
    ) {
      return {
        status: String(parsed.status || "")
          .trim()
          .toLowerCase()
      };
    }
  } catch {
    // Legacy description: company name only.
  }

  return { status: "active" };
}

function normalizeClientStatus(status) {
  const normalized = String(status || "")
    .trim()
    .toLowerCase();

  if (normalized === "inactive" || normalized === "suspended") {
    return normalized;
  }

  return "active";
}

async function listClientUsers(clientId) {
  const adminGroupName = `${clientId}-admins`;
  const userGroupName = `${clientId}-users`;

  const [adminUsers, regularUsers] = await Promise.all([
    listUsersInGroup(adminGroupName),
    listUsersInGroup(userGroupName)
  ]);

  const usersByUsername = new Map();

  for (const user of regularUsers) {
    const normalized = normalizeUser(
      user,
      "client_user",
      clientId
    );

    usersByUsername.set(
      normalized.username.toLowerCase(),
      normalized
    );
  }

  for (const user of adminUsers) {
    const normalized = normalizeUser(
      user,
      "client_admin",
      clientId
    );

    usersByUsername.set(
      normalized.username.toLowerCase(),
      normalized
    );
  }

  return Array.from(usersByUsername.values()).sort(compareUsers);
}

async function listUsersInGroup(groupName) {
  const users = [];
  let nextToken;

  do {
    try {
      const response = await cognitoClient.send(
        new ListUsersInGroupCommand({
          UserPoolId: userPoolId,
          GroupName: groupName,
          Limit: 60,
          NextToken: nextToken
        })
      );

      users.push(...(response.Users || []));
      nextToken = response.NextToken;
    } catch (error) {
      if (error?.name === "ResourceNotFoundException") {
        return [];
      }

      throw error;
    }
  } while (nextToken);

  return users;
}

function normalizeUser(user, role, clientId) {
  const attributes = user.Attributes || [];

  return {
    username: user.Username || "",
    fullName: getFullName(attributes),
    email: getAttributeValue(attributes, "email"),
    role,
    status: toUiUserStatus(user, attributes),
    clientId
  };
}

function getFullName(attributes) {
  return (
    getAttributeValue(attributes, "name") ||
    getAttributeValue(attributes, "custom:fullName") ||
    [
      getAttributeValue(attributes, "given_name"),
      getAttributeValue(attributes, "family_name")
    ]
      .filter(Boolean)
      .join(" ")
  );
}

function getAttributeValue(attributes, attributeName) {
  const attribute = (attributes || []).find(
    (item) => item.Name === attributeName
  );

  return attribute?.Value || "";
}

function isUserActive(user) {
  return (
    user?.Enabled === true &&
    user?.UserStatus !== "ARCHIVED"
  );
}

function toUiUserStatus(user, attributes) {
  const storedStatus = String(
    getAttributeValue(attributes, "custom:status") || ""
  )
    .trim()
    .toLowerCase();

  if (storedStatus === "suspended") {
    return "Suspended";
  }

  if (!isUserActive(user)) {
    return "Inactive";
  }

  return "Active";
}

function compareUsers(first, second) {
  if (first.role !== second.role) {
    return first.role === "client_admin" ? -1 : 1;
  }

  return first.username.localeCompare(
    second.username,
    undefined,
    { sensitivity: "base" }
  );
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}
