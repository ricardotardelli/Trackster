import {
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  CognitoIdentityProviderClient,
  ListGroupsCommand,
  ListUsersInGroupCommand
} from "@aws-sdk/client-cognito-identity-provider";

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";
const userPoolId = process.env.COGNITO_USER_POOL_ID || "";
const region = process.env.AWS_REGION || process.env.REGION || "eu-west-1";

const TRACKSTER_ADMINS_GROUP = "trackster-admins";
const CLIENT_GROUP_PATTERN = /^(\d{8})-(admins|users)$/;

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,GET",
  "Content-Type": "application/json"
};

const cognitoClient = new CognitoIdentityProviderClient({
  region
});

export const handler = async (event) => {
  console.log("Trackster admin list clients request", {
    method: event?.requestContext?.http?.method || event?.httpMethod,
    path: event?.rawPath || event?.path
  });

  const method =
    event?.requestContext?.http?.method ||
    event?.httpMethod;

  if (method === "OPTIONS") {
    return buildResponse(200, {
      success: true
    });
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
        error: "Authenticated user was not found in Trackster database."
      });
    }

    if (authenticatedUser.status !== "active") {
      return buildResponse(403, {
        success: false,
        error: "Authenticated user is not active."
      });
    }

    if (authenticatedUser.role !== "trackster_admin") {
      return buildResponse(403, {
        success: false,
        error: "Only active trackster_admin users can list clients."
      });
    }

    const clients = await listClients();

    return buildResponse(200, {
      success: true,
      clients
    });
  } catch (error) {
    console.error("Unable to list Trackster clients", {
      name: error?.name,
      message: error?.message,
      stack: error?.stack
    });

    return buildResponse(500, {
      success: false,
      error: "Unable to list clients."
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
  const httpApiClaims =
    event?.requestContext?.authorizer?.jwt?.claims;

  const restApiClaims =
    event?.requestContext?.authorizer?.claims;

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
    const [userResponse, groups] = await Promise.all([
      cognitoClient.send(
        new AdminGetUserCommand({
          UserPoolId: userPoolId,
          Username: username
        })
      ),
      listGroupsForUser(username)
    ]);

    const role = groups.includes(TRACKSTER_ADMINS_GROUP)
      ? "trackster_admin"
      : getClientRole(groups);

    return {
      id: getAttributeValue(userResponse.UserAttributes, "sub"),
      username: userResponse.Username || username,
      email: getAttributeValue(
        userResponse.UserAttributes,
        "email"
      ),
      fullName:
        getAttributeValue(userResponse.UserAttributes, "name") ||
        getAttributeValue(
          userResponse.UserAttributes,
          "custom:fullName"
        ),
      status: isCognitoUserActive(userResponse)
        ? "active"
        : "inactive",
      role
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

function getClientRole(groupNames) {
  for (const groupName of groupNames) {
    const match = CLIENT_GROUP_PATTERN.exec(groupName);

    if (!match) {
      continue;
    }

    const groupType = match[2];

    return groupType === "admins"
      ? "client_admin"
      : "client_user";
  }

  return null;
}

function isCognitoUserActive(userResponse) {
  return (
    userResponse.Enabled === true &&
    userResponse.UserStatus !== "ARCHIVED"
  );
}

async function listClients() {
  const groups = await listAllCognitoGroups();
  const clientsById = new Map();

  for (const group of groups) {
    const groupName = group.GroupName || "";
    const match = CLIENT_GROUP_PATTERN.exec(groupName);

    if (!match) {
      continue;
    }

    const clientId = match[1];
    const groupType = match[2];

    if (!clientsById.has(clientId)) {
      clientsById.set(clientId, {
        clientId,
        adminGroup: null,
        userGroup: null
      });
    }

    const clientEntry = clientsById.get(clientId);

    if (groupType === "admins") {
      clientEntry.adminGroup = group;
    } else {
      clientEntry.userGroup = group;
    }
  }

  const clients = await Promise.all(
    [...clientsById.values()].map(buildClientResponse)
  );

  return clients.sort((first, second) => {
    const nameComparison = first.name.localeCompare(
      second.name,
      undefined,
      {
        sensitivity: "base"
      }
    );

    if (nameComparison !== 0) {
      return nameComparison;
    }

    return first.clientId.localeCompare(second.clientId);
  });
}

async function listAllCognitoGroups() {
  const groups = [];
  let nextToken;

  do {
    const response = await cognitoClient.send(
      new ListGroupsCommand({
        UserPoolId: userPoolId,
        Limit: 60,
        NextToken: nextToken
      })
    );

    groups.push(...(response.Groups || []));
    nextToken = response.NextToken;
  } while (nextToken);

  return groups;
}

async function buildClientResponse(clientEntry) {
  const adminGroupName =
    clientEntry.adminGroup?.GroupName || "";

  const userGroupName =
    clientEntry.userGroup?.GroupName || "";

  const [adminUsers, regularUsers] = await Promise.all([
    adminGroupName
      ? listAllUsersInGroup(adminGroupName)
      : Promise.resolve([]),

    userGroupName
      ? listAllUsersInGroup(userGroupName)
      : Promise.resolve([])
  ]);

  /*
   * Mantém o mesmo comportamento da versão PostgreSQL:
   *
   * users = total de utilizadores distintos associados ao cliente
   * admins = total de utilizadores distintos no grupo de administradores
   *
   * A deduplicação por Username evita contagem dupla caso um utilizador
   * tenha sido colocado acidentalmente nos dois grupos.
   */
  const allUsernames = new Set();

  for (const user of adminUsers) {
    if (user.Username) {
      allUsernames.add(user.Username);
    }
  }

  for (const user of regularUsers) {
    if (user.Username) {
      allUsernames.add(user.Username);
    }
  }

  const adminUsernames = new Set(
    adminUsers
      .map((user) => user.Username)
      .filter(Boolean)
  );

  /*
   * A descrição dos grupos guarda os dados próprios do cliente:
   *
   * {
   *   "name": "Nome da empresa",
   *   "status": "active",
   *   "contactName": "Nome do contacto",
   *   "email": "contacto@empresa.com",
   *   "phone": "+351...",
   *   "country": "Portugal"
   * }
   *
   * Os dados do cliente não são lidos dos atributos dos utilizadores.
   *
   * Por compatibilidade com grupos antigos, uma descrição em texto puro
   * continua sendo interpretada como o nome da empresa, com status active
   * e os restantes campos vazios.
   *
   * Se ambos os grupos existirem, os metadados do grupo de administradores
   * têm prioridade. Campos ausentes podem ser completados pelo grupo users.
   */
  const adminMetadata = parseClientGroupDescription(
    clientEntry.adminGroup?.Description
  );

  const userMetadata = parseClientGroupDescription(
    clientEntry.userGroup?.Description
  );

  const companyName =
    adminMetadata.name ||
    userMetadata.name ||
    "";

  const clientStatus = normalizeClientStatus(
    adminMetadata.status ||
    userMetadata.status ||
    "active"
  );

  const contactName =
    adminMetadata.contactName ||
    userMetadata.contactName ||
    "";

  const email =
    adminMetadata.email ||
    userMetadata.email ||
    "";

  const phone =
    adminMetadata.phone ||
    userMetadata.phone ||
    "";

  const country =
    adminMetadata.country ||
    userMetadata.country ||
    "";

  return {
    clientId: clientEntry.clientId,
    name: companyName,
    email,
    contactName,
    phone,
    country,
    status: clientStatus === "active"
      ? "Active"
      : "Inactive",
    users: allUsernames.size,
    admins: adminUsernames.size
  };
}

function parseClientGroupDescription(description) {
  const rawDescription = String(description || "").trim();

  if (!rawDescription) {
    return {
      name: "",
      status: "",
      contactName: "",
      email: "",
      phone: "",
      country: ""
    };
  }

  try {
    const parsed = JSON.parse(rawDescription);

    if (
      parsed &&
      typeof parsed === "object" &&
      !Array.isArray(parsed)
    ) {
      return {
        name: String(parsed.name || "").trim(),
        status: String(parsed.status || "").trim().toLowerCase(),
        contactName: String(parsed.contactName || "").trim(),
        email: String(parsed.email || "").trim(),
        phone: String(parsed.phone || "").trim(),
        country: String(parsed.country || "").trim()
      };
    }
  } catch {
    /*
     * Grupos antigos podem conter apenas o nome da empresa.
     * Nesse caso, preservamos compatibilidade e assumimos status active.
     */
  }

  return {
    name: rawDescription,
    status: "active",
    contactName: "",
    email: "",
    phone: "",
    country: ""
  };
}

function normalizeClientStatus(status) {
  return String(status || "").trim().toLowerCase() === "inactive"
    ? "inactive"
    : "active";
}

async function listAllUsersInGroup(groupName) {
  const users = [];
  let nextToken;

  do {
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
  } while (nextToken);

  return users;
}

function getAttributeValue(attributes, attributeName) {
  const attribute = (attributes || []).find(
    (item) => item.Name === attributeName
  );

  return attribute?.Value || "";
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}

