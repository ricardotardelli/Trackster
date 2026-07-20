import {
  AdminDisableUserCommand,
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  CognitoIdentityProviderClient,
  ListGroupsCommand,
  ListUsersInGroupCommand,
  UpdateGroupCommand
} from "@aws-sdk/client-cognito-identity-provider";

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";
const userPoolId = process.env.COGNITO_USER_POOL_ID || "";
const region =
  process.env.AWS_REGION ||
  process.env.REGION ||
  "us-east-1";

const TRACKSTER_ADMINS_GROUP = "trackster-admins";
const CLIENT_ID_PATTERN = /^\d{8}$/;
const CLIENT_GROUP_PATTERN = /^(\d{8})-(admins|users)$/;
const validStatuses = ["active", "inactive", "suspended"];

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,POST",
  "Content-Type": "application/json"
};

const cognitoClient = new CognitoIdentityProviderClient({
  region
});

export const handler = async (event) => {
  console.log("Trackster admin update client request", {
    method:
      event?.requestContext?.http?.method ||
      event?.httpMethod,
    path:
      event?.rawPath ||
      event?.path
  });

  const method =
    event?.requestContext?.http?.method ||
    event?.httpMethod;

  if (method === "OPTIONS") {
    return buildResponse(200, {
      success: true
    });
  }

  if (method !== "POST") {
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
        error:
          "Authenticated username was not found in token claims."
      });
    }

    const authenticatedUser =
      await getAuthenticatedUserContext(username);

    if (!authenticatedUser) {
      return buildResponse(404, {
        success: false,
        error:
          "Authenticated user was not found in Trackster database."
      });
    }

    if (authenticatedUser.status !== "active") {
      return buildResponse(403, {
        success: false,
        error: "Authenticated user is not active."
      });
    }

    if (
      authenticatedUser.role !==
      "trackster_admin"
    ) {
      return buildResponse(403, {
        success: false,
        error:
          "Only active trackster_admin users can update clients."
      });
    }

    const body = parseBody(event);

    const clientId = String(
      body.clientId || ""
    ).trim();

    const contactName = String(
      body.contactName || ""
    ).trim();

    const email = String(
      body.email ||
      body.companyEmail ||
      ""
    ).trim();

    const phone = String(
      body.phone || ""
    ).trim();

    const country = String(
      body.country || ""
    ).trim();

    const requestedStatus =
      normalizeClientStatus(
        body.status,
        body.action,
        body.enabled
      );

    if (!clientId) {
      return buildResponse(400, {
        success: false,
        error: "clientId is required."
      });
    }

    if (!CLIENT_ID_PATTERN.test(clientId)) {
      return buildResponse(400, {
        success: false,
        error: "Invalid clientId."
      });
    }

    if (
      !validStatuses.includes(
        requestedStatus
      )
    ) {
      return buildResponse(400, {
        success: false,
        error: "Invalid client status."
      });
    }

    const clientContext =
      await getClientContext(clientId);

    if (!clientContext) {
      return buildResponse(404, {
        success: false,
        error: "Client was not found."
      });
    }

    const updatedMetadata = {
      name: String(
        body.name ??
        clientContext.name ??
        ""
      ).trim(),
      status: requestedStatus,
      contactName: String(
        body.contactName ??
        clientContext.contactName ??
        ""
      ).trim(),
      email: String(
        body.email ??
        body.companyEmail ??
        clientContext.email ??
        ""
      ).trim(),
      phone: String(
        body.phone ??
        clientContext.phone ??
        ""
      ).trim(),
      country: String(
        body.country ??
        clientContext.country ??
        ""
      ).trim()
    };

    await updateClientGroupMetadata(
      clientContext,
      updatedMetadata
    );

    let deactivatedUsers = [];

    if (updatedMetadata.status === "inactive") {
      deactivatedUsers =
        await deactivateClientUsers(
          clientContext.adminUsers,
          clientContext.regularUsers
        );
    }

    return buildResponse(200, {
      success: true,
      message:
        "Client updated successfully.",
      updatedClient: {
        clientId,
        name: updatedMetadata.name,
        email: updatedMetadata.email,
        contactName:
          updatedMetadata.contactName,
        phone: updatedMetadata.phone,
        country: updatedMetadata.country,
        status: updatedMetadata.status
      },
      deactivatedUsersCount:
        deactivatedUsers.length,
      deactivatedUsers
    });
  } catch (error) {
    console.error(
      "Unable to update Trackster client",
      {
        name: error?.name,
        message: error?.message,
        stack: error?.stack
      }
    );

    return buildResponse(500, {
      success: false,
      error: "Unable to update client."
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

function parseBody(event) {
  if (!event?.body) {
    return {};
  }

  try {
    return JSON.parse(event.body);
  } catch {
    return {};
  }
}

function normalizeClientStatus(
  status,
  action,
  enabled
) {
  const normalizedAction = String(
    action || ""
  )
    .trim()
    .toLowerCase();

  if (normalizedAction === "activate") {
    return "active";
  }

  if (
    normalizedAction === "deactivate" ||
    normalizedAction === "disable"
  ) {
    return "inactive";
  }

  if (normalizedAction === "suspend") {
    return "suspended";
  }

  if (typeof enabled === "boolean") {
    return enabled
      ? "active"
      : "inactive";
  }

  const normalizedStatus = String(
    status || ""
  )
    .trim()
    .toLowerCase();

  if (normalizedStatus === "active") {
    return "active";
  }

  if (
    normalizedStatus === "suspended"
  ) {
    return "suspended";
  }

  return "inactive";
}

function getAuthenticatedUsername(event) {
  const httpApiClaims =
    event?.requestContext?.authorizer
      ?.jwt?.claims;

  const restApiClaims =
    event?.requestContext?.authorizer
      ?.claims;

  const claims =
    httpApiClaims ||
    restApiClaims ||
    {};

  return String(
    claims["cognito:username"] ||
    claims.username ||
    claims.preferred_username ||
    claims.email ||
    ""
  ).trim();
}

async function getAuthenticatedUserContext(
  username
) {
  try {
    const [userResponse, groups] =
      await Promise.all([
        cognitoClient.send(
          new AdminGetUserCommand({
            UserPoolId: userPoolId,
            Username: username
          })
        ),
        listGroupsForUser(username)
      ]);

    const role = groups.includes(
      TRACKSTER_ADMINS_GROUP
    )
      ? "trackster_admin"
      : getClientRole(groups);

    return {
      id: getAttributeValue(
        userResponse.UserAttributes,
        "sub"
      ),
      username:
        userResponse.Username ||
        username,
      email: getAttributeValue(
        userResponse.UserAttributes,
        "email"
      ),
      fullName:
        getAttributeValue(
          userResponse.UserAttributes,
          "name"
        ) ||
        getAttributeValue(
          userResponse.UserAttributes,
          "custom:fullName"
        ),
      status: isCognitoUserActive(
        userResponse
      )
        ? "active"
        : "inactive",
      role
    };
  } catch (error) {
    if (
      error?.name ===
      "UserNotFoundException"
    ) {
      return null;
    }

    throw error;
  }
}

async function listGroupsForUser(username) {
  const groupNames = [];
  let nextToken;

  do {
    const response =
      await cognitoClient.send(
        new AdminListGroupsForUserCommand({
          UserPoolId: userPoolId,
          Username: username,
          Limit: 60,
          NextToken: nextToken
        })
      );

    for (
      const group of
      response.Groups || []
    ) {
      if (group.GroupName) {
        groupNames.push(
          group.GroupName
        );
      }
    }

    nextToken =
      response.NextToken;
  } while (nextToken);

  return groupNames;
}

function getClientRole(groupNames) {
  for (const groupName of groupNames) {
    const match =
      CLIENT_GROUP_PATTERN.exec(
        groupName
      );

    if (!match) {
      continue;
    }

    return match[2] === "admins"
      ? "client_admin"
      : "client_user";
  }

  return null;
}

function isCognitoUserActive(user) {
  return (
    user?.Enabled === true &&
    user?.UserStatus !== "ARCHIVED"
  );
}

async function getClientContext(
  clientId
) {
  const groups =
    await listAllCognitoGroups();

  const adminGroup =
    groups.find(
      (group) =>
        group.GroupName ===
        `${clientId}-admins`
    ) || null;

  const userGroup =
    groups.find(
      (group) =>
        group.GroupName ===
        `${clientId}-users`
    ) || null;

  if (!adminGroup && !userGroup) {
    return null;
  }

  const [adminUsers, regularUsers] =
    await Promise.all([
      adminGroup?.GroupName
        ? listAllUsersInGroup(
            adminGroup.GroupName
          )
        : Promise.resolve([]),

      userGroup?.GroupName
        ? listAllUsersInGroup(
            userGroup.GroupName
          )
        : Promise.resolve([])
    ]);

  const adminMetadata =
    parseClientGroupDescription(
      adminGroup?.Description
    );

  const userMetadata =
    parseClientGroupDescription(
      userGroup?.Description
    );

  return {
    clientId,
    name:
      adminMetadata.name ||
      userMetadata.name ||
      "",
    status:
      normalizeStoredClientStatus(
        adminMetadata.status ||
        userMetadata.status ||
        "active"
      ),
    contactName:
      adminMetadata.contactName ||
      userMetadata.contactName ||
      "",
    email:
      adminMetadata.email ||
      userMetadata.email ||
      "",
    phone:
      adminMetadata.phone ||
      userMetadata.phone ||
      "",
    country:
      adminMetadata.country ||
      userMetadata.country ||
      "",
    adminGroup,
    userGroup,
    adminUsers,
    regularUsers
  };
}

async function listAllCognitoGroups() {
  const groups = [];
  let nextToken;

  do {
    const response =
      await cognitoClient.send(
        new ListGroupsCommand({
          UserPoolId: userPoolId,
          Limit: 60,
          NextToken: nextToken
        })
      );

    groups.push(
      ...(response.Groups || [])
    );

    nextToken =
      response.NextToken;
  } while (nextToken);

  return groups;
}

function parseClientGroupDescription(
  description
) {
  const rawDescription = String(
    description || ""
  ).trim();

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
    const parsed =
      JSON.parse(rawDescription);

    if (
      parsed &&
      typeof parsed === "object" &&
      !Array.isArray(parsed)
    ) {
      return {
        name: String(
          parsed.name || ""
        ).trim(),
        status: String(
          parsed.status || ""
        )
          .trim()
          .toLowerCase(),
        contactName: String(
          parsed.contactName || ""
        ).trim(),
        email: String(
          parsed.email || ""
        ).trim(),
        phone: String(
          parsed.phone || ""
        ).trim(),
        country: String(
          parsed.country || ""
        ).trim()
      };
    }
  } catch {
    /*
     * Compatibilidade com grupos antigos:
     * Description contendo apenas o nome.
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

function normalizeStoredClientStatus(
  status
) {
  const normalized = String(
    status || ""
  )
    .trim()
    .toLowerCase();

  return validStatuses.includes(
    normalized
  )
    ? normalized
    : "active";
}

async function updateClientGroupMetadata(
  clientContext,
  metadata
) {
  const description =
    JSON.stringify({
      name: metadata.name,
      status: metadata.status,
      contactName: metadata.contactName,
      email: metadata.email,
      phone: metadata.phone,
      country: metadata.country
    });

  const commands = [];

  if (
    clientContext.adminGroup
      ?.GroupName
  ) {
    commands.push(
      cognitoClient.send(
        new UpdateGroupCommand({
          UserPoolId: userPoolId,
          GroupName:
            clientContext.adminGroup
              .GroupName,
          Description: description
        })
      )
    );
  }

  if (
    clientContext.userGroup
      ?.GroupName
  ) {
    commands.push(
      cognitoClient.send(
        new UpdateGroupCommand({
          UserPoolId: userPoolId,
          GroupName:
            clientContext.userGroup
              .GroupName,
          Description: description
        })
      )
    );
  }

  await Promise.all(commands);
}

async function deactivateClientUsers(
  adminUsers,
  regularUsers
) {
  const usersByUsername =
    new Map();

  for (
    const user of [
      ...(adminUsers || []),
      ...(regularUsers || [])
    ]
  ) {
    if (
      user.Username &&
      isCognitoUserActive(user)
    ) {
      usersByUsername.set(
        user.Username,
        user
      );
    }
  }

  const deactivatedUsers = [];

  for (
    const user of
    usersByUsername.values()
  ) {
    await cognitoClient.send(
      new AdminDisableUserCommand({
        UserPoolId: userPoolId,
        Username: user.Username
      })
    );

    const attributes =
      getUserAttributes(
        user.Attributes
      );

    deactivatedUsers.push({
      username: user.Username,
      email:
        attributes.email || "",
      fullName:
        attributes.name ||
        attributes[
          "custom:fullName"
        ] ||
        "",
      status: "inactive"
    });
  }

  return deactivatedUsers;
}

async function listAllUsersInGroup(
  groupName
) {
  const users = [];
  let nextToken;

  do {
    const response =
      await cognitoClient.send(
        new ListUsersInGroupCommand({
          UserPoolId: userPoolId,
          GroupName: groupName,
          Limit: 60,
          NextToken: nextToken
        })
      );

    users.push(
      ...(response.Users || [])
    );

    nextToken =
      response.NextToken;
  } while (nextToken);

  return users;
}

function getUserAttributes(
  attributes
) {
  return Object.fromEntries(
    (attributes || [])
      .filter(
        (attribute) =>
          attribute?.Name
      )
      .map((attribute) => [
        attribute.Name,
        attribute.Value || ""
      ])
  );
}

function getAttributeValue(
  attributes,
  attributeName
) {
  const attribute =
    (attributes || []).find(
      (item) =>
        item.Name ===
        attributeName
    );

  return attribute?.Value || "";
}

function buildResponse(
  statusCode,
  body
) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}
