import {
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  CognitoIdentityProviderClient,
  CreateGroupCommand,
  DeleteGroupCommand,
  GetGroupCommand
} from "@aws-sdk/client-cognito-identity-provider";

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";
const userPoolId = process.env.COGNITO_USER_POOL_ID || "";
const region =
  process.env.AWS_REGION ||
  process.env.REGION ||
  "eu-west-1";

const TRACKSTER_ADMINS_GROUP = "trackster-admins";
const CLIENT_ID_PATTERN = /^\d{8}$/;
const validStatuses = [
  "active",
  "inactive",
  "suspended"
];

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,POST",
  "Content-Type": "application/json"
};

const cognitoClient =
  new CognitoIdentityProviderClient({
    region
  });

export const handler = async (event) => {
  console.log(
    "Trackster admin add client request",
    {
      method:
        event?.requestContext?.http?.method ||
        event?.httpMethod,
      path:
        event?.rawPath ||
        event?.path
    }
  );

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

    const username =
      getAuthenticatedUsername(event);

    if (!username) {
      return buildResponse(401, {
        success: false,
        error:
          "Authenticated username was not found in token claims."
      });
    }

    const authenticatedUser =
      await getAuthenticatedUserContext(
        username
      );

    if (!authenticatedUser) {
      return buildResponse(404, {
        success: false,
        error:
          "Authenticated user was not found in Trackster database."
      });
    }

    if (
      authenticatedUser.status !== "active"
    ) {
      return buildResponse(403, {
        success: false,
        error:
          "Authenticated user is not active."
      });
    }

    if (
      authenticatedUser.role !==
      "trackster_admin"
    ) {
      return buildResponse(403, {
        success: false,
        error:
          "Only active trackster_admin users can add clients."
      });
    }

    const body = parseBody(event);
    const input =
      normalizeClientInput(body);

    const validationError =
      validateClientInput(input);

    if (validationError) {
      return buildResponse(400, {
        success: false,
        error: validationError
      });
    }

    const existingClient =
      await findClientByClientId(
        input.clientId
      );

    if (existingClient) {
      return buildResponse(409, {
        success: false,
        error:
          "A client with this clientId already exists."
      });
    }

    const client =
      await addClient(input);

    return buildResponse(201, {
      success: true,
      message:
        "Client added successfully.",
      client
    });
  } catch (error) {
    console.error(
      "Unable to add Trackster client",
      {
        name: error?.name,
        message: error?.message,
        stack: error?.stack
      }
    );

    return buildResponse(500, {
      success: false,
      error: "Unable to add client."
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
    if (event.isBase64Encoded) {
      const decodedBody = Buffer.from(
        event.body,
        "base64"
      ).toString("utf-8");

      return JSON.parse(decodedBody);
    }

    if (
      typeof event.body === "string"
    ) {
      return JSON.parse(event.body);
    }

    return event.body;
  } catch {
    return {};
  }
}

function normalizeClientInput(body) {
  return {
    clientId: String(
      body.clientId ||
      body.client_id ||
      ""
    ).trim(),

    companyName: String(
      body.companyName ||
      body.company_name ||
      body.name ||
      ""
    ).trim(),

    companyEmail: String(
      body.companyEmail ||
      body.company_email ||
      body.email ||
      ""
    ).trim(),

    contactName: String(
      body.contactName ||
      body.contact_name ||
      ""
    ).trim(),

    country: String(
      body.country ||
      ""
    ).trim(),

    phone: String(
      body.phone ||
      ""
    ).trim(),

    status: String(
      body.status ||
      "active"
    )
      .trim()
      .toLowerCase()
  };
}

function validateClientInput(input) {
  if (!input.clientId) {
    return "clientId is required.";
  }

  /*
   * As Lambdas clients-info-get e clients-update
   * identificam clientes pelo padrão de oito dígitos.
   */
  if (
    !CLIENT_ID_PATTERN.test(
      input.clientId
    )
  ) {
    return "clientId must contain exactly 8 digits.";
  }

  if (!input.companyName) {
    return "companyName is required.";
  }

  if (
    input.companyName.length > 255
  ) {
    return "companyName must have at most 255 characters.";
  }

  if (
    input.companyEmail &&
    input.companyEmail.length > 255
  ) {
    return "companyEmail must have at most 255 characters.";
  }

  if (
    input.contactName &&
    input.contactName.length > 255
  ) {
    return "contactName must have at most 255 characters.";
  }

  if (
    input.country &&
    input.country.length > 255
  ) {
    return "country must have at most 255 characters.";
  }

  if (
    input.phone &&
    input.phone.length > 64
  ) {
    return "phone must have at most 64 characters.";
  }

  if (
    !validStatuses.includes(
      input.status
    )
  ) {
    return "status must be active, inactive, or suspended.";
  }

  const description =
    buildClientDescription(input);

  if (description.length > 2048) {
    return "Client data exceeds the Cognito group description limit.";
  }

  return "";
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

      status:
        isCognitoUserActive(
          userResponse
        )
          ? "active"
          : "inactive",

      role: groups.includes(
        TRACKSTER_ADMINS_GROUP
      )
        ? "trackster_admin"
        : null
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

async function listGroupsForUser(
  username
) {
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

function isCognitoUserActive(user) {
  return (
    user?.Enabled === true &&
    user?.UserStatus !== "ARCHIVED"
  );
}

async function findClientByClientId(
  clientId
) {
  const adminGroupName =
    `${clientId}-admins`;

  const userGroupName =
    `${clientId}-users`;

  const [
    adminGroupExists,
    userGroupExists
  ] = await Promise.all([
    groupExists(adminGroupName),
    groupExists(userGroupName)
  ]);

  return (
    adminGroupExists ||
    userGroupExists
  )
    ? {
        clientId
      }
    : null;
}

async function groupExists(groupName) {
  try {
    await cognitoClient.send(
      new GetGroupCommand({
        UserPoolId: userPoolId,
        GroupName: groupName
      })
    );

    return true;
  } catch (error) {
    if (
      error?.name ===
      "ResourceNotFoundException"
    ) {
      return false;
    }

    throw error;
  }
}

async function addClient(input) {
  const adminGroupName =
    `${input.clientId}-admins`;

  const userGroupName =
    `${input.clientId}-users`;

  const description =
    buildClientDescription(input);

  let adminGroupCreated = false;
  let userGroupCreated = false;

  try {
    await cognitoClient.send(
      new CreateGroupCommand({
        UserPoolId: userPoolId,
        GroupName: adminGroupName,
        Description: description
      })
    );

    adminGroupCreated = true;

    await cognitoClient.send(
      new CreateGroupCommand({
        UserPoolId: userPoolId,
        GroupName: userGroupName,
        Description: description
      })
    );

    userGroupCreated = true;
  } catch (error) {
    /*
     * Evita deixar apenas um dos grupos criado
     * se a segunda operação falhar.
     */
    await rollbackCreatedGroups({
      adminGroupName,
      userGroupName,
      adminGroupCreated,
      userGroupCreated
    });

    throw error;
  }

  return {
    clientId: input.clientId,
    name: input.companyName,
    email: input.companyEmail,
    contactName: input.contactName,
    phone: input.phone,
    country: input.country,
    status: toUiStatus(
      input.status
    ),
    users: 0,
    admins: 0
  };
}

function buildClientDescription(input) {
  return JSON.stringify({
    name: input.companyName,
    status: input.status,
    contactName: input.contactName,
    email: input.companyEmail,
    phone: input.phone,
    country: input.country
  });
}

async function rollbackCreatedGroups({
  adminGroupName,
  userGroupName,
  adminGroupCreated,
  userGroupCreated
}) {
  const rollbackCommands = [];

  if (userGroupCreated) {
    rollbackCommands.push(
      deleteGroupIgnoringNotFound(
        userGroupName
      )
    );
  }

  if (adminGroupCreated) {
    rollbackCommands.push(
      deleteGroupIgnoringNotFound(
        adminGroupName
      )
    );
  }

  if (
    rollbackCommands.length === 0
  ) {
    return;
  }

  const results =
    await Promise.allSettled(
      rollbackCommands
    );

  for (const result of results) {
    if (
      result.status === "rejected"
    ) {
      console.error(
        "Unable to rollback Cognito group",
        result.reason
      );
    }
  }
}

async function deleteGroupIgnoringNotFound(
  groupName
) {
  try {
    await cognitoClient.send(
      new DeleteGroupCommand({
        UserPoolId: userPoolId,
        GroupName: groupName
      })
    );
  } catch (error) {
    if (
      error?.name !==
      "ResourceNotFoundException"
    ) {
      throw error;
    }
  }
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

function toUiStatus(status) {
  if (status === "active") {
    return "Active";
  }

  if (status === "suspended") {
    return "Suspended";
  }

  return "Inactive";
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
