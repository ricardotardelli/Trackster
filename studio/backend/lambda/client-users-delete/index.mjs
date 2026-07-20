import {
  AdminDeleteUserCommand,
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  CognitoIdentityProviderClient
} from "@aws-sdk/client-cognito-identity-provider";

const userPoolId =
  process.env.COGNITO_USER_POOL_ID || "";

const region =
  process.env.AWS_REGION ||
  process.env.REGION ||
  "eu-west-1";

const cognitoClient =
  new CognitoIdentityProviderClient({
    region
  });

const defaultHeaders = {
  "Access-Control-Allow-Origin":
    process.env.ALLOWED_ORIGIN || "*",
  "Access-Control-Allow-Headers":
    "Content-Type,Authorization",
  "Access-Control-Allow-Methods":
    "OPTIONS,POST",
  "Content-Type":
    "application/json"
};

export const handler = async (event) => {
  const method =
    event?.requestContext?.http?.method ||
    event?.httpMethod ||
    "";

  if (method === "OPTIONS") {
    return buildResponse(200, {
      success: true
    });
  }

  if (method !== "POST") {
    return buildResponse(405, {
      success: false,
      message:
        "Method not allowed.",
      receivedMethod:
        method || null
    });
  }

  try {
    if (!userPoolId) {
      throw new Error(
        "COGNITO_USER_POOL_ID is not configured."
      );
    }

    const authenticatedUsername =
      getAuthenticatedUsername(event);

    if (!authenticatedUsername) {
      return buildResponse(401, {
        success: false,
        message:
          "Authenticated username not found in token."
      });
    }

    const body =
      parseBody(event);

    const targetUsername =
      normalize(body.username);

    const clientId =
      normalize(body.clientId);

    if (!targetUsername) {
      return buildResponse(400, {
        success: false,
        message:
          "Missing required field: username."
      });
    }

    if (!clientId) {
      return buildResponse(400, {
        success: false,
        message:
          "Missing required field: clientId."
      });
    }

    const requesterGroups =
      await listGroupsForUser(
        authenticatedUsername
      );

    const isTracksterAdmin =
      requesterGroups.includes(
        "trackster-admins"
      );

    const isClientAdmin =
      requesterGroups.includes(
        `${clientId}-admins`
      );

    if (
      !isTracksterAdmin &&
      !isClientAdmin
    ) {
      return buildResponse(403, {
        success: false,
        message:
          "Access denied. Administrator permission is required."
      });
    }

    const targetUser =
      await cognitoClient.send(
        new AdminGetUserCommand({
          UserPoolId:
            userPoolId,
          Username:
            targetUsername
        })
      );

    const resolvedUsername =
      targetUser.Username ||
      targetUsername;

    const targetGroups =
      await listGroupsForUser(
        resolvedUsername
      );

    const adminGroup =
      `${clientId}-admins`;

    const userGroup =
      `${clientId}-users`;

    if (
      !targetGroups.includes(adminGroup) &&
      !targetGroups.includes(userGroup)
    ) {
      return buildResponse(404, {
        success: false,
        message:
          "Target user not found for the selected client."
      });
    }

    if (targetUser.Enabled !== false) {
      return buildResponse(409, {
        success: false,
        message:
          "The user must be inactive before deletion."
      });
    }

    const role =
      targetGroups.includes(adminGroup)
        ? "client_admin"
        : "client_user";

    const email =
      getAttribute(
        targetUser.UserAttributes,
        "email"
      );

    const fullName =
      getAttribute(
        targetUser.UserAttributes,
        "name"
      );

    await cognitoClient.send(
      new AdminDeleteUserCommand({
        UserPoolId:
          userPoolId,
        Username:
          resolvedUsername
      })
    );

    return buildResponse(200, {
      success: true,
      message:
        "User deleted successfully.",
      externalLoginDeleted: true,
      externalLoginWasMissing: false,
      deletedUser: {
        username:
          resolvedUsername,
        email,
        fullName,
        globalRole: null,
        clientRole: role,
        role,
        clientId
      }
    });
  } catch (error) {
    console.error(
      "client-users-delete error",
      error
    );

    if (
      error?.name ===
      "UserNotFoundException"
    ) {
      return buildResponse(404, {
        success: false,
        message:
          "Target user not found for the selected client.",
        externalLoginDeleted: false,
        externalLoginWasMissing: true
      });
    }

    if (
      error?.name ===
      "InvalidParameterException"
    ) {
      return buildResponse(400, {
        success: false,
        message:
          error?.message ||
          "Invalid Cognito data."
      });
    }

    return buildResponse(500, {
      success: false,
      message:
        error?.message ||
        "Internal server error while deleting user."
    });
  }
};

async function listGroupsForUser(
  username
) {
  const groups = [];
  let nextToken;

  do {
    const result =
      await cognitoClient.send(
        new AdminListGroupsForUserCommand({
          UserPoolId:
            userPoolId,
          Username:
            username,
          Limit: 60,
          NextToken:
            nextToken
        })
      );

    for (
      const group of
      result.Groups || []
    ) {
      if (group.GroupName) {
        groups.push(
          group.GroupName
        );
      }
    }

    nextToken =
      result.NextToken;
  } while (nextToken);

  return groups;
}

function getAuthenticatedUsername(
  event
) {
  const claims =
    event?.requestContext
      ?.authorizer?.jwt?.claims ||
    event?.requestContext
      ?.authorizer?.claims ||
    {};

  return normalize(
    claims["cognito:username"] ||
    claims.username ||
    claims.preferred_username ||
    claims.email ||
    ""
  );
}

function parseBody(event) {
  if (!event?.body) {
    return {};
  }

  if (
    typeof event.body === "object"
  ) {
    return event.body;
  }

  const rawBody =
    event.isBase64Encoded
      ? Buffer.from(
          event.body,
          "base64"
        ).toString("utf8")
      : event.body;

  return JSON.parse(rawBody);
}

function getAttribute(
  attributes,
  name
) {
  return (
    (attributes || []).find(
      (attribute) =>
        attribute.Name === name
    )?.Value ||
    ""
  );
}

function normalize(value) {
  return String(value || "").trim();
}

function buildResponse(
  statusCode,
  body
) {
  return {
    statusCode,
    headers:
      defaultHeaders,
    body:
      JSON.stringify(body)
  };
}
