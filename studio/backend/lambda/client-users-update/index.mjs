import {
  AdminAddUserToGroupCommand,
  AdminDisableUserCommand,
  AdminEnableUserCommand,
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  AdminRemoveUserFromGroupCommand,
  AdminUpdateUserAttributesCommand,
  CognitoIdentityProviderClient
} from "@aws-sdk/client-cognito-identity-provider";

const userPoolId = process.env.COGNITO_USER_POOL_ID || "";
const region =
  process.env.AWS_REGION ||
  process.env.REGION ||
  "eu-west-1";

const cognitoClient =
  new CognitoIdentityProviderClient({ region });

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
      message: "Method not allowed."
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

    const body = parseBody(event);

    const username =
      normalize(body.username);

    const clientId =
      normalize(body.clientId);

    const fullName =
      normalize(body.fullName);

    const email =
      normalize(body.email).toLowerCase();

    const role =
      normalize(body.role).toLowerCase();

    const status =
      normalize(body.status).toLowerCase();

    if (
      !username ||
      !clientId ||
      !fullName ||
      !email ||
      !role ||
      !status
    ) {
      return buildResponse(400, {
        success: false,
        message:
          "username, clientId, fullName, email, role and status are required."
      });
    }

    if (
      role !== "client_admin" &&
      role !== "client_user"
    ) {
      return buildResponse(400, {
        success: false,
        message: "Invalid role."
      });
    }

    if (
      status !== "active" &&
      status !== "inactive" &&
      status !== "suspended"
    ) {
      return buildResponse(400, {
        success: false,
        message: "Invalid status."
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
          UserPoolId: userPoolId,
          Username: username
        })
      );

    const targetGroups =
      await listGroupsForUser(
        targetUser.Username || username
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
          "User was not found for the selected client."
      });
    }

    await cognitoClient.send(
      new AdminUpdateUserAttributesCommand({
        UserPoolId: userPoolId,
        Username:
          targetUser.Username || username,
        UserAttributes: [
          {
            Name: "email",
            Value: email
          },
          {
            Name: "name",
            Value: fullName
          }
        ]
      })
    );

    const targetGroup =
      role === "client_admin"
        ? adminGroup
        : userGroup;

    const otherGroup =
      role === "client_admin"
        ? userGroup
        : adminGroup;

    if (
      !targetGroups.includes(targetGroup)
    ) {
      await cognitoClient.send(
        new AdminAddUserToGroupCommand({
          UserPoolId: userPoolId,
          Username:
            targetUser.Username || username,
          GroupName: targetGroup
        })
      );
    }

    if (
      targetGroups.includes(otherGroup)
    ) {
      await cognitoClient.send(
        new AdminRemoveUserFromGroupCommand({
          UserPoolId: userPoolId,
          Username:
            targetUser.Username || username,
          GroupName: otherGroup
        })
      );
    }

    if (status === "active") {
      await cognitoClient.send(
        new AdminEnableUserCommand({
          UserPoolId: userPoolId,
          Username:
            targetUser.Username || username
        })
      );
    } else {
      await cognitoClient.send(
        new AdminDisableUserCommand({
          UserPoolId: userPoolId,
          Username:
            targetUser.Username || username
        })
      );
    }

    return buildResponse(200, {
      success: true,
      message:
        "User updated successfully.",
      externalLoginUpdated: true,
      updatedUser: {
        username:
          targetUser.Username || username,
        email,
        fullName,
        clientRole: role,
        globalRole: null,
        role,
        clientId,
        status
      }
    });
  } catch (error) {
    console.error(
      "client-user-update error",
      error
    );

    if (
      error?.name ===
      "UserNotFoundException"
    ) {
      return buildResponse(404, {
        success: false,
        message:
          "User was not found."
      });
    }

    if (
      error?.name ===
      "AliasExistsException"
    ) {
      return buildResponse(409, {
        success: false,
        message:
          "The email address is already in use."
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
        "Unable to update user."
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
          UserPoolId: userPoolId,
          Username: username,
          Limit: 60,
          NextToken: nextToken
        })
      );

    for (
      const group of
      result.Groups || []
    ) {
      if (group.GroupName) {
        groups.push(group.GroupName);
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

function normalize(value) {
  return String(value || "").trim();
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
