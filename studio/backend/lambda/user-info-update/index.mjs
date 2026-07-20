import {
  AdminGetUserCommand,
  AdminUpdateUserAttributesCommand,
  CognitoIdentityProviderClient
} from "@aws-sdk/client-cognito-identity-provider";

const userPoolId =
  process.env.COGNITO_USER_POOL_ID || "";

const region =
  process.env.AWS_REGION ||
  process.env.REGION ||
  "eu-west-1";

const allowedOrigin =
  process.env.ALLOWED_ORIGIN || "*";

const cognitoClient =
  new CognitoIdentityProviderClient({
    region
  });

const defaultHeaders = {
  "Access-Control-Allow-Origin":
    allowedOrigin,
  "Access-Control-Allow-Headers":
    "Content-Type,Authorization",
  "Access-Control-Allow-Methods":
    "OPTIONS,POST",
  "Content-Type":
    "application/json"
};

export const handler = async (event) => {
  const method =
    getHttpMethod(event);

  try {
    console.log(
      "Incoming update user profile request",
      {
        method,
        hasAuthorizationHeader:
          Boolean(
            event?.headers?.authorization ||
            event?.headers?.Authorization
          ),
        hasBody:
          Boolean(event?.body)
      }
    );

    if (method === "OPTIONS") {
      return response(200, {
        success: true
      });
    }

    if (method !== "POST") {
      return response(405, {
        success: false,
        message:
          "Method not allowed"
      });
    }

    if (!userPoolId) {
      throw new Error(
        "COGNITO_USER_POOL_ID is not configured."
      );
    }

    const username =
      extractUsername(event);

    if (!username) {
      return response(401, {
        success: false,
        message:
          "Authenticated username was not found"
      });
    }

    const body =
      parseBody(event);

    const fullName =
      readString(
        body.fullName
      );

    const email =
      readString(
        body.email
      ).toLowerCase();

    if (!fullName || !email) {
      return response(400, {
        success: false,
        message:
          "fullName and email are required"
      });
    }

    const user =
      await cognitoClient.send(
        new AdminGetUserCommand({
          UserPoolId:
            userPoolId,
          Username:
            username
        })
      );

    if (user.Enabled !== true) {
      return response(404, {
        success: false,
        message:
          "Active user was not found"
      });
    }

    const resolvedUsername =
      user.Username ||
      username;

    await cognitoClient.send(
      new AdminUpdateUserAttributesCommand({
        UserPoolId:
          userPoolId,
        Username:
          resolvedUsername,
        UserAttributes: [
          {
            Name: "name",
            Value: fullName
          },
          {
            Name: "email",
            Value: email
          }
        ]
      })
    );

    const updatedUser =
      await cognitoClient.send(
        new AdminGetUserCommand({
          UserPoolId:
            userPoolId,
          Username:
            resolvedUsername
        })
      );

    return response(200, {
      success: true,
      message:
        "User profile updated successfully",
      user: {
        id:
          getAttribute(
            updatedUser.UserAttributes,
            "sub"
          ),
        username:
          updatedUser.Username ||
          resolvedUsername,
        email:
          getAttribute(
            updatedUser.UserAttributes,
            "email"
          ),
        fullName:
          getAttribute(
            updatedUser.UserAttributes,
            "name"
          ),
        status:
          updatedUser.Enabled === true
            ? "active"
            : "inactive",
        createdAt:
          updatedUser.UserCreateDate ||
          null,
        updatedAt:
          updatedUser.UserLastModifiedDate ||
          null
      }
    });
  } catch (error) {
    console.error(
      "Update user profile error",
      {
        name:
          error?.name,
        message:
          error?.message,
        code:
          error?.code,
        stack:
          error?.stack
      }
    );

    if (
      error?.name ===
      "UserNotFoundException"
    ) {
      return response(404, {
        success: false,
        message:
          "Active user was not found"
      });
    }

    if (
      error?.name ===
      "AliasExistsException"
    ) {
      return response(409, {
        success: false,
        message:
          "Email is already used by another user"
      });
    }

    if (
      error?.name ===
      "InvalidParameterException"
    ) {
      return response(400, {
        success: false,
        message:
          error?.message ||
          "Invalid Cognito user data"
      });
    }

    return response(500, {
      success: false,
      message:
        error?.message ||
        "Internal server error"
    });
  }
};

function getHttpMethod(event) {
  return (
    event?.httpMethod ||
    event?.requestContext?.http?.method ||
    event?.requestContext?.httpMethod ||
    ""
  ).toUpperCase();
}

function extractUsername(event) {
  const claims =
    event?.requestContext?.authorizer
      ?.jwt?.claims ||
    event?.requestContext?.authorizer
      ?.claims ||
    {};

  return (
    readString(
      claims["cognito:username"]
    ) ||
    readString(
      claims.username
    ) ||
    readString(
      claims.preferred_username
    ) ||
    readString(
      claims.email
    )
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

function readString(value) {
  if (
    typeof value !== "string"
  ) {
    return "";
  }

  return value.trim();
}

function response(
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
