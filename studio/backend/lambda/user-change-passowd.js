import {
  CognitoIdentityProviderClient,
  ChangePasswordCommand
} from "@aws-sdk/client-cognito-identity-provider";

const cognito = new CognitoIdentityProviderClient({});

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,POST",
  "Content-Type": "application/json"
};

export const handler = async (event) => {
  const method = getHttpMethod(event);

  try {
    console.log("Incoming change password request debug:", {
      method,
      hasAuthorizationHeader: Boolean(
        event.headers?.authorization || event.headers?.Authorization
      ),
      authorizationPrefix: String(
        event.headers?.authorization || event.headers?.Authorization || ""
      ).substring(0, 20),
      hasBody: Boolean(event.body),
      bodyPreview: typeof event.body === "string"
        ? event.body.substring(0, 120)
        : event.body
    });

    if (method === "OPTIONS") {
      return response(200, {
        success: true
      });
    }

    if (method !== "POST") {
      return response(405, {
        success: false,
        message: "Method not allowed"
      });
    }

    const accessToken = extractBearerToken(event);

    console.log("Token debug:", {
      hasAccessToken: Boolean(accessToken),
      accessTokenPrefix: accessToken ? accessToken.substring(0, 20) : "",
      accessTokenLength: accessToken ? accessToken.length : 0
    });

    if (!accessToken) {
      return response(401, {
        success: false,
        message: "Missing Authorization bearer token"
      });
    }

    const body = parseBody(event.body);

    const currentPassword = readString(body.currentPassword);
    const newPassword = readString(body.newPassword);

    console.log("Password payload debug:", {
      hasCurrentPassword: Boolean(currentPassword),
      hasNewPassword: Boolean(newPassword),
      currentPasswordLength: currentPassword.length,
      newPasswordLength: newPassword.length
    });

    if (!currentPassword || !newPassword) {
      return response(400, {
        success: false,
        message: "currentPassword and newPassword are required"
      });
    }

    if (currentPassword === newPassword) {
      return response(400, {
        success: false,
        message: "New password must be different from the current password"
      });
    }

    await cognito.send(
      new ChangePasswordCommand({
        AccessToken: accessToken,
        PreviousPassword: currentPassword,
        ProposedPassword: newPassword
      })
    );

    return response(200, {
      success: true,
      message: "Password changed successfully"
    });
  } catch (error) {
    console.error("Change password full error:", error);

    console.error("Change password summarized error:", {
      name: error?.name,
      message: error?.message,
      code: error?.code,
      statusCode: error?.$metadata?.httpStatusCode,
      requestId: error?.$metadata?.requestId,
      attempts: error?.$metadata?.attempts,
      totalRetryDelay: error?.$metadata?.totalRetryDelay
    });

    if (error.name === "NotAuthorizedException") {
      return response(401, {
        success: false,
        message: "Invalid current password or expired session"
      });
    }

    if (error.name === "InvalidPasswordException") {
      return response(400, {
        success: false,
        message: "The new password does not match the password policy"
      });
    }

    if (error.name === "LimitExceededException") {
      return response(429, {
        success: false,
        message: "Too many attempts. Please try again later"
      });
    }

    if (error.name === "PasswordResetRequiredException") {
      return response(403, {
        success: false,
        message: "Password reset is required before changing the password"
      });
    }

    if (error.name === "UserNotConfirmedException") {
      return response(403, {
        success: false,
        message: "User is not confirmed"
      });
    }

    return response(500, {
      success: false,
      message: "Internal server error"
    });
  }
};

function getHttpMethod(event) {
  return (
    event.httpMethod ||
    event.requestContext?.http?.method ||
    event.requestContext?.httpMethod ||
    ""
  ).toUpperCase();
}

function extractBearerToken(event) {
  const headers = event.headers || {};

  const authorization =
    headers.Authorization ||
    headers.authorization ||
    "";

  if (!authorization.startsWith("Bearer ")) {
    return null;
  }

  return authorization.substring("Bearer ".length).trim();
}

function parseBody(rawBody) {
  if (!rawBody) {
    return {};
  }

  if (typeof rawBody === "object") {
    return rawBody;
  }

  try {
    return JSON.parse(rawBody);
  } catch (error) {
    console.error("Invalid JSON body:", {
      message: error.message,
      bodyPreview: String(rawBody).substring(0, 120)
    });

    return {};
  }
}

function readString(value) {
  if (typeof value !== "string") {
    return "";
  }

  return value.trim();
}

function response(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}