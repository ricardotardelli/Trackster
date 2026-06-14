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
  try {
    if (event.httpMethod === "OPTIONS") {
      return response(200, { success: true });
    }

    if (event.httpMethod !== "POST") {
      return response(405, {
        success: false,
        message: "Method not allowed"
      });
    }

    const accessToken = extractBearerToken(event);

    if (!accessToken) {
      return response(401, {
        success: false,
        message: "Missing Authorization bearer token"
      });
    }

    const body = parseBody(event.body);

    const previousPassword = body.previousPassword;
    const newPassword = body.newPassword;

    if (!previousPassword || !newPassword) {
      return response(400, {
        success: false,
        message: "previousPassword and newPassword are required"
      });
    }

    if (previousPassword === newPassword) {
      return response(400, {
        success: false,
        message: "New password must be different from the previous password"
      });
    }

    await cognito.send(
      new ChangePasswordCommand({
        AccessToken: accessToken,
        PreviousPassword: previousPassword,
        ProposedPassword: newPassword
      })
    );

    return response(200, {
      success: true,
      message: "Password changed successfully"
    });
  } catch (error) {
    console.error("Change password error:", {
      name: error.name,
      message: error.message
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

    return response(500, {
      success: false,
      message: "Internal server error"
    });
  }
};

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

  return JSON.parse(rawBody);
}

function response(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}