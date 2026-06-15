import pg from "pg";

const { Pool } = pg;

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,POST",
  "Content-Type": "application/json"
};

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: process.env.DB_SSL === "true"
    ? {
        rejectUnauthorized: false
      }
    : false,
  max: 2,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

export const handler = async (event) => {
  const method = getHttpMethod(event);

  try {
    console.log("Incoming update user profile request debug:", {
      method,
      hasAuthorizationHeader: Boolean(
        event.headers?.authorization || event.headers?.Authorization
      ),
      hasBody: Boolean(event.body),
      bodyPreview: typeof event.body === "string"
        ? event.body.substring(0, 160)
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

    const username = extractUsername(event);

    console.log("Authenticated user debug:", {
      hasUsername: Boolean(username),
      username
    });

    if (!username) {
      return response(401, {
        success: false,
        message: "Authenticated username was not found"
      });
    }

    const body = parseBody(event.body);

    const fullName = readString(body.fullName);
    const email = readString(body.email).toLowerCase();

    console.log("User profile payload debug:", {
      hasFullName: Boolean(fullName),
      hasEmail: Boolean(email),
      fullNameLength: fullName.length,
      emailLength: email.length
    });

    if (!fullName || !email) {
      return response(400, {
        success: false,
        message: "fullName and email are required"
      });
    }

    if (!isValidEmail(email)) {
      return response(400, {
        success: false,
        message: "Invalid email format"
      });
    }

    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      const updateResult = await client.query(
        `
          UPDATE trackster_users
          SET
              full_name = $1,
              email = $2,
              updated_at = NOW()
          WHERE username = $3
            AND status = 'active'
          RETURNING
              id,
              username,
              email,
              full_name,
              status,
              created_at,
              updated_at
        `,
        [fullName, email, username]
      );

      if (updateResult.rowCount === 0) {
        await client.query("ROLLBACK");

        return response(404, {
          success: false,
          message: "Active user was not found"
        });
      }

      await client.query("COMMIT");

      const updatedUser = updateResult.rows[0];

      return response(200, {
        success: true,
        message: "User profile updated successfully",
        user: {
          id: updatedUser.id,
          username: updatedUser.username,
          email: updatedUser.email,
          fullName: updatedUser.full_name,
          status: updatedUser.status,
          createdAt: updatedUser.created_at,
          updatedAt: updatedUser.updated_at
        }
      });
    } catch (error) {
      await client.query("ROLLBACK");

      if (error?.code === "23505") {
        return response(409, {
          success: false,
          message: "Email is already used by another user"
        });
      }

      throw error;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error("Update user profile full error:", error);

    console.error("Update user profile summarized error:", {
      name: error?.name,
      message: error?.message,
      code: error?.code,
      detail: error?.detail,
      constraint: error?.constraint
    });

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

function extractUsername(event) {
  const claims =
    event.requestContext?.authorizer?.jwt?.claims ||
    event.requestContext?.authorizer?.claims ||
    {};

  return (
    readString(claims["cognito:username"]) ||
    readString(claims.username) ||
    readString(claims["preferred_username"])
  );
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
      bodyPreview: String(rawBody).substring(0, 160)
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

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function response(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}