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
  max: Number(process.env.DB_POOL_MAX || 2),
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

export const handler = async (event) => {
  console.log("Trackster admin update client request", {
    method: event?.requestContext?.http?.method || event?.httpMethod,
    path: event?.rawPath || event?.path
  });

  const method = event?.requestContext?.http?.method || event?.httpMethod;

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
        error: "Only active trackster_admin users can update clients."
      });
    }

    const body = parseBody(event);

    const clientId = (body.clientId || "").trim();
    const contactName = (body.contactName || "").trim();
    const email = (body.email || "").trim();
    const phone = (body.phone || "").trim();
    const country = (body.country || "").trim();

    if (!clientId) {
      return buildResponse(400, {
        success: false,
        error: "clientId is required."
      });
    }

    const existingClient = await getClient(clientId);

    if (!existingClient) {
      return buildResponse(404, {
        success: false,
        error: "Client was not found."
      });
    }

    await updateClient({
      clientId,
      contactName,
      email,
      phone,
      country
    });

    return buildResponse(200, {
      success: true,
      message: "Client updated successfully."
    });
  } catch (error) {
    console.error("Unable to update Trackster client", error);

    return buildResponse(500, {
      success: false,
      error: "Unable to update client."
    });
  }
};

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

function getAuthenticatedUsername(event) {
  const httpApiClaims = event?.requestContext?.authorizer?.jwt?.claims;
  const restApiClaims = event?.requestContext?.authorizer?.claims;

  const claims = httpApiClaims || restApiClaims || {};

  return (
    claims["cognito:username"] ||
    claims.username ||
    claims.preferred_username ||
    claims.email ||
    ""
  ).trim();
}

async function getAuthenticatedUserContext(username) {
  const query = `
    SELECT
      u.id AS user_id,
      u.username,
      u.email,
      u.full_name,
      u.status AS user_status,
      r.role_code AS user_role

    FROM trackster_users u

    INNER JOIN trackster_roles r
      ON r.id = u.role_id

    WHERE LOWER(u.username) = LOWER($1)

    LIMIT 1
  `;

  const result = await pool.query(query, [username]);

  if (result.rows.length === 0) {
    return null;
  }

  const row = result.rows[0];

  return {
    id: row.user_id,
    username: row.username,
    email: row.email || "",
    fullName: row.full_name || "",
    status: row.user_status,
    role: row.user_role || null
  };
}

async function getClient(clientId) {
  const query = `
    SELECT
      client_id
    FROM trackster_clients
    WHERE client_id = $1
    LIMIT 1
  `;

  const result = await pool.query(query, [clientId]);

  if (result.rows.length === 0) {
    return null;
  }

  return result.rows[0];
}

async function updateClient(client) {
  const query = `
    UPDATE trackster_clients
    SET
      contact_name = $2,
      company_email = $3,
      phone = $4,
      country = $5,
      updated_at = NOW()
    WHERE client_id = $1
  `;

  await pool.query(query, [
    client.clientId,
    client.contactName,
    client.email,
    client.phone,
    client.country
  ]);
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}