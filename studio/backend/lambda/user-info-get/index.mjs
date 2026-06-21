import pg from "pg";

const { Pool } = pg;

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,GET",
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
  console.log("Trackster user info get request", {
    method: event?.requestContext?.http?.method || event?.httpMethod,
    path: event?.rawPath || event?.path
  });

  const method = event?.requestContext?.http?.method || event?.httpMethod;

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
    const username = getAuthenticatedUsername(event);

    if (!username) {
      return buildResponse(401, {
        success: false,
        error: "Authenticated username was not found in token claims."
      });
    }

    const userInfo = await getUserInfo(username);

    if (!userInfo) {
      return buildResponse(404, {
        success: false,
        error: "User was not found in Trackster database."
      });
    }

    if (userInfo.status !== "active") {
      return buildResponse(403, {
        success: false,
        error: "User is not active."
      });
    }

    return buildResponse(200, {
      success: true,
      user: userInfo
    });
  } catch (error) {
    console.error("Unable to retrieve Trackster user information", error);

    return buildResponse(500, {
      success: false,
      error: "Unable to retrieve user information."
    });
  }
};

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

async function getUserInfo(username) {
  const query = `
    SELECT
      u.id AS user_id,
      u.username,
      u.email,
      u.full_name,
      u.status AS user_status,
      u.created_at AS user_created_at,
      u.updated_at AS user_updated_at,

      r.role_code AS user_role,

      cu.client_id,

      c.company_name,
      c.company_email,
      c.contact_name,
      c.country,
      c.phone,
      c.status AS client_status

    FROM trackster_users u

    INNER JOIN trackster_roles r
      ON r.id = u.role_id

    LEFT JOIN trackster_client_users cu
      ON cu.user_id = u.id

    LEFT JOIN trackster_clients c
      ON c.client_id = cu.client_id

    WHERE LOWER(u.username) = LOWER($1)

    ORDER BY
      CASE WHEN c.status = 'active' THEN 0 ELSE 1 END,
      cu.created_at ASC
  `;

  const result = await pool.query(query, [username]);

  if (result.rows.length === 0) {
    return null;
  }

  const firstRow = result.rows[0];

  const clientAssociations = result.rows
    .filter((row) => row.client_id)
    .map((row) => ({
      clientId: row.client_id,
      clientRole: firstRow.user_role || null,
      status: firstRow.user_status || null,
      companyName: row.company_name || "",
      companyEmail: row.company_email || "",
      contactName: row.contact_name || "",
      country: row.country || "",
      phone: row.phone || "",
      clientStatus: row.client_status || null
    }));

  const primaryClient =
    clientAssociations.find((client) => client.clientStatus === "active") ||
    clientAssociations[0] ||
    null;

  return {
    id: firstRow.user_id,
    username: firstRow.username,
    email: firstRow.email || "",
    fullName: firstRow.full_name || "",
    status: firstRow.user_status,
    globalRole: firstRow.user_role === "trackster_admin" ? "trackster_admin" : null,
    clientRole: firstRow.user_role !== "trackster_admin" ? firstRow.user_role : null,
    clientId: primaryClient?.clientId || "",
    companyName: primaryClient?.companyName || "",
    companyEmail: primaryClient?.companyEmail || "",
    contactName: primaryClient?.contactName || "",
    country: primaryClient?.country || "",
    phone: primaryClient?.phone || "",
    createdAt: firstRow.user_created_at,
    updatedAt: firstRow.user_updated_at,
    clientAssociations
  };
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}