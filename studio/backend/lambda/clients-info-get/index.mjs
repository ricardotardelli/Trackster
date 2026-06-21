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
  console.log("Trackster admin list clients request", {
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
        error: "Only active trackster_admin users can list clients."
      });
    }

    const clients = await listClients();

    return buildResponse(200, {
      success: true,
      clients
    });
  } catch (error) {
    console.error("Unable to list Trackster clients", error);

    return buildResponse(500, {
      success: false,
      error: "Unable to list clients."
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

    ORDER BY u.username
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

async function listClients() {
  const query = `
    SELECT
      c.client_id,
      c.company_name,
      c.company_email,
      c.contact_name,
      c.phone,
      c.country,
      c.status,

      COUNT(DISTINCT cu.user_id)::int AS users_count,

      COUNT(
        DISTINCT CASE
          WHEN r.role_code = 'client_admin' THEN u.id
          ELSE NULL
        END
      )::int AS admins_count

    FROM trackster_clients c

    LEFT JOIN trackster_client_users cu
      ON cu.client_id = c.client_id

    LEFT JOIN trackster_users u
      ON u.id = cu.user_id

    LEFT JOIN trackster_roles r
      ON r.id = u.role_id

    GROUP BY
      c.company_name,
      c.client_id,
      c.company_email,
      c.contact_name,
      c.phone,
      c.country,
      c.status

    ORDER BY
      LOWER(c.company_name) ASC,
      c.client_id ASC
  `;

  const result = await pool.query(query);

  return result.rows.map((row) => ({
    clientId: row.client_id,
    name: row.company_name || "",
    email: row.company_email || "",
    contactName: row.contact_name || "",
    phone: row.phone || "",
    country: row.country || "",
    status: toUiStatus(row.status),
    users: Number(row.users_count || 0),
    admins: Number(row.admins_count || 0)
  }));
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

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}