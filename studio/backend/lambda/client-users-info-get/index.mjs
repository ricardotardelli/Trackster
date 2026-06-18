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
  console.log("Trackster admin list client users request", {
    method: event?.requestContext?.http?.method || event?.httpMethod,
    path: event?.rawPath || event?.path,
    queryStringParameters: event?.queryStringParameters || {}
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

    const requestedClientId = (event?.queryStringParameters?.clientId || "").trim();

    const authorizationResult = resolveAuthorizedClientId(
      authenticatedUser,
      requestedClientId
    );

    if (!authorizationResult.success) {
      return buildResponse(authorizationResult.statusCode, {
        success: false,
        error: authorizationResult.error
      });
    }

    const users = await listClientUsers(authorizationResult.clientId);

    return buildResponse(200, {
      success: true,
      clientId: authorizationResult.clientId,
      users
    });
  } catch (error) {
    console.error("Unable to list Trackster client users", error);

    return buildResponse(500, {
      success: false,
      error: "Unable to list client users."
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

      gr.role_code AS global_role,

      cu.client_id,
      cu.status AS client_user_status,

      cr.role_code AS client_role,

      c.status AS client_status

    FROM trackster_users u

    LEFT JOIN trackster_roles gr
      ON gr.id = u.global_role_id

    LEFT JOIN trackster_client_users cu
      ON cu.user_id = u.id

    LEFT JOIN trackster_roles cr
      ON cr.id = cu.role_id

    LEFT JOIN trackster_clients c
      ON c.client_id = cu.client_id

    WHERE LOWER(u.username) = LOWER($1)

    ORDER BY
      CASE WHEN cu.status = 'active' THEN 0 ELSE 1 END,
      cu.created_at ASC
  `;

  const result = await pool.query(query, [username]);

  if (result.rows.length === 0) {
    return null;
  }

  const firstRow = result.rows[0];

  return {
    id: firstRow.user_id,
    username: firstRow.username,
    email: firstRow.email || "",
    fullName: firstRow.full_name || "",
    status: firstRow.user_status,
    globalRole: firstRow.global_role || null,
    clientAssociations: result.rows
      .filter((row) => row.client_id)
      .map((row) => ({
        clientId: row.client_id,
        clientRole: row.client_role || null,
        status: row.client_user_status || null,
        clientStatus: row.client_status || null
      }))
  };
}

function resolveAuthorizedClientId(authenticatedUser, requestedClientId) {
  if (authenticatedUser.globalRole === "trackster_admin") {
    if (!requestedClientId) {
      return {
        success: false,
        statusCode: 400,
        error: "clientId is required for trackster_admin users."
      };
    }

    return {
      success: true,
      clientId: requestedClientId
    };
  }

  const activeClientAdminAssociation = authenticatedUser.clientAssociations.find(
    (association) =>
      association.clientRole === "client_admin" &&
      association.status === "active" &&
      association.clientStatus === "active"
  );

  if (!activeClientAdminAssociation) {
    return {
      success: false,
      statusCode: 403,
      error: "Only trackster_admin or active client_admin users can list client users."
    };
  }

  if (
    requestedClientId &&
    requestedClientId !== activeClientAdminAssociation.clientId
  ) {
    return {
      success: false,
      statusCode: 403,
      error: "client_admin users cannot list users from another client."
    };
  }

  return {
    success: true,
    clientId: activeClientAdminAssociation.clientId
  };
}

async function listClientUsers(clientId) {
  const query = `
    SELECT
      u.username,
      u.full_name,
      u.email,
      u.status AS user_status,

      cu.client_id,
      cu.status AS client_user_status,

      r.role_code AS client_role

    FROM trackster_client_users cu

    INNER JOIN trackster_users u
      ON u.id = cu.user_id

    INNER JOIN trackster_roles r
      ON r.id = cu.role_id

    WHERE cu.client_id = $1

    ORDER BY
      CASE r.role_code
        WHEN 'client_admin' THEN 0
        ELSE 1
      END,
      LOWER(u.username) ASC
  `;

  const result = await pool.query(query, [clientId]);

  return result.rows.map((row) => ({
    username: row.username,
    fullName: row.full_name || "",
    email: row.email || "",
    role: row.client_role,
    status: toUiStatus(row.client_user_status || row.user_status),
    clientId: row.client_id
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