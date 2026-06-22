import pg from "pg";

const { Pool } = pg;

const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,POST",
  "Content-Type": "application/json"
};

const validStatuses = ["active", "inactive", "suspended"];

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

  const dbClient = await pool.connect();

  try {
    const username = getAuthenticatedUsername(event);

    if (!username) {
      return buildResponse(401, {
        success: false,
        error: "Authenticated username was not found in token claims."
      });
    }

    const authenticatedUser = await getAuthenticatedUserContext(dbClient, username);

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

    const clientId = String(body.clientId || "").trim();
    const contactName = String(body.contactName || "").trim();
    const email = String(body.email || body.companyEmail || "").trim();
    const phone = String(body.phone || "").trim();
    const country = String(body.country || "").trim();
    const status = normalizeClientStatus(body.status, body.action, body.enabled);

    if (!clientId) {
      return buildResponse(400, {
        success: false,
        error: "clientId is required."
      });
    }

    if (!validStatuses.includes(status)) {
      return buildResponse(400, {
        success: false,
        error: "Invalid client status."
      });
    }

    await dbClient.query("BEGIN");

    const existingClient = await getClient(dbClient, clientId);

    if (!existingClient) {
      await dbClient.query("ROLLBACK");

      return buildResponse(404, {
        success: false,
        error: "Client was not found."
      });
    }

    const updatedClient = await updateClient(dbClient, {
      clientId,
      contactName,
      email,
      phone,
      country,
      status
    });

    let deactivatedUsers = [];

    if (status === "inactive") {
      deactivatedUsers = await deactivateClientUsers(dbClient, clientId);
    }

    await dbClient.query("COMMIT");

    return buildResponse(200, {
      success: true,
      message: "Client updated successfully.",
      updatedClient,
      deactivatedUsersCount: deactivatedUsers.length,
      deactivatedUsers
    });
  } catch (error) {
    try {
      await dbClient.query("ROLLBACK");
    } catch (rollbackError) {
      console.error("Unable to rollback Trackster client update transaction", rollbackError);
    }

    console.error("Unable to update Trackster client", error);

    return buildResponse(500, {
      success: false,
      error: "Unable to update client."
    });
  } finally {
    dbClient.release();
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

function normalizeClientStatus(status, action, enabled) {
  const normalizedAction = String(action || "").trim().toLowerCase();

  if (normalizedAction === "activate") {
    return "active";
  }

  if (normalizedAction === "deactivate" || normalizedAction === "disable") {
    return "inactive";
  }

  if (typeof enabled === "boolean") {
    return enabled ? "active" : "inactive";
  }

  const normalizedStatus = String(status || "").trim().toLowerCase();

  if (normalizedStatus === "active") {
    return "active";
  }

  if (normalizedStatus === "suspended") {
    return "suspended";
  }

  return "inactive";
}

function getAuthenticatedUsername(event) {
  const httpApiClaims = event?.requestContext?.authorizer?.jwt?.claims;
  const restApiClaims = event?.requestContext?.authorizer?.claims;

  const claims = httpApiClaims || restApiClaims || {};

  return String(
    claims["cognito:username"] ||
    claims.username ||
    claims.preferred_username ||
    claims.email ||
    ""
  ).trim();
}

async function getAuthenticatedUserContext(dbClient, username) {
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

  const result = await dbClient.query(query, [username]);

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

async function getClient(dbClient, clientId) {
  const query = `
    SELECT
      id,
      client_id,
      status
    FROM trackster_clients
    WHERE client_id = $1
    LIMIT 1
  `;

  const result = await dbClient.query(query, [clientId]);

  if (result.rows.length === 0) {
    return null;
  }

  return result.rows[0];
}

async function updateClient(dbClient, client) {
  const query = `
    UPDATE trackster_clients
    SET
      contact_name = $2,
      company_email = $3,
      phone = $4,
      country = $5,
      status = $6,
      updated_at = NOW()
    WHERE client_id = $1
    RETURNING
      client_id,
      company_name,
      company_email,
      contact_name,
      phone,
      country,
      status
  `;

  const result = await dbClient.query(query, [
    client.clientId,
    client.contactName,
    client.email,
    client.phone,
    client.country,
    client.status
  ]);

  const row = result.rows[0];

  return {
    clientId: row.client_id,
    name: row.company_name || "",
    email: row.company_email || "",
    contactName: row.contact_name || "",
    phone: row.phone || "",
    country: row.country || "",
    status: row.status || "inactive"
  };
}

async function deactivateClientUsers(dbClient, clientId) {
  const query = `
    UPDATE trackster_users u
    SET
      status = 'inactive',
      updated_at = NOW()
    WHERE u.id IN (
      SELECT cu.user_id
      FROM trackster_client_users cu
      WHERE cu.client_id = $1
    )
    AND u.status <> 'inactive'
    RETURNING
      u.username,
      u.email,
      u.full_name,
      u.status
  `;

  const result = await dbClient.query(query, [clientId]);

  return result.rows.map((row) => ({
    username: row.username,
    email: row.email || "",
    fullName: row.full_name || "",
    status: row.status || "inactive"
  }));
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}