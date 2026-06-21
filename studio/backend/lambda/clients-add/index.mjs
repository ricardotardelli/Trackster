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
  console.log("Trackster admin add client request", {
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
        error: "Only active trackster_admin users can add clients."
      });
    }

    const body = parseBody(event);
    const input = normalizeClientInput(body);
    const validationError = validateClientInput(input);

    if (validationError) {
      return buildResponse(400, {
        success: false,
        error: validationError
      });
    }

    const existingClient = await findClientByClientId(input.clientId);

    if (existingClient) {
      return buildResponse(409, {
        success: false,
        error: "A client with this clientId already exists."
      });
    }

    const client = await addClient(input);

    return buildResponse(201, {
      success: true,
      message: "Client added successfully.",
      client
    });
  } catch (error) {
    console.error("Unable to add Trackster client", error);

    return buildResponse(500, {
      success: false,
      error: "Unable to add client."
    });
  }
};

function parseBody(event) {
  if (!event?.body) {
    return {};
  }

  if (event.isBase64Encoded) {
    const decodedBody = Buffer.from(event.body, "base64").toString("utf-8");
    return JSON.parse(decodedBody);
  }

  if (typeof event.body === "string") {
    return JSON.parse(event.body);
  }

  return event.body;
}

function normalizeClientInput(body) {
  return {
    clientId: String(body.clientId || body.client_id || "").trim(),
    companyName: String(body.companyName || body.company_name || body.name || "").trim(),
    companyEmail: String(body.companyEmail || body.company_email || body.email || "").trim(),
    contactName: String(body.contactName || body.contact_name || "").trim(),
    country: String(body.country || "").trim(),
    phone: String(body.phone || "").trim(),
    status: String(body.status || "active").trim().toLowerCase()
  };
}

function validateClientInput(input) {
  if (!input.clientId) {
    return "clientId is required.";
  }

  if (input.clientId.length > 32) {
    return "clientId must have at most 32 characters.";
  }

  if (!/^[A-Za-z0-9_-]+$/.test(input.clientId)) {
    return "clientId can only contain letters, numbers, underscores, and hyphens.";
  }

  if (!input.companyName) {
    return "companyName is required.";
  }

  if (input.companyName.length > 255) {
    return "companyName must have at most 255 characters.";
  }

  if (input.companyEmail && input.companyEmail.length > 255) {
    return "companyEmail must have at most 255 characters.";
  }

  if (input.contactName && input.contactName.length > 255) {
    return "contactName must have at most 255 characters.";
  }

  if (input.country && input.country.length > 255) {
    return "country must have at most 255 characters.";
  }

  if (input.phone && input.phone.length > 64) {
    return "phone must have at most 64 characters.";
  }

  if (!validStatuses.includes(input.status)) {
    return "status must be active, inactive, or suspended.";
  }

  return "";
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

async function findClientByClientId(clientId) {
  const query = `
    SELECT
      c.client_id

    FROM trackster_clients c

    WHERE LOWER(c.client_id) = LOWER($1)

    LIMIT 1
  `;

  const result = await pool.query(query, [clientId]);

  return result.rows.length > 0 ? result.rows[0] : null;
}

async function addClient(input) {
  const query = `
    INSERT INTO trackster_clients (
      client_id,
      company_name,
      company_email,
      contact_name,
      country,
      phone,
      status
    )
    VALUES (
      $1,
      $2,
      NULLIF($3, ''),
      NULLIF($4, ''),
      NULLIF($5, ''),
      NULLIF($6, ''),
      $7
    )
    RETURNING
      client_id,
      company_name,
      company_email,
      contact_name,
      phone,
      country,
      status
  `;

  const values = [
    input.clientId,
    input.companyName,
    input.companyEmail,
    input.contactName,
    input.country,
    input.phone,
    input.status
  ];

  const result = await pool.query(query, values);
  const row = result.rows[0];

  return {
    clientId: row.client_id,
    name: row.company_name || "",
    email: row.company_email || "",
    contactName: row.contact_name || "",
    phone: row.phone || "",
    country: row.country || "",
    status: toUiStatus(row.status),
    users: 0,
    admins: 0
  };
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