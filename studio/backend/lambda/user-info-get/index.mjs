import {
  AdminGetUserCommand,
  AdminListGroupsForUserCommand,
  CognitoIdentityProviderClient,
  GetGroupCommand
} from "@aws-sdk/client-cognito-identity-provider";

const userPoolId = process.env.COGNITO_USER_POOL_ID || "";
const region = process.env.AWS_REGION || process.env.REGION || "eu-west-1";
const allowedOrigin = process.env.ALLOWED_ORIGIN || "*";

const cognitoClient = new CognitoIdentityProviderClient({ region });

const defaultHeaders = {
  "Access-Control-Allow-Origin": allowedOrigin,
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "OPTIONS,GET",
  "Content-Type": "application/json"
};

export const handler = async (event) => {
  const method = event?.requestContext?.http?.method || event?.httpMethod || "";

  console.log("Trackster user info get request", {
    method,
    path: event?.rawPath || event?.path
  });

  if (method === "OPTIONS") {
    return buildResponse(200, { success: true });
  }

  if (method !== "GET") {
    return buildResponse(405, {
      success: false,
      error: "Method not allowed."
    });
  }

  try {
    if (!userPoolId) {
      throw new Error("COGNITO_USER_POOL_ID is not configured.");
    }

    const username = getAuthenticatedUsername(event);

    if (!username) {
      return buildResponse(401, {
        success: false,
        error: "Authenticated username was not found in token claims."
      });
    }

    const user = await cognitoClient.send(
      new AdminGetUserCommand({
        UserPoolId: userPoolId,
        Username: username
      })
    );

    if (user.Enabled !== true) {
      return buildResponse(403, {
        success: false,
        error: "User is not active."
      });
    }

    const groups = await listGroupsForUser(user.Username || username);
    const userInfo = await buildUserInfo(user, groups);

    return buildResponse(200, {
      success: true,
      user: userInfo
    });
  } catch (error) {
    console.error("Unable to retrieve Trackster user information", error);

    if (error?.name === "UserNotFoundException") {
      return buildResponse(404, {
        success: false,
        error: "User was not found in Cognito."
      });
    }

    return buildResponse(500, {
      success: false,
      error: error?.message || "Unable to retrieve user information."
    });
  }
};

async function buildUserInfo(user, groups) {
  const username = user.Username || "";
  const attributes = user.UserAttributes || [];
  const isTracksterAdmin = groups.includes("trackster-admins");

  const clientGroups = groups.map(parseClientGroup).filter(Boolean);
  const clientAssociations = [];

  for (const clientGroup of clientGroups) {
    const group = await getGroupOrNull(clientGroup.groupName);
    const metadata = parseClientDescription(group?.Description);

    clientAssociations.push({
      clientId: clientGroup.clientId,
      clientRole: clientGroup.clientRole,
      status: user.Enabled === true ? "active" : "inactive",
      companyName: metadata.name || "",
      companyEmail: metadata.email || "",
      contactName: metadata.contactName || "",
      country: metadata.country || "",
      phone: metadata.phone || "",
      clientStatus: metadata.status || "active"
    });
  }

  clientAssociations.sort((a, b) => {
    if (a.clientStatus === "active" && b.clientStatus !== "active") return -1;
    if (a.clientStatus !== "active" && b.clientStatus === "active") return 1;
    if (a.clientRole === "client_admin" && b.clientRole !== "client_admin") return -1;
    if (a.clientRole !== "client_admin" && b.clientRole === "client_admin") return 1;
    return a.clientId.localeCompare(b.clientId);
  });

  const primaryClient = clientAssociations[0] || null;

  return {
    id: getAttribute(attributes, "sub"),
    username,
    email: getAttribute(attributes, "email"),
    fullName: getAttribute(attributes, "name"),
    status: user.Enabled === true ? "active" : "inactive",
    globalRole: isTracksterAdmin ? "trackster_admin" : null,
    clientRole: isTracksterAdmin ? null : primaryClient?.clientRole || null,
    clientId: primaryClient?.clientId || "",
    companyName: primaryClient?.companyName || "",
    companyEmail: primaryClient?.companyEmail || "",
    contactName: primaryClient?.contactName || "",
    country: primaryClient?.country || "",
    phone: primaryClient?.phone || "",
    createdAt: user.UserCreateDate || null,
    updatedAt: user.UserLastModifiedDate || null,
    clientAssociations
  };
}

async function listGroupsForUser(username) {
  const groups = [];
  let nextToken;

  do {
    const result = await cognitoClient.send(
      new AdminListGroupsForUserCommand({
        UserPoolId: userPoolId,
        Username: username,
        Limit: 60,
        NextToken: nextToken
      })
    );

    for (const group of result.Groups || []) {
      if (group.GroupName) groups.push(group.GroupName);
    }

    nextToken = result.NextToken;
  } while (nextToken);

  return groups;
}

async function getGroupOrNull(groupName) {
  try {
    return await cognitoClient.send(
      new GetGroupCommand({
        UserPoolId: userPoolId,
        GroupName: groupName
      })
    );
  } catch (error) {
    if (error?.name === "ResourceNotFoundException") return null;
    throw error;
  }
}

function parseClientGroup(groupName) {
  const match = /^(\d{8})-(admins|users)$/.exec(groupName);
  if (!match) return null;

  return {
    groupName,
    clientId: match[1],
    clientRole: match[2] === "admins" ? "client_admin" : "client_user"
  };
}

function parseClientDescription(description) {
  const raw = normalize(description);

  if (!raw) {
    return {
      name: "",
      status: "active",
      contactName: "",
      email: "",
      phone: "",
      country: ""
    };
  }

  try {
    const parsed = JSON.parse(raw);

    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return {
        name: normalize(parsed.name || parsed.companyName),
        status: normalize(parsed.status).toLowerCase() || "active",
        contactName: normalize(parsed.contactName),
        email: normalize(parsed.email || parsed.companyEmail),
        phone: normalize(parsed.phone),
        country: normalize(parsed.country)
      };
    }
  } catch {
    return {
      name: raw,
      status: "active",
      contactName: "",
      email: "",
      phone: "",
      country: ""
    };
  }

  return {
    name: "",
    status: "active",
    contactName: "",
    email: "",
    phone: "",
    country: ""
  };
}

function getAuthenticatedUsername(event) {
  const claims =
    event?.requestContext?.authorizer?.jwt?.claims ||
    event?.requestContext?.authorizer?.claims ||
    {};

  return normalize(
    claims["cognito:username"] ||
    claims.username ||
    claims.preferred_username ||
    claims.email ||
    ""
  );
}

function getAttribute(attributes, name) {
  return (attributes || []).find((attribute) => attribute.Name === name)?.Value || "";
}

function normalize(value) {
  return String(value || "").trim();
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}
