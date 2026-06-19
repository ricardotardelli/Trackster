import { CognitoIdentityProviderClient, AdminCreateUserCommand, AdminGetUserCommand } from '@aws-sdk/client-cognito-identity-provider';

const cognitoClient = new CognitoIdentityProviderClient({
  region: process.env.AWS_REGION || process.env.LAMBDA_REGION || 'us-east-1'
});

function normalizeString(value) {
  return String(value || '').trim();
}

function buildTemporaryPassword(clientId, username, providedTemporaryPassword) {
  const normalizedProvidedPassword = normalizeString(providedTemporaryPassword);

  if (normalizedProvidedPassword) {
    return normalizedProvidedPassword;
  }

  const normalizedClientId = normalizeString(clientId);
  const normalizedUsername = normalizeString(username);

  return `${normalizedClientId}.${normalizedUsername}#T1`;
}

async function cognitoUserExists(userPoolId, username) {
  try {
    await cognitoClient.send(
      new AdminGetUserCommand({
        UserPoolId: userPoolId,
        Username: username
      })
    );

    return true;
  } catch (error) {
    if (error?.name === 'UserNotFoundException') {
      return false;
    }

    throw error;
  }
}

export const handler = async (event) => {
  try {
    const userPoolId = normalizeString(process.env.USER_POOL_ID);

    if (!userPoolId) {
      throw new Error('Missing required environment variable: USER_POOL_ID');
    }

    const username = normalizeString(event?.username);
    const email = normalizeString(event?.email);
    const fullName = normalizeString(event?.fullName);
    const clientId = normalizeString(event?.clientId);
    const temporaryPassword = buildTemporaryPassword(
      clientId,
      username,
      event?.temporaryPassword
    );

    if (!username) {
      return {
        success: false,
        message: 'Missing required field: username.'
      };
    }

    if (!email) {
      return {
        success: false,
        message: 'Missing required field: email.'
      };
    }

    if (!clientId) {
      return {
        success: false,
        message: 'Missing required field: clientId.'
      };
    }

    const alreadyExists = await cognitoUserExists(userPoolId, username);

    if (alreadyExists) {
      return {
        success: false,
        externalLoginAlreadyExists: true,
        message: 'External login account already exists.'
      };
    }

    const userAttributes = [
      {
        Name: 'email',
        Value: email
      }
    ];

    if (fullName) {
      userAttributes.push({
        Name: 'name',
        Value: fullName
      });
    }

    const result = await cognitoClient.send(
      new AdminCreateUserCommand({
        UserPoolId: userPoolId,
        Username: username,
        TemporaryPassword: temporaryPassword,
        UserAttributes: userAttributes,
        DesiredDeliveryMediums: ['EMAIL']
      })
    );

    return {
      success: true,
      message: 'External login account created successfully.',
      externalLoginCreated: true,
      username,
      email,
      temporaryPasswordCreated: true,
      userStatus: result?.User?.UserStatus || null
    };
  } catch (error) {
    console.error('identity-create-user error:', error);

    return {
      success: false,
      message: error?.message || 'Unable to create external login account.'
    };
  }
};