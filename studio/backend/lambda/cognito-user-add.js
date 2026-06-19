import {
  CognitoIdentityProviderClient,
  AdminCreateUserCommand,
  AdminGetUserCommand
} from '@aws-sdk/client-cognito-identity-provider';

const cognitoClient = new CognitoIdentityProviderClient({
  region: process.env.AWS_REGION || process.env.LAMBDA_REGION || 'us-east-1'
});

function normalizeString(value) {
  return String(value || '').trim();
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
    const temporaryPassword = normalizeString(event?.temporaryPassword);

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
      },
      {
        Name: 'email_verified',
        Value: 'true'
      }
    ];

    if (fullName) {
      userAttributes.push({
        Name: 'name',
        Value: fullName
      });
    }

    const commandInput = {
      UserPoolId: userPoolId,
      Username: username,
      UserAttributes: userAttributes,
      DesiredDeliveryMediums: ['EMAIL']
    };

    if (temporaryPassword) {
      commandInput.TemporaryPassword = temporaryPassword;
    }

    const result = await cognitoClient.send(
      new AdminCreateUserCommand(commandInput)
    );

    return {
      success: true,
      message: 'External login account created successfully.',
      externalLoginCreated: true,
      username,
      email,
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