import {
  CognitoIdentityProviderClient,
  AdminGetUserCommand,
  AdminUpdateUserAttributesCommand,
  AdminEnableUserCommand,
  AdminDisableUserCommand
} from '@aws-sdk/client-cognito-identity-provider';

const cognitoClient = new CognitoIdentityProviderClient({
  region: process.env.AWS_REGION || process.env.LAMBDA_REGION || 'us-east-1'
});

function normalizeString(value) {
  return String(value || '').trim();
}

function normalizeStatus(value) {
  return normalizeString(value);
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
    const status = normalizeStatus(event?.status);

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

    if (!email.includes('@')) {
      return {
        success: false,
        message: 'Invalid email.'
      };
    }

    const alreadyExists = await cognitoUserExists(userPoolId, username);

    if (!alreadyExists) {
      return {
        success: false,
        externalLoginNotFound: true,
        message: 'External login account was not found.'
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

    await cognitoClient.send(
      new AdminUpdateUserAttributesCommand({
        UserPoolId: userPoolId,
        Username: username,
        UserAttributes: userAttributes
      })
    );

    if (status === 'Active') {
      await cognitoClient.send(
        new AdminEnableUserCommand({
          UserPoolId: userPoolId,
          Username: username
        })
      );
    }

    if (status === 'Inactive' || status === 'Suspended') {
      await cognitoClient.send(
        new AdminDisableUserCommand({
          UserPoolId: userPoolId,
          Username: username
        })
      );
    }

    return {
      success: true,
      message: 'External login account updated successfully.',
      externalLoginUpdated: true,
      username,
      email,
      fullName,
      status: status || null
    };
  } catch (error) {
    console.error('identity-update-user error:', error);

    return {
      success: false,
      message: error?.message || 'Unable to update external login account.'
    };
  }
};