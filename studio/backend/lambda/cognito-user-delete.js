import {
  CognitoIdentityProviderClient,
  AdminDeleteUserCommand
} from '@aws-sdk/client-cognito-identity-provider';

const identityClient = new CognitoIdentityProviderClient({
  region: process.env.AWS_REGION || process.env.COGNITO_REGION || 'us-east-1'
});

export const handler = async (event) => {
  const username = String(event?.username || '').trim();

  if (!username) {
    return {
      success: false,
      errorCode: 'MISSING_USERNAME',
      message: 'Missing username.'
    };
  }

  const userPoolId = process.env.COGNITO_USER_POOL_ID;

  if (!userPoolId) {
    return {
      success: false,
      errorCode: 'MISSING_USER_POOL_ID',
      message: 'Missing identity configuration.'
    };
  }

  try {
    await identityClient.send(
      new AdminDeleteUserCommand({
        UserPoolId: userPoolId,
        Username: username
      })
    );

    return {
      success: true,
      externalLoginDeleted: true,
      externalLoginWasMissing: false,
      message: 'External login account deleted successfully.'
    };
  } catch (error) {
    if (error?.name === 'UserNotFoundException') {
      return {
        success: true,
        externalLoginDeleted: false,
        externalLoginWasMissing: true,
        message: 'External login account was already unavailable.'
      };
    }

    console.error('trackster-cognito-user-delete error:', error);

    return {
      success: false,
      errorCode: error?.name || 'IDENTITY_DELETE_ERROR',
      message: 'Unable to delete external login account.'
    };
  }
};