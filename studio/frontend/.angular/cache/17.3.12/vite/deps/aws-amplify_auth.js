import {
  PasskeyError,
  PasskeyErrorCode,
  assertCredentialIsPkcWithAuthenticatorAttestationResponse,
  assertPasskeyError,
  assertValidCredentialCreationOptions,
  autoSignIn,
  confirmResetPassword,
  confirmSignIn,
  confirmSignUp,
  confirmUserAttribute,
  deleteUser,
  deleteUserAttributes,
  deserializeJsonToPkcCreationOptions,
  fetchDevices,
  fetchMFAPreference,
  fetchUserAttributes,
  forgetDevice,
  getIsPasskeySupported,
  handlePasskeyError,
  passkeyErrorMap,
  rememberDevice,
  resendSignUpCode,
  resetPassword,
  sendUserAttributeVerificationCode,
  serializePkcWithAttestationToJson,
  setUpTOTP,
  signIn,
  signInWithRedirect,
  signOut,
  signUp,
  updateMFAPreference,
  updatePassword,
  updateUserAttribute,
  updateUserAttributes,
  verifyTOTPSetup
} from "./chunk-GSTMTGK7.js";
import {
  Amplify,
  AuthAction,
  AuthError,
  DEFAULT_SERVICE_CLIENT_API_CONFIG,
  assertAuthTokens,
  assertTokenProviderConfig,
  cognitoUserPoolTransferHandler,
  composeServiceApi,
  createCognitoUserPoolEndpointResolver,
  createUserPoolDeserializer,
  createUserPoolSerializer,
  decodeJWT,
  fetchAuthSession2 as fetchAuthSession,
  getAuthUserAgentValue,
  getCurrentUser,
  getRegionFromUserPoolId
} from "./chunk-5KMNRBIN.js";
import "./chunk-WQJEAQSM.js";
import {
  __async,
  __spreadValues
} from "./chunk-MF5NBIAP.js";

// node_modules/@aws-amplify/auth/dist/esm/client/utils/passkey/errors/handlePasskeyRegistrationError.mjs
var handlePasskeyRegistrationError = (err) => {
  if (err instanceof PasskeyError) {
    return err;
  }
  if (err instanceof Error) {
    if (err.name === "InvalidStateError") {
      const { message, recoverySuggestion } = passkeyErrorMap[PasskeyErrorCode.PasskeyAlreadyExists];
      return new PasskeyError({
        name: PasskeyErrorCode.PasskeyAlreadyExists,
        message,
        recoverySuggestion,
        underlyingError: err
      });
    }
    if (err.name === "NotAllowedError") {
      const { message, recoverySuggestion } = passkeyErrorMap[PasskeyErrorCode.PasskeyRegistrationCanceled];
      return new PasskeyError({
        name: PasskeyErrorCode.PasskeyRegistrationCanceled,
        message,
        recoverySuggestion,
        underlyingError: err
      });
    }
  }
  return handlePasskeyError(err);
};

// node_modules/@aws-amplify/auth/dist/esm/client/utils/passkey/registerPasskey.mjs
var registerPasskey = (input) => __async(void 0, null, function* () {
  try {
    const isPasskeySupported = getIsPasskeySupported();
    assertPasskeyError(isPasskeySupported, PasskeyErrorCode.PasskeyNotSupported);
    const passkeyCreationOptions = deserializeJsonToPkcCreationOptions(input);
    const credential = yield navigator.credentials.create({
      publicKey: passkeyCreationOptions
    });
    assertCredentialIsPkcWithAuthenticatorAttestationResponse(credential);
    return serializePkcWithAttestationToJson(credential);
  } catch (err) {
    throw handlePasskeyRegistrationError(err);
  }
});

// node_modules/@aws-amplify/auth/dist/esm/foundation/factories/serviceClients/cognitoIdentityProvider/createStartWebAuthnRegistrationClient.mjs
var createStartWebAuthnRegistrationClient = (config) => composeServiceApi(cognitoUserPoolTransferHandler, createUserPoolSerializer("StartWebAuthnRegistration"), createUserPoolDeserializer(), __spreadValues(__spreadValues({}, DEFAULT_SERVICE_CLIENT_API_CONFIG), config));

// node_modules/@aws-amplify/auth/dist/esm/foundation/factories/serviceClients/cognitoIdentityProvider/createCompleteWebAuthnRegistrationClient.mjs
var createCompleteWebAuthnRegistrationClient = (config) => composeServiceApi(cognitoUserPoolTransferHandler, createUserPoolSerializer("CompleteWebAuthnRegistration"), createUserPoolDeserializer(), __spreadValues(__spreadValues({}, DEFAULT_SERVICE_CLIENT_API_CONFIG), config));

// node_modules/@aws-amplify/auth/dist/esm/client/apis/associateWebAuthnCredential.mjs
function associateWebAuthnCredential() {
  return __async(this, null, function* () {
    const authConfig = Amplify.getConfig().Auth?.Cognito;
    assertTokenProviderConfig(authConfig);
    const { userPoolEndpoint, userPoolId } = authConfig;
    const { tokens } = yield fetchAuthSession();
    assertAuthTokens(tokens);
    const startWebAuthnRegistration = createStartWebAuthnRegistrationClient({
      endpointResolver: createCognitoUserPoolEndpointResolver({
        endpointOverride: userPoolEndpoint
      })
    });
    const { CredentialCreationOptions: credentialCreationOptions } = yield startWebAuthnRegistration({
      region: getRegionFromUserPoolId(userPoolId),
      userAgentValue: getAuthUserAgentValue(AuthAction.StartWebAuthnRegistration)
    }, {
      AccessToken: tokens.accessToken.toString()
    });
    assertValidCredentialCreationOptions(credentialCreationOptions);
    const cred = yield registerPasskey(credentialCreationOptions);
    const completeWebAuthnRegistration = createCompleteWebAuthnRegistrationClient({
      endpointResolver: createCognitoUserPoolEndpointResolver({
        endpointOverride: userPoolEndpoint
      })
    });
    yield completeWebAuthnRegistration({
      region: getRegionFromUserPoolId(userPoolId),
      userAgentValue: getAuthUserAgentValue(AuthAction.CompleteWebAuthnRegistration)
    }, {
      AccessToken: tokens.accessToken.toString(),
      Credential: cred
    });
  });
}

// node_modules/@aws-amplify/auth/dist/esm/foundation/factories/serviceClients/cognitoIdentityProvider/createListWebAuthnCredentialsClient.mjs
var createListWebAuthnCredentialsClient = (config) => composeServiceApi(cognitoUserPoolTransferHandler, createUserPoolSerializer("ListWebAuthnCredentials"), createUserPoolDeserializer(), __spreadValues(__spreadValues({}, DEFAULT_SERVICE_CLIENT_API_CONFIG), config));

// node_modules/@aws-amplify/auth/dist/esm/foundation/apis/listWebAuthnCredentials.mjs
function listWebAuthnCredentials(amplify, input) {
  return __async(this, null, function* () {
    const authConfig = amplify.getConfig().Auth?.Cognito;
    assertTokenProviderConfig(authConfig);
    const { userPoolEndpoint, userPoolId } = authConfig;
    const { tokens } = yield amplify.Auth.fetchAuthSession();
    assertAuthTokens(tokens);
    const listWebAuthnCredentialsResult = createListWebAuthnCredentialsClient({
      endpointResolver: createCognitoUserPoolEndpointResolver({
        endpointOverride: userPoolEndpoint
      })
    });
    const { Credentials: commandCredentials = [], NextToken: nextToken } = yield listWebAuthnCredentialsResult({
      region: getRegionFromUserPoolId(userPoolId),
      userAgentValue: getAuthUserAgentValue(AuthAction.ListWebAuthnCredentials)
    }, {
      AccessToken: tokens.accessToken.toString(),
      MaxResults: input?.pageSize,
      NextToken: input?.nextToken
    });
    const credentials = commandCredentials.map((item) => ({
      credentialId: item.CredentialId,
      friendlyCredentialName: item.FriendlyCredentialName,
      relyingPartyId: item.RelyingPartyId,
      authenticatorAttachment: item.AuthenticatorAttachment,
      authenticatorTransports: item.AuthenticatorTransports,
      createdAt: item.CreatedAt ? new Date(item.CreatedAt * 1e3) : void 0
    }));
    return {
      credentials,
      nextToken
    };
  });
}

// node_modules/@aws-amplify/auth/dist/esm/client/apis/listWebAuthnCredentials.mjs
function listWebAuthnCredentials2(input) {
  return __async(this, null, function* () {
    return listWebAuthnCredentials(Amplify, input);
  });
}

// node_modules/@aws-amplify/auth/dist/esm/foundation/factories/serviceClients/cognitoIdentityProvider/createDeleteWebAuthnCredentialClient.mjs
var createDeleteWebAuthnCredentialClient = (config) => composeServiceApi(cognitoUserPoolTransferHandler, createUserPoolSerializer("DeleteWebAuthnCredential"), createUserPoolDeserializer(), __spreadValues(__spreadValues({}, DEFAULT_SERVICE_CLIENT_API_CONFIG), config));

// node_modules/@aws-amplify/auth/dist/esm/foundation/apis/deleteWebAuthnCredential.mjs
function deleteWebAuthnCredential(amplify, input) {
  return __async(this, null, function* () {
    const authConfig = amplify.getConfig().Auth?.Cognito;
    assertTokenProviderConfig(authConfig);
    const { userPoolEndpoint, userPoolId } = authConfig;
    const { tokens } = yield amplify.Auth.fetchAuthSession();
    assertAuthTokens(tokens);
    const deleteWebAuthnCredentialResult = createDeleteWebAuthnCredentialClient({
      endpointResolver: createCognitoUserPoolEndpointResolver({
        endpointOverride: userPoolEndpoint
      })
    });
    yield deleteWebAuthnCredentialResult({
      region: getRegionFromUserPoolId(userPoolId),
      userAgentValue: getAuthUserAgentValue(AuthAction.DeleteWebAuthnCredential)
    }, {
      AccessToken: tokens.accessToken.toString(),
      CredentialId: input.credentialId
    });
  });
}

// node_modules/@aws-amplify/auth/dist/esm/client/apis/deleteWebAuthnCredential.mjs
function deleteWebAuthnCredential2(input) {
  return __async(this, null, function* () {
    return deleteWebAuthnCredential(Amplify, input);
  });
}
export {
  AuthError,
  associateWebAuthnCredential,
  autoSignIn,
  confirmResetPassword,
  confirmSignIn,
  confirmSignUp,
  confirmUserAttribute,
  decodeJWT,
  deleteUser,
  deleteUserAttributes,
  deleteWebAuthnCredential2 as deleteWebAuthnCredential,
  fetchAuthSession,
  fetchDevices,
  fetchMFAPreference,
  fetchUserAttributes,
  forgetDevice,
  getCurrentUser,
  listWebAuthnCredentials2 as listWebAuthnCredentials,
  rememberDevice,
  resendSignUpCode,
  resetPassword,
  sendUserAttributeVerificationCode,
  setUpTOTP,
  signIn,
  signInWithRedirect,
  signOut,
  signUp,
  updateMFAPreference,
  updatePassword,
  updateUserAttribute,
  updateUserAttributes,
  verifyTOTPSetup
};
//# sourceMappingURL=aws-amplify_auth.js.map
