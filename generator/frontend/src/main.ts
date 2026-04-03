import 'aws-amplify/auth/enable-oauth-listener';
import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { Amplify } from 'aws-amplify';
import { fetchAuthSession, signInWithRedirect } from 'aws-amplify/auth';
import { AppComponent } from './app/app.component';
import { cognitoConfig } from './app/auth/cognito.config';

Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId: cognitoConfig.userPoolId,
      userPoolClientId: cognitoConfig.userPoolClientId,
      loginWith: {
        oauth: {
          domain: cognitoConfig.domain,
          scopes: cognitoConfig.scopes,
          redirectSignIn: [cognitoConfig.redirectSignIn],
          redirectSignOut: [cognitoConfig.redirectSignOut],
          responseType: 'token'
        }
      }
    }
  }
});

async function hasAuthenticatedSession(): Promise<boolean> {
  try {
    const session = await fetchAuthSession();
    return !!session.tokens?.idToken || !!session.tokens?.accessToken;
  } catch {
    return false;
  }
}

async function waitForOAuthSession(): Promise<boolean> {
  for (let attempt = 0; attempt < 20; attempt++) {
    if (await hasAuthenticatedSession()) {
      return true;
    }

    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  return false;
}

async function start(): Promise<void> {
  const isOAuthReturn =
    window.location.hash.includes('id_token=') ||
    window.location.hash.includes('access_token=') ||
    window.location.hash.includes('error=') ||
    window.location.search.includes('code=') ||
    window.location.search.includes('state=') ||
    window.location.search.includes('error=');

  if (isOAuthReturn) {
    const authenticated = await waitForOAuthSession();

    if (!authenticated) {
      document.body.innerHTML = '<div style="padding:24px;font-family:Arial,sans-serif;">Authentication failed.</div>';
      return;
    }

    window.history.replaceState({}, document.title, window.location.pathname);
  } else {
    const authenticated = await hasAuthenticatedSession();

    if (!authenticated) {
      await signInWithRedirect();
      return;
    }
  }

  await bootstrapApplication(AppComponent, {
    providers: [provideHttpClient()]
  });
}

start().catch((err) => {
  console.error(err);
  document.body.innerHTML = '<div style="padding:24px;font-family:Arial,sans-serif;">Application startup failed.</div>';
});