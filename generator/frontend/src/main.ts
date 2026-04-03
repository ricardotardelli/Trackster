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
          responseType: 'code'
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

async function waitForSession(): Promise<boolean> {
  for (let i = 0; i < 20; i++) {
    if (await hasAuthenticatedSession()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  return false;
}

async function start(): Promise<void> {
  const url = new URL(window.location.href);
  const hasOAuthReturn =
    url.searchParams.has('code') ||
    url.searchParams.has('state') ||
    url.searchParams.has('error');

  if (hasOAuthReturn) {
    const ok = await waitForSession();

    if (!ok) {
      document.body.innerHTML =
        '<div style="padding:24px;font-family:Arial,sans-serif;">Authentication failed.</div>';
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

void start().catch((error) => {
  console.error('Application startup failed.', error);
  document.body.innerHTML =
    '<div style="padding:24px;font-family:Arial,sans-serif;">Application startup failed.</div>';
});