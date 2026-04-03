import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { Amplify } from 'aws-amplify';
import { fetchAuthSession } from 'aws-amplify/auth';
import { AppComponent } from './app/app.component';
import { cognitoConfig } from './app/auth/cognito.config';

const LOGIN_URL =
  'https://us-east-1rzmuaolzz.auth.us-east-1.amazoncognito.com/login' +
  '?client_id=7g4slp3sne6rsvtpiacglgjt8o' +
  '&response_type=code' +
  '&scope=openid+email' +
  '&redirect_uri=https://www.trackster.pt/';

Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId: cognitoConfig.userPoolId,
      userPoolClientId: cognitoConfig.userPoolClientId
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

function showAuthError(message: string): void {
  document.body.innerHTML = `
    <div style="padding:24px;font-family:Arial,sans-serif;">
      ${message}
    </div>
  `;
}

async function start(): Promise<void> {
  const url = new URL(window.location.href);
  const hasOAuthCode = url.searchParams.has('code');
  const hasOAuthError = url.searchParams.has('error');

  if (hasOAuthError) {
    showAuthError('Authentication failed.');
    return;
  }

  const authenticated = await hasAuthenticatedSession();

  if (authenticated) {
    await bootstrapApplication(AppComponent, {
      providers: [provideHttpClient()]
    });
    return;
  }

  if (hasOAuthCode) {
    showAuthError(
      'Authentication callback received, but this frontend is not processing the authorization code.'
    );
    return;
  }

  window.location.assign(LOGIN_URL);
}

void start().catch((error) => {
  console.error('Application startup failed.', error);
  showAuthError('Application startup failed.');
});