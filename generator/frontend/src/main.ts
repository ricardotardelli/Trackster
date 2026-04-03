import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { Amplify } from 'aws-amplify';
import { fetchAuthSession } from 'aws-amplify/auth';
import { AppComponent } from './app/app.component';
import { cognitoConfig } from './app/auth/cognito.config';

const LOGIN_URL = '/login';

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

function goToLogin(): void {
  window.location.assign(LOGIN_URL);
}

async function start(): Promise<void> {
  const authenticated = await hasAuthenticatedSession();

  if (!authenticated) {
    goToLogin();
    return;
  }

  await bootstrapApplication(AppComponent, {
    providers: [provideHttpClient()]
  });
}

void start().catch((error) => {
  console.error('Application startup failed.', error);
  document.body.innerHTML = `
    <div style="padding:24px;font-family:Arial,sans-serif;">
      Application startup failed.
    </div>
  `;
});