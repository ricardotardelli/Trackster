import 'aws-amplify/auth/enable-oauth-listener';
import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { Amplify } from 'aws-amplify';
import { AppComponent } from './app/app.component';
import { cognitoConfig } from './app/auth/cognito.config';
import { AuthService } from './app/auth/auth.service';

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

async function start(): Promise<void> {
  const authService = new AuthService();

  const canStart = await authService.prepareApplicationStart();

  if (!canStart) return;

  await bootstrapApplication(AppComponent, {
    providers: [provideHttpClient()]
  });
}

void start().catch((error) => {
  console.error('Application startup failed.', error);
  document.body.innerHTML =
    '<div style="padding:24px;font-family:Arial,sans-serif;">Application startup failed.</div>';
});