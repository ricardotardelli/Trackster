import 'aws-amplify/auth/enable-oauth-listener';
import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { Amplify } from 'aws-amplify';
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

bootstrapApplication(AppComponent, {
  providers: [
    provideHttpClient()
  ]
}).catch((err) => console.error(err));

