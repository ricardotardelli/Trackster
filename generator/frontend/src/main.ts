import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { Amplify } from 'aws-amplify';
import { AppComponent } from './app/app.component';
import { cognitoConfig } from './app/auth/cognito.config';

Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId: cognitoConfig.userPoolId,
      userPoolClientId: cognitoConfig.userPoolClientId
    }
  }
});

bootstrapApplication(AppComponent, {
  providers: [
    provideHttpClient()
  ]
}).catch((err) => console.error(err));