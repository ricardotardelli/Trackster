import 'aws-amplify/auth/enable-oauth-listener';
import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { AppComponent } from './app/app.component';
import { AuthService, configureAuth } from './app/auth/auth.service';

async function start(): Promise<void> {
  configureAuth();

  const authService = new AuthService();
  const canStart = await authService.prepareApplicationStart();

  if (!canStart) {
    return;
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