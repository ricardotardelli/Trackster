import 'aws-amplify/auth/enable-oauth-listener';
import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { AppComponent } from './app/app.component';
import { AuthService, configureAuth } from './app/auth/auth.service';
import { environment } from './environments/environment';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';

async function start(): Promise<void> {
  const isLocalhost =
    window.location.hostname === 'localhost' ||
    window.location.hostname === '127.0.0.1';

  const shouldDisableAuth = environment.disableAuth && isLocalhost;

  if (!shouldDisableAuth) {
    configureAuth();

    const authService = new AuthService();
    const canStart = await authService.prepareApplicationStart();

    if (!canStart) {
      return;
    }
  }

  await bootstrapApplication(AppComponent, {
    providers: [provideHttpClient(), provideAnimationsAsync('noop'), provideAnimationsAsync(), provideAnimationsAsync()]
  });
}

void start().catch((error) => {
  console.error('Application startup failed.', error);
  document.body.innerHTML =
    '<div style="padding:24px;font-family:Arial,sans-serif;">Application startup failed.</div>';
});