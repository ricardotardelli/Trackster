import { fetchAuthSession, signInWithRedirect } from 'aws-amplify/auth';

export class AuthService {
  private async hasAuthenticatedSession(): Promise<boolean> {
    try {
      const session = await fetchAuthSession();

      // Optional debug logs (remove in production)
      console.log('ID TOKEN:', session.tokens?.idToken?.toString() ?? null);
      console.log('ACCESS TOKEN:', session.tokens?.accessToken?.toString() ?? null);

      return !!session.tokens?.idToken || !!session.tokens?.accessToken;
    } catch {
      return false;
    }
  }

  private async waitForSession(): Promise<boolean> {
    for (let i = 0; i < 20; i++) {
      if (await this.hasAuthenticatedSession()) {
        return true;
      }
      await new Promise((resolve) => setTimeout(resolve, 300));
    }
    return false;
  }

  private isOAuthReturn(): boolean {
    const url = new URL(window.location.href);

    return (
      url.searchParams.has('code') ||
      url.searchParams.has('state') ||
      url.searchParams.has('error')
    );
  }

  private cleanUrl(): void {
    window.history.replaceState({}, document.title, window.location.pathname);
  }

  /**
   * Main entry point used by main.ts
   * Controls the entire auth flow before app bootstrap
   */
  async prepareApplicationStart(): Promise<boolean> {
    if (this.isOAuthReturn()) {
      const ok = await this.waitForSession();

      if (!ok) {
        document.body.innerHTML =
          '<div style="padding:24px;font-family:Arial,sans-serif;">Authentication failed.</div>';
        return false;
      }

      this.cleanUrl();
      return true;
    }

    const authenticated = await this.hasAuthenticatedSession();

    if (!authenticated) {
      await signInWithRedirect();
      return false;
    }

    return true;
  }
}