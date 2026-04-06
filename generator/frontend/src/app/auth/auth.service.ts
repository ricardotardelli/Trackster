import { Amplify } from 'aws-amplify';
import { Injectable } from '@angular/core';
import { fetchAuthSession, signInWithRedirect, signOut, getCurrentUser } from 'aws-amplify/auth';
import { cognitoConfig } from './cognito.config';

let amplifyConfigured = false;

export function configureAuth(): void {
  if (amplifyConfigured) {
    return;
  }

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

  amplifyConfigured = true;
}

@Injectable({ providedIn: 'root' })
export class AuthService {
  private async hasAuthenticatedSession(): Promise<boolean> {
    try {
      const session = await fetchAuthSession();

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

  async logout(): Promise<void> {
    await signOut({ global: true });
  }

  async getUsername(): Promise<string | null> {
    try {
      const user = await getCurrentUser();
      return user.username;
    } 
    catch {
      return null;
    }
  }
  
  async isAuthenticated(): Promise<boolean> {
    try {
      const user = await getCurrentUser();
      const session = await fetchAuthSession();
      return !!user && !!session.tokens?.idToken;
    } 
    catch {
      return false;
    }
  }
}