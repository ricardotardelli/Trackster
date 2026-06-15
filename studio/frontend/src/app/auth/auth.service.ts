import { Amplify } from 'aws-amplify';
import { Injectable } from '@angular/core';
import {
  fetchAuthSession,
  signInWithRedirect,
  signOut,
  getCurrentUser
} from 'aws-amplify/auth';
import { cognitoConfig } from './cognito.config';
import { environment } from '../../environments/environment';

export type TracksterGlobalRole = 'trackster_admin' | null;
export type TracksterClientRole = 'client_admin' | 'client_user' | null;

export interface TracksterUserAccessContext {
  isAuthenticated: boolean;
  username: string;
  cognitoSub: string;
  email: string;
  name: string;
  globalRole: TracksterGlobalRole;
  clientRole: TracksterClientRole;
  clientId: string;
  groups: string[];
  idToken: string | null;
  accessToken: string | null;
}

let amplifyConfigured = false;

function isLocalhost(): boolean {
  return (
    window.location.hostname === 'localhost' ||
    window.location.hostname === '127.0.0.1'
  );
}

function isAuthDisabled(): boolean {
  return environment.disableAuth && isLocalhost();
}

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly devProfile: 'trackster_admin' | 'client_admin' | 'client_user' = 'trackster_admin';
  private readonly devClientId = '00000000';

  private authDisabled(): boolean {
    return isAuthDisabled();
  }

  configureAuth(): void {
    if (amplifyConfigured || this.authDisabled()) {
      return;
    }

    Amplify.configure({
      Auth: {
        Cognito: {
          userPoolId: cognitoConfig.userPoolId,
          userPoolClientId: cognitoConfig.userPoolClientId,
          identityPoolId: cognitoConfig.identityPoolId,
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

  async prepareApplicationStart(): Promise<boolean> {
    if (this.authDisabled()) {
      return true;
    }

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
    if (this.authDisabled()) {
      return;
    }

    await signOut({ global: true });
  }

  async isAuthenticated(): Promise<boolean> {
    if (this.authDisabled()) {
      return true;
    }

    try {
      const user = await getCurrentUser();
      const session = await fetchAuthSession();
      return !!user && !!session.tokens?.idToken;
    } catch {
      return false;
    }
  }

  async getUsername(): Promise<string | null> {
    const context = await this.getUserAccessContext();
    return context.username || null;
  }

  async getIdToken(): Promise<string | null> {
    if (this.authDisabled()) {
      return null;
    }

    try {
      const session = await fetchAuthSession();
      return session.tokens?.idToken?.toString() ?? null;
    } catch {
      return null;
    }
  }

  async getAccessToken(): Promise<string | null> {
    if (this.authDisabled()) {
      return null;
    }

    try {
      const session = await fetchAuthSession();
      return session.tokens?.accessToken?.toString() ?? null;
    } catch {
      return null;
    }
  }

  async getUserAccessContext(): Promise<TracksterUserAccessContext> {
    if (this.authDisabled()) {
      return this.getDevUserAccessContext();
    }

    try {
      const user = await getCurrentUser();
      const session = await fetchAuthSession();

      const idToken = session.tokens?.idToken?.toString() ?? null;
      const accessToken = session.tokens?.accessToken?.toString() ?? null;
      const idTokenPayload = session.tokens?.idToken?.payload ?? {};
      const accessTokenPayload = session.tokens?.accessToken?.payload ?? {};

      console.log('Trackster Cognito access token:', accessToken);
      console.log('Trackster Cognito id token:', idToken);
      // console.log( 'ACCESS TOKEN PAYLOAD', session.tokens?.accessToken?.payload );

      const groups = this.getStringArrayClaim(
        idTokenPayload,
        'cognito:groups',
        this.getStringArrayClaim(accessTokenPayload, 'cognito:groups', [])
      );

      const cognitoSub = this.getStringClaim(idTokenPayload, 'sub')
        || this.getStringClaim(accessTokenPayload, 'sub')
        || '';

      const username = this.getStringClaim(idTokenPayload, 'cognito:username')
        || user.username
        || 'User';

      const email = this.getStringClaim(idTokenPayload, 'email');
      const name = this.getStringClaim(idTokenPayload, 'name');

      const globalRole = this.resolveGlobalRole(idTokenPayload, accessTokenPayload, groups);
      const clientRole = this.resolveClientRole(idTokenPayload, accessTokenPayload, groups, globalRole);
      const clientId = this.resolveClientId(idTokenPayload, accessTokenPayload);

      return {
        isAuthenticated: true,
        username,
        cognitoSub,
        email,
        name,
        globalRole,
        clientRole,
        clientId,
        groups,
        idToken,
        accessToken
      };
    } catch {
      return {
        isAuthenticated: false,
        username: 'User',
        cognitoSub: '',
        email: '',
        name: '',
        globalRole: null,
        clientRole: null,
        clientId: '',
        groups: [],
        idToken: null,
        accessToken: null
      };
    }
  }

  private getDevUserAccessContext(): TracksterUserAccessContext {
    if (this.devProfile === 'trackster_admin') {
      return {
        isAuthenticated: true,
        username: 'local-dev',
        cognitoSub: 'local-dev-sub',
        email: 'local-dev@trackster.local',
        name: 'Local Dev',
        globalRole: 'trackster_admin',
        clientRole: 'client_admin',
        clientId: this.devClientId,
        groups: ['trackster_admin', 'client_admin'],
        idToken: null,
        accessToken: null
      };
    }

    if (this.devProfile === 'client_admin') {
      return {
        isAuthenticated: true,
        username: 'local-client-admin',
        cognitoSub: 'local-client-admin-sub',
        email: 'local-client-admin@trackster.local',
        name: 'Local Client Admin',
        globalRole: null,
        clientRole: 'client_admin',
        clientId: this.devClientId,
        groups: ['client_admin'],
        idToken: null,
        accessToken: null
      };
    }

    return {
      isAuthenticated: true,
      username: 'local-client-user',
      cognitoSub: 'local-client-user-sub',
      email: 'local-client-user@trackster.local',
      name: 'Local Client User',
      globalRole: null,
      clientRole: 'client_user',
      clientId: this.devClientId,
      groups: ['client_user'],
      idToken: null,
      accessToken: null
    };
  }

  private async hasAuthenticatedSession(): Promise<boolean> {
    if (this.authDisabled()) {
      return true;
    }

    try {
      const session = await fetchAuthSession();
      return !!session.tokens?.idToken || !!session.tokens?.accessToken;
    } catch {
      return false;
    }
  }

  private async waitForSession(): Promise<boolean> {
    if (this.authDisabled()) {
      return true;
    }

    for (let i = 0; i < 20; i++) {
      if (await this.hasAuthenticatedSession()) {
        return true;
      }

      await new Promise((resolve) => setTimeout(resolve, 300));
    }

    return false;
  }

  private isOAuthReturn(): boolean {
    if (this.authDisabled()) {
      return false;
    }

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

  private resolveGlobalRole(
    idTokenPayload: Record<string, unknown>,
    accessTokenPayload: Record<string, unknown>,
    groups: string[]
  ): TracksterGlobalRole {
    const claimValue = this.getStringClaim(idTokenPayload, 'custom:global_role')
      || this.getStringClaim(accessTokenPayload, 'custom:global_role')
      || this.getStringClaim(idTokenPayload, 'global_role')
      || this.getStringClaim(accessTokenPayload, 'global_role');

    if (claimValue === 'trackster_admin' || groups.includes('trackster_admin')) {
      return 'trackster_admin';
    }

    return null;
  }

  private resolveClientRole(
    idTokenPayload: Record<string, unknown>,
    accessTokenPayload: Record<string, unknown>,
    groups: string[],
    globalRole: TracksterGlobalRole
  ): TracksterClientRole {
    const claimValue = this.getStringClaim(idTokenPayload, 'custom:client_role')
      || this.getStringClaim(accessTokenPayload, 'custom:client_role')
      || this.getStringClaim(idTokenPayload, 'client_role')
      || this.getStringClaim(accessTokenPayload, 'client_role');

    if (claimValue === 'client_admin' || groups.includes('client_admin')) {
      return 'client_admin';
    }

    if (claimValue === 'client_user' || groups.includes('client_user')) {
      return 'client_user';
    }

    if (globalRole === 'trackster_admin') {
      return 'client_admin';
    }

    return null;
  }

  private resolveClientId(
    idTokenPayload: Record<string, unknown>,
    accessTokenPayload: Record<string, unknown>
  ): string {
    return this.getStringClaim(idTokenPayload, 'custom:client_id')
      || this.getStringClaim(accessTokenPayload, 'custom:client_id')
      || this.getStringClaim(idTokenPayload, 'client_id')
      || this.getStringClaim(accessTokenPayload, 'client_id')
      || '';
  }

  private getStringClaim(payload: Record<string, unknown>, claimName: string): string {
    const value = payload[claimName];

    if (typeof value === 'string') {
      return value.trim();
    }

    if (typeof value === 'number' || typeof value === 'boolean') {
      return String(value);
    }

    return '';
  }

  private getStringArrayClaim(
    payload: Record<string, unknown>,
    claimName: string,
    fallback: string[]
  ): string[] {
    const value = payload[claimName];

    if (Array.isArray(value)) {
      return value
        .filter((item): item is string => typeof item === 'string')
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
    }

    if (typeof value === 'string') {
      return value
        .split(',')
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
    }

    return fallback;
  }
}

export function configureAuth(): void {
  const service = new AuthService();
  service.configureAuth();
}