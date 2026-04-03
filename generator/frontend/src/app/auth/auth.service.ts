import { Injectable } from '@angular/core';
import { fetchAuthSession, signInWithRedirect, signOut } from 'aws-amplify/auth';

@Injectable({ providedIn: 'root' })
export class AuthService {
  async login(): Promise<void> {
    await signInWithRedirect();
  }

  async logout(): Promise<void> {
    await signOut();
  }

  async isAuthenticated(): Promise<boolean> {
    try {
      const session = await fetchAuthSession();
      return !!session.tokens?.idToken || !!session.tokens?.accessToken;
    } catch {
      return false;
    }
  }

  async getUsername(): Promise<string | null> {
    try {
      const session = await fetchAuthSession();
      const idToken = session.tokens?.idToken;

      if (!idToken) {
        return null;
      }

      const username = idToken.payload['cognito:username'];
      return typeof username === 'string' ? username : null;
    } catch {
      return null;
    }
  }
}