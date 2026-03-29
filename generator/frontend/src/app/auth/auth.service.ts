import { Injectable } from '@angular/core';
import { fetchAuthSession, signInWithRedirect, signOut, getCurrentUser } from 'aws-amplify/auth';

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
      return !!session.tokens?.idToken;
    } catch {
      return false;
    }
  }

  async getUsername(): Promise<string | null> {
    try {
      const user = await getCurrentUser();
      return user.username;
    } catch {
      return null;
    }
  }
}