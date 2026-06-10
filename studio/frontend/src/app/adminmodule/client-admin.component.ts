import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';

interface ClientUser {
  username: string;
  email: string;
  fullName: string;
  role: 'client_admin' | 'client_user';
  status: 'active' | 'inactive' | 'suspended';
}

@Component({
  selector: 'app-client-admin',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './client-admin.component.html',
  styleUrl: './client-admin.component.css'
})
export class ClientAdminComponent {
  readonly clientId = '00000000';
  readonly companyName = 'Trackster Demo';

  users: ClientUser[] = [
    {
      username: 'kadut',
      email: 'kadut3@gmail.com',
      fullName: 'Ricardo Tardelli',
      role: 'client_admin',
      status: 'active'
    }
  ];

  newUser: ClientUser = {
    username: '',
    email: '',
    fullName: '',
    role: 'client_user',
    status: 'active'
  };

  get activeUserCount(): number {
    return this.users.filter((user) => user.status === 'active').length;
  }

  get adminUserCount(): number {
    return this.users.filter((user) => user.role === 'client_admin').length;
  }

  createUser(): void {
    const username = this.newUser.username.trim();

    if (!username) {
      return;
    }

    const exists = this.users.some((user) => user.username === username);

    if (exists) {
      return;
    }

    this.users = [
      ...this.users,
      {
        username,
        email: this.newUser.email.trim(),
        fullName: this.newUser.fullName.trim(),
        role: this.newUser.role,
        status: 'active'
      }
    ];

    this.newUser = {
      username: '',
      email: '',
      fullName: '',
      role: 'client_user',
      status: 'active'
    };
  }

  resetPassword(user: ClientUser): void {
    window.alert(`Password reset requested for ${user.username}.`);
  }

  disableUser(user: ClientUser): void {
    user.status = user.status === 'active' ? 'inactive' : 'active';
  }
}