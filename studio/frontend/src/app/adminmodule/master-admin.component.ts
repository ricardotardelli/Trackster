import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';

interface TracksterClient {
  clientId: string;
  companyName: string;
  companyEmail: string;
  status: 'active' | 'inactive' | 'suspended';
  createdAt: string;
}

interface TracksterUser {
  username: string;
  email: string;
  fullName: string;
  globalRole: 'trackster_admin' | null;
  clientRole: 'client_admin' | 'client_user';
  clientId: string;
  status: 'active' | 'inactive' | 'suspended';
}

@Component({
  selector: 'app-master-admin',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './master-admin.component.html',
  styleUrl: './master-admin.component.css'
})
export class MasterAdminComponent {
  clients: TracksterClient[] = [
    {
      clientId: '00000000',
      companyName: 'Trackster Demo',
      companyEmail: 'kadut3@gmail.com',
      status: 'active',
      createdAt: '2026-06-10'
    }
  ];

  users: TracksterUser[] = [
    {
      username: 'kadut',
      email: 'kadut3@gmail.com',
      fullName: 'Ricardo Tardelli',
      globalRole: 'trackster_admin',
      clientRole: 'client_admin',
      clientId: '00000000',
      status: 'active'
    }
  ];

  newClient: TracksterClient = {
    clientId: '',
    companyName: '',
    companyEmail: '',
    status: 'active',
    createdAt: ''
  };

  newUser: TracksterUser = {
    username: '',
    email: '',
    fullName: '',
    globalRole: null,
    clientRole: 'client_admin',
    clientId: '',
    status: 'active'
  };

  selectedClientId = '00000000';

  get activeClients(): TracksterClient[] {
    return this.clients.filter((client) => client.status === 'active');
  }

  get filteredUsers(): TracksterUser[] {
    if (!this.selectedClientId) {
      return this.users;
    }

    return this.users.filter((user) => user.clientId === this.selectedClientId);
  }

  createClient(): void {
    const clientId = this.newClient.clientId.trim();
    const companyName = this.newClient.companyName.trim();

    if (!clientId || !companyName) {
      return;
    }

    const exists = this.clients.some((client) => client.clientId === clientId);

    if (exists) {
      return;
    }

    this.clients = [
      ...this.clients,
      {
        clientId,
        companyName,
        companyEmail: this.newClient.companyEmail.trim(),
        status: 'active',
        createdAt: new Date().toISOString().slice(0, 10)
      }
    ];

    this.selectedClientId = clientId;

    this.newClient = {
      clientId: '',
      companyName: '',
      companyEmail: '',
      status: 'active',
      createdAt: ''
    };
  }

  createUser(): void {
    const username = this.newUser.username.trim();
    const email = this.newUser.email.trim();
    const fullName = this.newUser.fullName.trim();
    const clientId = this.newUser.clientId.trim() || this.selectedClientId;

    if (!username || !clientId) {
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
        email,
        fullName,
        globalRole: null,
        clientRole: this.newUser.clientRole,
        clientId,
        status: 'active'
      }
    ];

    this.newUser = {
      username: '',
      email: '',
      fullName: '',
      globalRole: null,
      clientRole: 'client_admin',
      clientId: '',
      status: 'active'
    };
  }

  suspendClient(client: TracksterClient): void {
    client.status = client.status === 'active' ? 'suspended' : 'active';
  }

  disableUser(user: TracksterUser): void {
    user.status = user.status === 'active' ? 'inactive' : 'active';
  }

  resetPassword(user: TracksterUser): void {
    window.alert(`Password reset requested for ${user.username}.`);
  }
}