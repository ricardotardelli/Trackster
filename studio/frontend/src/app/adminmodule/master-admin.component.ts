import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';

interface MasterAdminPlatformSummary {
  clients: number;
  users: number;
  tracksterAdmins: number;
}

interface MasterAdminClientSummary {
  clientId: string;
  name: string;
  email: string;
  status: 'Active' | 'Suspended' | 'Inactive';
  users: number;
  admins: number;
}

interface MasterAdminUser {
  username: string;
  fullName: string;
  email: string;
  role: 'client_admin' | 'client_user';
  status: 'Active' | 'Inactive' | 'Suspended';
  clientId: string;
}

@Component({
  selector: 'app-master-admin',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule,
    MatSelectModule
  ],
  templateUrl: './master-admin.component.html',
  styleUrl: './master-admin.component.css'
})
export class MasterAdminComponent {
  readonly clients: MasterAdminClientSummary[] = [
    {
      clientId: '00000000',
      name: 'Trackster Demo',
      email: 'kadut3@gmail.com',
      status: 'Active',
      users: 3,
      admins: 1
    },
    {
      clientId: '00000001',
      name: 'Client A',
      email: 'admin-a@example.com',
      status: 'Active',
      users: 2,
      admins: 1
    },
    {
      clientId: '00000002',
      name: 'Client B',
      email: 'admin-b@example.com',
      status: 'Active',
      users: 4,
      admins: 2
    }
  ];

  readonly users: MasterAdminUser[] = [
    {
      username: 'kadut',
      fullName: 'Ricardo Tardelli',
      email: 'kadut3@gmail.com',
      role: 'client_admin',
      status: 'Active',
      clientId: '00000000'
    },
    {
      username: 'trackster.demo.user',
      fullName: 'Trackster Demo User',
      email: 'demo.user@trackster.local',
      role: 'client_user',
      status: 'Active',
      clientId: '00000000'
    },
    {
      username: 'trackster.demo.ops',
      fullName: 'Trackster Demo Ops',
      email: 'demo.ops@trackster.local',
      role: 'client_user',
      status: 'Inactive',
      clientId: '00000000'
    },
    {
      username: 'client.a.admin',
      fullName: 'Client A Admin',
      email: 'admin-a@example.com',
      role: 'client_admin',
      status: 'Active',
      clientId: '00000001'
    },
    {
      username: 'client.a.user',
      fullName: 'Client A User',
      email: 'user-a@example.com',
      role: 'client_user',
      status: 'Active',
      clientId: '00000001'
    },
    {
      username: 'client.b.admin',
      fullName: 'Client B Admin',
      email: 'admin-b@example.com',
      role: 'client_admin',
      status: 'Active',
      clientId: '00000002'
    },
    {
      username: 'client.b.ops',
      fullName: 'Client B Ops',
      email: 'ops-b@example.com',
      role: 'client_admin',
      status: 'Active',
      clientId: '00000002'
    },
    {
      username: 'client.b.user.one',
      fullName: 'Client B User One',
      email: 'user-one-b@example.com',
      role: 'client_user',
      status: 'Active',
      clientId: '00000002'
    },
    {
      username: 'client.b.user.two',
      fullName: 'Client B User Two',
      email: 'user-two-b@example.com',
      role: 'client_user',
      status: 'Suspended',
      clientId: '00000002'
    }
  ];

  selectedClient: MasterAdminClientSummary = this.clients[0];

  isEditingClient = false;
  editableClientName = this.selectedClient.name;
  editableClientEmail = this.selectedClient.email;

  get platformSummary(): MasterAdminPlatformSummary {
    return {
      clients: this.clients.length,
      users: this.users.length,
      tracksterAdmins: 1
    };
  }

  get selectedClientUsers(): MasterAdminUser[] {
    return this.users.filter((user) => user.clientId === this.selectedClient.clientId);
  }

  selectClientById(clientId: string): void {
    const client = this.clients.find((item) => item.clientId === clientId);

    if (!client) {
      return;
    }

    this.selectedClient = client;
    this.isEditingClient = false;
    this.syncEditableClientFields();
  }

  addClient(): void {
  }

  editClient(): void {
    this.isEditingClient = true;
    this.syncEditableClientFields();
  }

  saveClient(): void {
    this.selectedClient.name = this.editableClientName.trim();
    this.selectedClient.email = this.editableClientEmail.trim();
    this.isEditingClient = false;
  }

  cancelClientEdit(): void {
    this.isEditingClient = false;
    this.syncEditableClientFields();
  }

  disableClient(): void {
    this.selectedClient.status = 'Inactive';
  }

  activateClient(): void {
    this.selectedClient.status = 'Active';
  }

  removeClient(): void {
    if (this.selectedClient.status === 'Active') {
      return;
    }
  }

  private syncEditableClientFields(): void {
    this.editableClientName = this.selectedClient.name;
    this.editableClientEmail = this.selectedClient.email;
  }
}