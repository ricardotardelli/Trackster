import { CommonModule } from '@angular/common';
import { Component, TemplateRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';

type ClientStatus = 'Active' | 'Suspended' | 'Inactive';
type UserRole = 'client_admin' | 'client_user';

type ConfirmationAction =
  | 'saveClient'
  | 'disableClient'
  | 'activateClient'
  | 'removeClient'
  | 'saveUser'
  | 'disableUser'
  | 'activateUser'
  | 'removeUser';

interface MasterAdminPlatformSummary {
  clients: number;
  users: number;
  tracksterAdmins: number;
}

interface MasterAdminClientSummary {
  clientId: string;
  name: string;
  email: string;
  contactName: string;
  phone: string;
  country: string;
  status: ClientStatus;
  users: number;
  admins: number;
}

interface MasterAdminUser {
  username: string;
  fullName: string;
  email: string;
  role: UserRole;
  status: ClientStatus;
  clientId: string;
}

@Component({
  selector: 'app-master-admin',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatDialogModule,
    MatIconModule,
    MatSelectModule
  ],
  templateUrl: './master-admin.component.html',
  styleUrl: './master-admin.component.css'
})
export class MasterAdminComponent {
  @ViewChild('confirmationDialog') confirmationDialog?: TemplateRef<unknown>;
  @ViewChild('messageDialog') messageDialog?: TemplateRef<unknown>;
  @ViewChild('userDialog') userDialog?: TemplateRef<unknown>;

  readonly userRoles: UserRole[] = ['client_admin', 'client_user'];
  readonly userStatuses: ClientStatus[] = ['Active', 'Inactive', 'Suspended'];

  clients: MasterAdminClientSummary[] = [
    {
      clientId: '00000000',
      name: 'Trackster Demo',
      email: 'kadut3@gmail.com',
      contactName: 'Ricardo Tardelli',
      phone: '+351 000 000 000',
      country: 'Portugal',
      status: 'Active',
      users: 3,
      admins: 1
    },
    {
      clientId: '00000001',
      name: 'Client A',
      email: 'admin-a@example.com',
      contactName: 'Client A Admin',
      phone: '+351 111 111 111',
      country: 'Portugal',
      status: 'Active',
      users: 2,
      admins: 1
    },
    {
      clientId: '00000002',
      name: 'Client B',
      email: 'admin-b@example.com',
      contactName: 'Client B Admin',
      phone: '+351 222 222 222',
      country: 'Portugal',
      status: 'Active',
      users: 4,
      admins: 2
    }
  ];

  users: MasterAdminUser[] = [
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
  selectedUser: MasterAdminUser | null = null;

  isEditingClient = false;
  isCreatingClient = false;

  isEditingUser = false;
  isCreatingUser = false;

  editableClientName = this.selectedClient.name;
  editableClientEmail = this.selectedClient.email;
  editableClientContactName = this.selectedClient.contactName;
  editableClientPhone = this.selectedClient.phone;
  editableClientCountry = this.selectedClient.country;

  editableUsername = '';
  editableFullName = '';
  editableUserEmail = '';
  editableUserRole: UserRole = 'client_user';
  editableUserStatus: ClientStatus = 'Active';

  confirmationTitle = '';
  confirmationMessage = '';
  confirmationAction: ConfirmationAction | null = null;

  messageTitle = '';
  messageText = '';

  constructor(private readonly dialog: MatDialog) {}

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

  get canAddUser(): boolean {
    return !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedClient.status === 'Active';
  }

  get canEditSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser;
  }

  get canDisableSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status === 'Active';
  }

  get canActivateSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status !== 'Active';
  }

  get canRemoveSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status !== 'Active'
      && this.selectedUser.role !== 'client_admin';
  }

  selectClientById(clientId: string): void {
    if (this.isEditingClient || this.isCreatingClient || this.isEditingUser || this.isCreatingUser) {
      return;
    }

    const client = this.clients.find((item) => item.clientId === clientId);

    if (!client) {
      return;
    }

    this.selectedClient = client;
    this.selectedUser = null;
    this.syncEditableClientFields();
    this.clearEditableUserFields();
  }

  addClient(): void {
    if (this.isEditingClient || this.isCreatingClient || this.isEditingUser || this.isCreatingUser) {
      return;
    }

    const newClient: MasterAdminClientSummary = {
      clientId: this.createNextClientId(),
      name: '',
      email: '',
      contactName: '',
      phone: '',
      country: '',
      status: 'Inactive',
      users: 0,
      admins: 0
    };

    this.clients = [...this.clients, newClient];
    this.selectedClient = newClient;
    this.selectedUser = null;
    this.isCreatingClient = true;
    this.isEditingClient = true;
    this.syncEditableClientFields();
    this.clearEditableUserFields();
  }

  editClient(): void {
    if (this.isEditingClient || this.isEditingUser || this.isCreatingUser) {
      return;
    }

    this.isEditingClient = true;
    this.isCreatingClient = false;
    this.syncEditableClientFields();
  }

  saveClient(): void {
    const validationMessage = this.getClientValidationMessage();

    if (validationMessage) {
      this.openMessageDialog('Validation Required', validationMessage);
      return;
    }

    this.openConfirmationDialog(
      this.isCreatingClient ? 'Create Client' : 'Save Client',
      this.isCreatingClient
        ? 'Confirm creation of this new Trackster client?'
        : `Confirm changes to client "${this.selectedClient.name}"?`,
      'saveClient'
    );
  }

  cancelClientEdit(): void {
    if (this.isCreatingClient) {
      this.clients = this.clients.filter(
        (client) => client.clientId !== this.selectedClient.clientId
      );

      this.selectedClient = this.clients[0];
      this.selectedUser = null;
      this.isCreatingClient = false;
      this.isEditingClient = false;
      this.syncEditableClientFields();
      this.clearEditableUserFields();
      return;
    }

    this.isEditingClient = false;
    this.syncEditableClientFields();
  }

  disableClient(): void {
    if (this.isEditingClient || this.selectedClient.status !== 'Active') {
      return;
    }

    this.openConfirmationDialog(
      'Disable Client',
      `Disable client "${this.selectedClient.name}"?`,
      'disableClient'
    );
  }

  activateClient(): void {
    if (this.isEditingClient || this.selectedClient.status === 'Active') {
      return;
    }

    this.openConfirmationDialog(
      'Activate Client',
      `Activate client "${this.selectedClient.name}"?`,
      'activateClient'
    );
  }

  removeClient(): void {
    if (this.isEditingClient || this.selectedClient.status === 'Active') {
      return;
    }

    if (this.selectedClientUsers.length > 0) {
      this.openMessageDialog(
        'Client Cannot Be Removed',
        'This client cannot be removed because it still has associated users.'
      );
      return;
    }

    this.openConfirmationDialog(
      'Remove Client',
      `Remove client "${this.selectedClient.name}"? This action cannot be undone.`,
      'removeClient'
    );
  }

  selectUser(user: MasterAdminUser): void {
    if (this.isEditingClient || this.isCreatingClient || this.isEditingUser || this.isCreatingUser) {
      return;
    }

    this.selectedUser = user;
    this.syncEditableUserFields();
  }

  openUserForEdit(user: MasterAdminUser): void {
    if (this.isEditingClient || this.isCreatingClient || this.isEditingUser || this.isCreatingUser) {
      return;
    }

    this.selectedUser = user;
    this.editUser();
  }

  isUserSelected(user: MasterAdminUser): boolean {
    return !!this.selectedUser
      && this.selectedUser.username === user.username
      && this.selectedUser.clientId === user.clientId;
  }

  addUser(): void {
    if (!this.canAddUser) {
      return;
    }

    this.selectedUser = null;
    this.isCreatingUser = true;
    this.isEditingUser = true;
    this.editableUsername = this.createNewUsername();
    this.editableFullName = '';
    this.editableUserEmail = '';
    this.editableUserRole = 'client_user';
    this.editableUserStatus = 'Active';

    this.openUserDialog();
  }

  editUser(): void {
    if (!this.canEditSelectedUser || !this.selectedUser) {
      return;
    }

    this.isEditingUser = true;
    this.isCreatingUser = false;
    this.syncEditableUserFields();
    this.openUserDialog();
  }

  saveUser(): void {
    const validationMessage = this.getUserValidationMessage();

    if (validationMessage) {
      this.openMessageDialog('Validation Required', validationMessage);
      return;
    }

    this.openConfirmationDialog(
      this.isCreatingUser ? 'Create User' : 'Save User',
      this.isCreatingUser
        ? `Confirm creation of this user for client "${this.selectedClient.name || this.selectedClient.clientId}"?`
        : `Confirm changes to user "${this.selectedUser?.username}"?`,
      'saveUser'
    );
  }

  cancelUserEdit(): void {
    this.isCreatingUser = false;
    this.isEditingUser = false;

    if (this.selectedUser) {
      this.syncEditableUserFields();
    } else {
      this.clearEditableUserFields();
    }

    this.closeDialogs();
  }

  disableUser(): void {
    if (!this.canDisableSelectedUser || !this.selectedUser) {
      return;
    }

    this.openConfirmationDialog(
      'Disable User',
      `Disable user "${this.selectedUser.username}"?`,
      'disableUser'
    );
  }

  activateUser(): void {
    if (!this.canActivateSelectedUser || !this.selectedUser) {
      return;
    }

    this.openConfirmationDialog(
      'Activate User',
      `Activate user "${this.selectedUser.username}"?`,
      'activateUser'
    );
  }

  removeUser(): void {
    if (!this.selectedUser || this.selectedUser.status === 'Active') {
      return;
    }

    if (this.selectedUser.role === 'client_admin') {
      this.openMessageDialog(
        'User Cannot Be Removed',
        'Client administrators cannot be removed directly. Change the role to client_user first.'
      );
      return;
    }

    this.openConfirmationDialog(
      'Remove User',
      `Remove user "${this.selectedUser.username}" from client "${this.selectedClient.name || this.selectedClient.clientId}"? This action cannot be undone.`,
      'removeUser'
    );
  }

  confirmDialogAction(): void {
    const action = this.confirmationAction;
    this.dialog.closeAll();

    if (!action) {
      return;
    }

    if (action === 'saveClient') {
      this.confirmSaveClient();
      return;
    }

    if (action === 'disableClient') {
      this.confirmDisableClient();
      return;
    }

    if (action === 'activateClient') {
      this.confirmActivateClient();
      return;
    }

    if (action === 'removeClient') {
      this.confirmRemoveClient();
      return;
    }

    if (action === 'saveUser') {
      this.confirmSaveUser();
      return;
    }

    if (action === 'disableUser') {
      this.confirmDisableUser();
      return;
    }

    if (action === 'activateUser') {
      this.confirmActivateUser();
      return;
    }

    if (action === 'removeUser') {
      this.confirmRemoveUser();
    }
  }

  closeDialogs(): void {
    this.dialog.closeAll();
  }

  private confirmSaveClient(): void {
    this.selectedClient.name = this.editableClientName.trim();
    this.selectedClient.email = this.editableClientEmail.trim();
    this.selectedClient.contactName = this.editableClientContactName.trim();
    this.selectedClient.phone = this.editableClientPhone.trim();
    this.selectedClient.country = this.editableClientCountry.trim();

    /*
      TODO: Replace the local update above with the Admin Client Lambda call.

      Example:
      await this.masterAdminService.saveClient(this.selectedClient);
    */

    this.isEditingClient = false;
    this.isCreatingClient = false;
    this.syncEditableClientFields();

    this.openMessageDialog(
      'Client Saved',
      'Client information was saved successfully.'
    );
  }

  private confirmDisableClient(): void {
    this.selectedClient.status = 'Inactive';

    /*
      TODO: Replace the local status update above with the Admin Client Lambda call.

      Example:
      await this.masterAdminService.updateClientStatus(this.selectedClient.clientId, 'Inactive');
    */
  }

  private confirmActivateClient(): void {
    this.selectedClient.status = 'Active';

    /*
      TODO: Replace the local status update above with the Admin Client Lambda call.

      Example:
      await this.masterAdminService.updateClientStatus(this.selectedClient.clientId, 'Active');
    */
  }

  private confirmRemoveClient(): void {
    const removedClientId = this.selectedClient.clientId;

    /*
      TODO: Replace the local removal below with the Admin Client Lambda call.

      Example:
      await this.masterAdminService.removeClient(removedClientId);
    */

    this.clients = this.clients.filter((client) => client.clientId !== removedClientId);
    this.users = this.users.filter((user) => user.clientId !== removedClientId);

    this.selectedClient = this.clients[0];
    this.selectedUser = null;
    this.isEditingClient = false;
    this.isCreatingClient = false;
    this.isEditingUser = false;
    this.isCreatingUser = false;
    this.syncEditableClientFields();
    this.clearEditableUserFields();
    this.refreshSelectedClientCounters();
  }

  private confirmSaveUser(): void {
    const nextUsername = this.editableUsername.trim();
    const nextFullName = this.editableFullName.trim();
    const nextEmail = this.editableUserEmail.trim();
    const nextRole = this.editableUserRole;
    const nextStatus = this.editableUserStatus;

    const duplicateUser = this.users.some((user) => {
      const isSameCurrentUser = !!this.selectedUser
        && user.username === this.selectedUser.username
        && user.clientId === this.selectedUser.clientId;

      return !isSameCurrentUser
        && user.username.toLowerCase() === nextUsername.toLowerCase()
        && user.clientId === this.selectedClient.clientId;
    });

    if (duplicateUser) {
      this.openMessageDialog(
        'Duplicate User',
        'A user with this username already exists for the selected client.'
      );
      return;
    }

    if (this.isCreatingUser) {
      const newUser: MasterAdminUser = {
        username: nextUsername,
        fullName: nextFullName,
        email: nextEmail,
        role: nextRole,
        status: nextStatus,
        clientId: this.selectedClient.clientId
      };

      /*
        TODO: Replace the local create below with the Admin User Lambda call.

        Example:
        await this.masterAdminService.createUser(this.selectedClient.clientId, newUser);
      */

      this.users = [...this.users, newUser];
      this.selectedUser = newUser;
    } else if (this.selectedUser) {
      this.selectedUser.username = nextUsername;
      this.selectedUser.fullName = nextFullName;
      this.selectedUser.email = nextEmail;
      this.selectedUser.role = nextRole;
      this.selectedUser.status = nextStatus;
      this.selectedUser.clientId = this.selectedClient.clientId;

      /*
        TODO: Replace the local update above with the Admin User Lambda call.

        Example:
        await this.masterAdminService.updateUser(this.selectedClient.clientId, this.selectedUser);
      */
    }

    this.isEditingUser = false;
    this.isCreatingUser = false;
    this.syncEditableUserFields();
    this.refreshSelectedClientCounters();

    this.openMessageDialog(
      'User Saved',
      'User information was saved successfully.'
    );
  }

  private confirmDisableUser(): void {
    if (!this.selectedUser) {
      return;
    }

    this.selectedUser.status = 'Inactive';

    /*
      TODO: Replace the local status update above with the Admin User Lambda call.

      Example:
      await this.masterAdminService.updateUserStatus(
        this.selectedClient.clientId,
        this.selectedUser.username,
        'Inactive'
      );
    */

    this.syncEditableUserFields();
    this.refreshSelectedClientCounters();
  }

  private confirmActivateUser(): void {
    if (!this.selectedUser) {
      return;
    }

    this.selectedUser.status = 'Active';

    /*
      TODO: Replace the local status update above with the Admin User Lambda call.

      Example:
      await this.masterAdminService.updateUserStatus(
        this.selectedClient.clientId,
        this.selectedUser.username,
        'Active'
      );
    */

    this.syncEditableUserFields();
    this.refreshSelectedClientCounters();
  }

  private confirmRemoveUser(): void {
    if (!this.selectedUser) {
      return;
    }

    const removedUsername = this.selectedUser.username;
    const removedClientId = this.selectedUser.clientId;

    /*
      TODO: Replace the local removal below with the Admin User Lambda call.

      Example:
      await this.masterAdminService.removeUser(removedClientId, removedUsername);
    */

    this.users = this.users.filter(
      (user) => !(user.username === removedUsername && user.clientId === removedClientId)
    );

    this.selectedUser = null;
    this.isEditingUser = false;
    this.isCreatingUser = false;
    this.clearEditableUserFields();
    this.refreshSelectedClientCounters();
  }

  private openConfirmationDialog(
    title: string,
    message: string,
    action: ConfirmationAction
  ): void {
    if (!this.confirmationDialog) {
      return;
    }

    this.confirmationTitle = title;
    this.confirmationMessage = message;
    this.confirmationAction = action;

    this.dialog.open(this.confirmationDialog, {
      width: '420px',
      panelClass: 'trackster-admin-dialog-panel',
      disableClose: true
    });
  }

  private openMessageDialog(title: string, message: string): void {
    if (!this.messageDialog) {
      return;
    }

    this.messageTitle = title;
    this.messageText = message;

    this.dialog.open(this.messageDialog, {
      width: '420px',
      panelClass: 'trackster-admin-dialog-panel'
    });
  }

  private openUserDialog(): void {
    if (!this.userDialog) {
      return;
    }

    this.dialog.open(this.userDialog, {
      width: '520px',
      panelClass: 'trackster-admin-dialog-panel',
      disableClose: true
    });
  }

  private getClientValidationMessage(): string {
    if (!this.editableClientName.trim()) {
      return 'Company is required.';
    }

    if (!this.editableClientContactName.trim()) {
      return 'Contact name is required.';
    }

    if (!this.editableClientEmail.trim()) {
      return 'Email is required.';
    }

    if (!this.editableClientEmail.includes('@')) {
      return 'Email must contain @.';
    }

    if (!this.editableClientCountry.trim()) {
      return 'Country is required.';
    }

    return '';
  }

  private getUserValidationMessage(): string {
    if (!this.editableUsername.trim()) {
      return 'Username is required.';
    }

    if (!this.editableFullName.trim()) {
      return 'Full name is required.';
    }

    if (!this.editableUserEmail.trim()) {
      return 'Email is required.';
    }

    if (!this.editableUserEmail.includes('@')) {
      return 'Email must contain @.';
    }

    if (!this.editableUserRole) {
      return 'Role is required.';
    }

    if (!this.editableUserStatus) {
      return 'Status is required.';
    }

    return '';
  }

  private syncEditableClientFields(): void {
    this.editableClientName = this.selectedClient.name;
    this.editableClientEmail = this.selectedClient.email;
    this.editableClientContactName = this.selectedClient.contactName;
    this.editableClientPhone = this.selectedClient.phone;
    this.editableClientCountry = this.selectedClient.country;
  }

  private syncEditableUserFields(): void {
    if (!this.selectedUser) {
      this.clearEditableUserFields();
      return;
    }

    this.editableUsername = this.selectedUser.username;
    this.editableFullName = this.selectedUser.fullName;
    this.editableUserEmail = this.selectedUser.email;
    this.editableUserRole = this.selectedUser.role;
    this.editableUserStatus = this.selectedUser.status;
  }

  private clearEditableUserFields(): void {
    this.editableUsername = '';
    this.editableFullName = '';
    this.editableUserEmail = '';
    this.editableUserRole = 'client_user';
    this.editableUserStatus = 'Active';
  }

  private createNextClientId(): string {
    const nextNumber = this.clients
      .map((client) => Number(client.clientId))
      .filter((clientIdNumber) => Number.isFinite(clientIdNumber))
      .reduce((maxValue, currentValue) => Math.max(maxValue, currentValue), -1) + 1;

    return String(nextNumber).padStart(8, '0');
  }

  private createNewUsername(): string {
    const baseUsername = `${this.selectedClient.clientId}.new.user`;
    let candidateUsername = baseUsername;
    let counter = 1;

    while (
      this.users.some(
        (user) => user.username === candidateUsername && user.clientId === this.selectedClient.clientId
      )
    ) {
      candidateUsername = `${baseUsername}.${counter}`;
      counter += 1;
    }

    return candidateUsername;
  }

  private refreshSelectedClientCounters(): void {
    const clientUsers = this.users.filter(
      (user) => user.clientId === this.selectedClient.clientId
    );

    this.selectedClient.users = clientUsers.length;
    this.selectedClient.admins = clientUsers.filter(
      (user) => user.role === 'client_admin'
    ).length;
  }
}