import { CommonModule } from '@angular/common';
import { Component, TemplateRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';

type ClientStatus = 'Active' | 'Suspended' | 'Inactive';
type UserRole = 'client_admin' | 'client_user';

type ConfirmationAction =
  | 'saveUser'
  | 'disableUser'
  | 'activateUser'
  | 'removeUser';

interface ClientAdminTenantSummary {
  clientId: string;
  name: string;
  contactName: string;
  country: string;
  status: ClientStatus;
  users: number;
  admins: number;
}

interface ClientAdminUser {
  username: string;
  fullName: string;
  email: string;
  role: UserRole;
  status: ClientStatus;
  clientId: string;
}

@Component({
  selector: 'app-client-admin',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatDialogModule,
    MatIconModule,
    MatSelectModule
  ],
  templateUrl: './client-admin.component.html',
  styleUrl: './client-admin.component.css'
})
export class ClientAdminComponent {
  @ViewChild('confirmationDialog') confirmationDialog?: TemplateRef<unknown>;
  @ViewChild('messageDialog') messageDialog?: TemplateRef<unknown>;
  @ViewChild('userDialog') userDialog?: TemplateRef<unknown>;

  readonly userRoles: UserRole[] = ['client_admin', 'client_user'];
  readonly userStatuses: ClientStatus[] = ['Active', 'Inactive', 'Suspended'];

  currentClient: ClientAdminTenantSummary = {
    clientId: '00000000',
    name: 'Trackster Demo',
    contactName: 'Ricardo Tardelli',
    country: 'Portugal',
    status: 'Active',
    users: 3,
    admins: 1
  };

  users: ClientAdminUser[] = [
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
    }
  ];

  selectedUser: ClientAdminUser | null = null;

  isEditingUser = false;
  isCreatingUser = false;

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

  constructor(private readonly dialog: MatDialog) {
    this.refreshCurrentClientCounters();
  }

  get currentClientUsers(): ClientAdminUser[] {
    return this.users.filter((user) => user.clientId === this.currentClient.clientId);
  }

  get canAddUser(): boolean {
    return !this.isEditingUser
      && !this.isCreatingUser
      && this.currentClient.status === 'Active';
  }

  get canEditSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingUser
      && !this.isCreatingUser;
  }

  get canDisableSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status === 'Active';
  }

  get canActivateSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status !== 'Active';
  }

  get canRemoveSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status !== 'Active'
      && this.selectedUser.role !== 'client_admin';
  }

  selectUser(user: ClientAdminUser): void {
    if (this.isEditingUser || this.isCreatingUser) {
      return;
    }

    this.selectedUser = user;
    this.syncEditableUserFields();
  }

  openUserForEdit(user: ClientAdminUser): void {
    if (this.isEditingUser || this.isCreatingUser) {
      return;
    }

    this.selectedUser = user;
    this.editUser();
  }

  isUserSelected(user: ClientAdminUser): boolean {
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
        ? `Confirm creation of this user for client "${this.currentClient.name || this.currentClient.clientId}"?`
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
      `Remove user "${this.selectedUser.username}" from client "${this.currentClient.name || this.currentClient.clientId}"? This action cannot be undone.`,
      'removeUser'
    );
  }

  confirmDialogAction(): void {
    const action = this.confirmationAction;
    this.dialog.closeAll();

    if (!action) {
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
        && user.clientId === this.currentClient.clientId;
    });

    if (duplicateUser) {
      this.openMessageDialog(
        'Duplicate User',
        'A user with this username already exists for the current client.'
      );
      return;
    }

    if (this.isCreatingUser) {
      const newUser: ClientAdminUser = {
        username: nextUsername,
        fullName: nextFullName,
        email: nextEmail,
        role: nextRole,
        status: nextStatus,
        clientId: this.currentClient.clientId
      };

      /*
        TODO: Replace the local create below with the Client Admin User Lambda call.

        Example:
        await this.clientAdminService.createUser(this.currentClient.clientId, newUser);
      */

      this.users = [...this.users, newUser];
      this.selectedUser = newUser;
    } else if (this.selectedUser) {
      this.selectedUser.username = nextUsername;
      this.selectedUser.fullName = nextFullName;
      this.selectedUser.email = nextEmail;
      this.selectedUser.role = nextRole;
      this.selectedUser.status = nextStatus;
      this.selectedUser.clientId = this.currentClient.clientId;

      /*
        TODO: Replace the local update above with the Client Admin User Lambda call.

        Example:
        await this.clientAdminService.updateUser(this.currentClient.clientId, this.selectedUser);
      */
    }

    this.isEditingUser = false;
    this.isCreatingUser = false;
    this.syncEditableUserFields();
    this.refreshCurrentClientCounters();

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
      TODO: Replace the local status update above with the Client Admin User Lambda call.

      Example:
      await this.clientAdminService.updateUserStatus(
        this.currentClient.clientId,
        this.selectedUser.username,
        'Inactive'
      );
    */

    this.syncEditableUserFields();
    this.refreshCurrentClientCounters();
  }

  private confirmActivateUser(): void {
    if (!this.selectedUser) {
      return;
    }

    this.selectedUser.status = 'Active';

    /*
      TODO: Replace the local status update above with the Client Admin User Lambda call.

      Example:
      await this.clientAdminService.updateUserStatus(
        this.currentClient.clientId,
        this.selectedUser.username,
        'Active'
      );
    */

    this.syncEditableUserFields();
    this.refreshCurrentClientCounters();
  }

  private confirmRemoveUser(): void {
    if (!this.selectedUser) {
      return;
    }

    const removedUsername = this.selectedUser.username;
    const removedClientId = this.selectedUser.clientId;

    /*
      TODO: Replace the local removal below with the Client Admin User Lambda call.

      Example:
      await this.clientAdminService.removeUser(removedClientId, removedUsername);
    */

    this.users = this.users.filter(
      (user) => !(user.username === removedUsername && user.clientId === removedClientId)
    );

    this.selectedUser = null;
    this.isEditingUser = false;
    this.isCreatingUser = false;
    this.clearEditableUserFields();
    this.refreshCurrentClientCounters();
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

  private createNewUsername(): string {
    const baseUsername = `${this.currentClient.clientId}.new.user`;
    let candidateUsername = baseUsername;
    let counter = 1;

    while (
      this.users.some(
        (user) => user.username === candidateUsername && user.clientId === this.currentClient.clientId
      )
    ) {
      candidateUsername = `${baseUsername}.${counter}`;
      counter += 1;
    }

    return candidateUsername;
  }

  private refreshCurrentClientCounters(): void {
    const clientUsers = this.users.filter(
      (user) => user.clientId === this.currentClient.clientId
    );

    this.currentClient.users = clientUsers.length;
    this.currentClient.admins = clientUsers.filter(
      (user) => user.role === 'client_admin'
    ).length;
  }
}