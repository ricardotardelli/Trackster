import { CommonModule } from '@angular/common';
import { HttpClient, HttpClientModule, HttpHeaders } from '@angular/common/http';
import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';
import { firstValueFrom } from 'rxjs';
import { AuthService } from '../auth/auth.service';

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

interface AdminClientUsersResponse {
  success: boolean;
  clientId: string;
  users: MasterAdminUser[];
  error?: string;
}

interface AdminClientUserDeleteResponse {
  success: boolean;
  message?: string;
  error?: string;
  deletedUser?: {
    username?: string;
    email?: string;
    fullName?: string;
    globalRole?: string;
    clientRole?: string;
    clientId?: string;
  };
}

interface AdminClientUserAddResponse {
  success: boolean;
  message?: string;
  error?: string;
  externalLoginCreated?: boolean;
  createdUser?: {
    username?: string;
    email?: string;
    fullName?: string;
    globalRole?: string | null;
    clientRole?: string;
    role?: string;
    clientId?: string;
    status?: string;
  };
}

interface TracksterRuntimeConfig {
  usermanagementApi?: {
    clientUserInfoGet?: string;
    clientUserDelete?: string;
    clientUserAdd?: string;
  };
}

@Component({
  selector: 'app-master-admin',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    HttpClientModule,
    MatDialogModule,
    MatIconModule,
    MatSelectModule
  ],
  templateUrl: './master-admin.component.html',
  styleUrl: './master-admin.component.css'
})
export class MasterAdminComponent implements OnInit {
  @ViewChild('confirmationDialog') confirmationDialog?: TemplateRef<unknown>;
  @ViewChild('messageDialog') messageDialog?: TemplateRef<unknown>;
  @ViewChild('userDialog') userDialog?: TemplateRef<unknown>;

  readonly userRoles: UserRole[] = ['client_admin', 'client_user'];
  readonly userStatuses: ClientStatus[] = ['Active', 'Inactive', 'Suspended'];

  private readonly configPath = 'assets/config.json';
  private readonly devClientUsersMockPath = 'assets/mock/client-users-info-response.json';
  private readonly devClientUserDeleteMockPath = 'assets/mock/client-users.delete-response.json';
  private readonly devClientUserAddMockPath = 'assets/mock/client-users.add-response.json';

  private runtimeConfig: TracksterRuntimeConfig = {};
  private isLoadingUsers = false;

  clients: MasterAdminClientSummary[] = [
    {
      clientId: '00000000',
      name: 'Trackster Demo',
      email: 'kadut3@gmail.com',
      contactName: 'Ricardo Tardelli',
      phone: '+351 000 000 000',
      country: 'Portugal',
      status: 'Active',
      users: 0,
      admins: 0
    },
    {
      clientId: '00000001',
      name: 'Client A',
      email: 'admin-a@example.com',
      contactName: 'Client A Admin',
      phone: '+351 111 111 111',
      country: 'Portugal',
      status: 'Active',
      users: 0,
      admins: 0
    },
    {
      clientId: '00000002',
      name: 'Client B',
      email: 'admin-b@example.com',
      contactName: 'Client B Admin',
      phone: '+351 222 222 222',
      country: 'Portugal',
      status: 'Active',
      users: 0,
      admins: 0
    }
  ];

  users: MasterAdminUser[] = [];

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

  constructor(
    private readonly dialog: MatDialog,
    private readonly http: HttpClient,
    private readonly authService: AuthService
  ) {}

  async ngOnInit(): Promise<void> {
    await this.loadRuntimeConfig();
    await this.loadClientUsers();
  }

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
    return !this.isLoadingUsers
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedClient.status === 'Active';
  }

  get canEditSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isLoadingUsers
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser;
  }

  get canDisableSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isLoadingUsers
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status === 'Active';
  }

  get canActivateSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isLoadingUsers
      && !this.isEditingClient
      && !this.isCreatingClient
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status !== 'Active';
  }

  get canRemoveSelectedUser(): boolean {
    if (!this.selectedUser
      || this.isLoadingUsers
      || this.isEditingClient
      || this.isCreatingClient
      || this.isEditingUser
      || this.isCreatingUser
      || this.selectedUser.status === 'Active') {
      return false;
    }

    if (this.selectedUser.role !== 'client_admin') {
      return true;
    }

    return this.selectedClientUsers.some((user) => user.username !== this.selectedUser?.username
      && user.role === 'client_admin'
      && user.status === 'Active');
  }

  async selectClientById(clientId: string): Promise<void> {
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

    await this.loadClientUsers();
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

    if (!this.canRemoveSelectedUser) {
      this.openMessageDialog(
        'User Cannot Be Removed',
        'This user cannot be removed because the selected client must keep at least one active client administrator.'
      );
      return;
    }

    this.openConfirmationDialog(
      'Remove User',
      `Remove user "${this.selectedUser.username}" from client "${this.selectedClient.name || this.selectedClient.clientId}"? This action will remove the user from the database and Cognito.`,
      'removeUser'
    );
  }

  async confirmDialogAction(): Promise<void> {
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
      await this.confirmSaveUser();
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
      await this.confirmRemoveUser();
    }
  }

  closeDialogs(): void {
    this.dialog.closeAll();
  }

  private async loadRuntimeConfig(): Promise<void> {
    try {
      this.runtimeConfig = await firstValueFrom(
        this.http.get<TracksterRuntimeConfig>(this.configPath)
      );
    } catch (error) {
      console.error('Unable to load Trackster runtime config.', error);
      this.runtimeConfig = {};
    }
  }

  private async loadClientUsers(): Promise<void> {
    this.isLoadingUsers = true;

    try {
      const response = this.isDevelopmentMode()
        ? await this.loadClientUsersFromMock()
        : await this.loadClientUsersFromApi();

      if (!response.success) {
        this.openMessageDialog(
          'Client Users Error',
          response.error || 'Unable to load client users.'
        );
        return;
      }

      this.users = [
        ...this.users.filter((user) => user.clientId !== response.clientId),
        ...response.users.map((user) => ({
          ...user,
          status: this.normalizeStatus(user.status),
          role: this.normalizeUserRole(user.role)
        }))
      ];

      this.refreshClientCounters(response.clientId);
      this.selectedUser = null;
      this.clearEditableUserFields();
    } catch (error) {
      console.error('Unable to load client users.', error);

      this.openMessageDialog(
        'Client Users Error',
        'Unable to load client users.'
      );
    } finally {
      this.isLoadingUsers = false;
    }
  }

  private async loadClientUsersFromMock(): Promise<AdminClientUsersResponse> {
    const response = await firstValueFrom(
      this.http.get<AdminClientUsersResponse>(this.devClientUsersMockPath)
    );

    return {
      ...response,
      clientId: this.selectedClient.clientId,
      users: response.users.map((user) => ({
        ...user,
        clientId: this.selectedClient.clientId
      }))
    };
  }

  private async loadClientUsersFromApi(): Promise<AdminClientUsersResponse> {
    const apiUrl = this.getClientUsersApiUrl();

    if (!apiUrl) {
      return {
        success: false,
        clientId: this.selectedClient.clientId,
        users: [],
        error: 'Client users API URL was not found in assets/config.json.'
      };
    }

    const accessToken = await this.getAccessToken();

    if (!accessToken) {
      return {
        success: false,
        clientId: this.selectedClient.clientId,
        users: [],
        error: 'Access token was not found.'
      };
    }

    const url = `${apiUrl}?clientId=${encodeURIComponent(this.selectedClient.clientId)}`;

    return await firstValueFrom(
      this.http.get<AdminClientUsersResponse>(url, {
        headers: new HttpHeaders({
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        })
      })
    );
  }

  private async createClientUser(user: MasterAdminUser): Promise<AdminClientUserAddResponse> {
    return this.isDevelopmentMode()
      ? await this.createClientUserFromMock(user)
      : await this.createClientUserFromApi(user);
  }

  private async createClientUserFromMock(user: MasterAdminUser): Promise<AdminClientUserAddResponse> {
    const response = await firstValueFrom(
      this.http.get<AdminClientUserAddResponse>(this.devClientUserAddMockPath)
    );

    return {
      ...response,
      success: response.success,
      message: response.message || 'User created successfully.',
      createdUser: {
        username: user.username,
        email: user.email,
        fullName: user.fullName,
        clientRole: user.role,
        clientId: user.clientId,
        status: user.status
      }
    };
  }

  private async createClientUserFromApi(user: MasterAdminUser): Promise<AdminClientUserAddResponse> {
    const apiUrl = this.getClientUserAddApiUrl();

    if (!apiUrl) {
      return {
        success: false,
        message: 'Client user add API URL was not found in assets/config.json.'
      };
    }

    const accessToken = await this.getAccessToken();

    if (!accessToken) {
      return {
        success: false,
        message: 'Access token was not found.'
      };
    }

    return await firstValueFrom(
      this.http.post<AdminClientUserAddResponse>(
        apiUrl,
        {
          username: user.username,
          email: user.email,
          fullName: user.fullName,
          role: user.role,
          clientId: user.clientId
        },
        {
          headers: new HttpHeaders({
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
          })
        }
      )
    );
  }

  private async deleteClientUser(username: string, clientId: string): Promise<AdminClientUserDeleteResponse> {
    return this.isDevelopmentMode()
      ? await this.deleteClientUserFromMock()
      : await this.deleteClientUserFromApi(username, clientId);
  }

  private async deleteClientUserFromMock(): Promise<AdminClientUserDeleteResponse> {
    return await firstValueFrom(
      this.http.get<AdminClientUserDeleteResponse>(this.devClientUserDeleteMockPath)
    );
  }

  private async deleteClientUserFromApi(username: string, clientId: string): Promise<AdminClientUserDeleteResponse> {
    const apiUrl = this.getClientUserDeleteApiUrl();

    if (!apiUrl) {
      return {
        success: false,
        message: 'Client user delete API URL was not found in assets/config.json.'
      };
    }

    const accessToken = await this.getAccessToken();

    if (!accessToken) {
      return {
        success: false,
        message: 'Access token was not found.'
      };
    }

    return await firstValueFrom(
      this.http.post<AdminClientUserDeleteResponse>(
        apiUrl,
        {
          username,
          clientId
        },
        {
          headers: new HttpHeaders({
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
          })
        }
      )
    );
  }

  private getClientUsersApiUrl(): string {
    return (this.runtimeConfig.usermanagementApi?.clientUserInfoGet || '').trim();
  }

  private getClientUserDeleteApiUrl(): string {
    return (this.runtimeConfig.usermanagementApi?.clientUserDelete || '').trim();
  }

  private getClientUserAddApiUrl(): string {
    return (this.runtimeConfig.usermanagementApi?.clientUserAdd || '').trim();
  }

  private isDevelopmentMode(): boolean {
    const authServiceAsAny = this.authService as any;

    if (typeof authServiceAsAny.isDevelopmentMode === 'function') {
      return !!authServiceAsAny.isDevelopmentMode();
    }

    if (typeof authServiceAsAny.isDevMode === 'function') {
      return !!authServiceAsAny.isDevMode();
    }

    return window.location.hostname === 'localhost'
      || window.location.hostname === '127.0.0.1';
  }

  private async getAccessToken(): Promise<string> {
    const authServiceAsAny = this.authService as any;

    if (typeof authServiceAsAny.getAccessToken === 'function') {
      const token = await authServiceAsAny.getAccessToken();
      return String(token || '').trim();
    }

    if (typeof authServiceAsAny.getCurrentAccessToken === 'function') {
      const token = await authServiceAsAny.getCurrentAccessToken();
      return String(token || '').trim();
    }

    if (typeof authServiceAsAny.getToken === 'function') {
      const token = await authServiceAsAny.getToken();
      return String(token || '').trim();
    }

    if (typeof authServiceAsAny.accessToken === 'string') {
      return authServiceAsAny.accessToken.trim();
    }

    return '';
  }

  private confirmSaveClient(): void {
    this.selectedClient.name = this.editableClientName.trim();
    this.selectedClient.email = this.editableClientEmail.trim();
    this.selectedClient.contactName = this.editableClientContactName.trim();
    this.selectedClient.phone = this.editableClientPhone.trim();
    this.selectedClient.country = this.editableClientCountry.trim();

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
  }

  private confirmActivateClient(): void {
    this.selectedClient.status = 'Active';
  }

  private confirmRemoveClient(): void {
    const removedClientId = this.selectedClient.clientId;

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

  private async confirmSaveUser(): Promise<void> {
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
      await this.confirmCreateUserFromApi(
        nextUsername,
        nextFullName,
        nextEmail,
        nextRole,
        nextStatus
      );
      return;
    }

    if (this.selectedUser) {
      this.selectedUser.username = nextUsername;
      this.selectedUser.fullName = nextFullName;
      this.selectedUser.email = nextEmail;
      this.selectedUser.role = nextRole;
      this.selectedUser.status = nextStatus;
      this.selectedUser.clientId = this.selectedClient.clientId;
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

  private async confirmCreateUserFromApi(
    username: string,
    fullName: string,
    email: string,
    role: UserRole,
    status: ClientStatus
  ): Promise<void> {
    const newUserRequest: MasterAdminUser = {
      username,
      fullName,
      email,
      role,
      status,
      clientId: this.selectedClient.clientId
    };

    this.isLoadingUsers = true;

    try {
      const response = await this.createClientUser(newUserRequest);

      if (!response.success) {
        this.openMessageDialog(
          'User Create Error',
          response.message || response.error || 'Unable to create user.'
        );
        return;
      }

      const createdUser = response.createdUser || {};

      const newUser: MasterAdminUser = {
        username: String(createdUser.username || username).trim(),
        fullName: String(createdUser.fullName || fullName).trim(),
        email: String(createdUser.email || email).trim(),
        role: this.normalizeUserRole(String(createdUser.clientRole || createdUser.role || role)),
        status: this.normalizeStatus(String(createdUser.status || status)),
        clientId: String(createdUser.clientId || this.selectedClient.clientId).trim()
      };

      this.users = [
        ...this.users.filter(
          (user) => !(user.username === newUser.username && user.clientId === newUser.clientId)
        ),
        newUser
      ];

      this.selectedUser = newUser;
      this.isEditingUser = false;
      this.isCreatingUser = false;
      this.syncEditableUserFields();
      this.refreshClientCounters(newUser.clientId);

      this.openMessageDialog(
        'User Created',
        response.message || 'User created successfully.'
      );
    } catch (error) {
      console.error('Unable to create client user.', error);

      this.openMessageDialog(
        'User Create Error',
        'Unable to create user.'
      );
    } finally {
      this.isLoadingUsers = false;
    }
  }

  private confirmDisableUser(): void {
    if (!this.selectedUser) {
      return;
    }

    this.selectedUser.status = 'Inactive';
    this.syncEditableUserFields();
    this.refreshSelectedClientCounters();
  }

  private confirmActivateUser(): void {
    if (!this.selectedUser) {
      return;
    }

    this.selectedUser.status = 'Active';
    this.syncEditableUserFields();
    this.refreshSelectedClientCounters();
  }

  private async confirmRemoveUser(): Promise<void> {
    if (!this.selectedUser) {
      return;
    }

    const removedUsername = this.selectedUser.username;
    const removedClientId = this.selectedUser.clientId;

    this.isLoadingUsers = true;

    try {
      const response = await this.deleteClientUser(removedUsername, removedClientId);

      if (!response.success) {
        this.openMessageDialog(
          'User Remove Error',
          response.message || response.error || 'Unable to remove user.'
        );
        return;
      }

      this.users = this.users.filter(
        (user) => !(user.username === removedUsername && user.clientId === removedClientId)
      );

      this.selectedUser = null;
      this.isEditingUser = false;
      this.isCreatingUser = false;
      this.clearEditableUserFields();
      this.refreshSelectedClientCounters();

      this.openMessageDialog(
        'User Removed',
        response.message || 'User was removed successfully.'
      );
    } catch (error) {
      console.error('Unable to remove client user.', error);

      this.openMessageDialog(
        'User Remove Error',
        'Unable to remove user.'
      );
    } finally {
      this.isLoadingUsers = false;
    }
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
    this.refreshClientCounters(this.selectedClient.clientId);
  }

  private refreshClientCounters(clientId: string): void {
    const client = this.clients.find((item) => item.clientId === clientId);

    if (!client) {
      return;
    }

    const clientUsers = this.users.filter(
      (user) => user.clientId === client.clientId
    );

    client.users = clientUsers.length;
    client.admins = clientUsers.filter(
      (user) => user.role === 'client_admin'
    ).length;
  }

  private normalizeStatus(status: string): ClientStatus {
    if (status === 'Active' || status === 'active') {
      return 'Active';
    }

    if (status === 'Suspended' || status === 'suspended') {
      return 'Suspended';
    }

    return 'Inactive';
  }

  private normalizeUserRole(role: string): UserRole {
    if (role === 'client_admin') {
      return 'client_admin';
    }

    return 'client_user';
  }
}