import { CommonModule } from '@angular/common';
import { HttpClient, HttpClientModule, HttpHeaders } from '@angular/common/http';
import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatDialog, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';
import { firstValueFrom } from 'rxjs';
import { AuthService } from '../auth/auth.service';

type ClientStatus = 'Active' | 'Suspended' | 'Inactive';
type UserRole = 'client_admin' | 'client_user';

type AdminUserWorkflowState = 'idle' | 'confirm' | 'running' | 'success' | 'error';
type AdminUserWorkflowAction =
  | 'createUser'
  | 'updateUser'
  | 'disableUser'
  | 'activateUser'
  | 'removeUser';

type ConfirmationAction =
  | 'saveClient'
  | 'disableClient'
  | 'activateClient'
  | 'removeClient'
  | 'saveUser';

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

interface AdminClientUserUpdateResponse {
  success: boolean;
  message?: string;
  error?: string;
  updatedUser?: {
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
    clientUserUpdate?: string;
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
  @ViewChild('processingDialog') processingDialog?: TemplateRef<unknown>;
  @ViewChild('userDialog') userDialog?: TemplateRef<unknown>;
  @ViewChild('adminUserWorkflowDialog') adminUserWorkflowDialog?: TemplateRef<unknown>;

  readonly userRoles: UserRole[] = ['client_admin', 'client_user'];
  readonly userStatuses: ClientStatus[] = ['Active', 'Inactive', 'Suspended'];

  private readonly configPath = 'assets/config.json';
  private readonly devClientUsersMockPath = 'assets/mock/client-users-info-response.json';
  private readonly devClientUserDeleteMockPath = 'assets/mock/client-users.delete-response.json';
  private readonly devClientUserAddMockPath = 'assets/mock/client-users.add-response.json';
  private readonly devClientUserUpdateMockPath = 'assets/mock/client-users.update-response.json';

  private runtimeConfig: TracksterRuntimeConfig = {};
  private isLoadingUsers = false;
  private adminUserWorkflowDialogRef?: MatDialogRef<unknown>;

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

  processingTitle = '';
  processingMessage = '';

  adminUserWorkflowState: AdminUserWorkflowState = 'idle';
  adminUserWorkflowAction: AdminUserWorkflowAction | null = null;
  adminUserWorkflowTitle = '';
  adminUserWorkflowMessage = '';
  adminUserWorkflowDetails = '';

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
    this.editableUsername = '';
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

    if (this.isCreatingUser) {
      this.openCreateUserWorkflowDialog();
      return;
    }

    this.openUpdateUserWorkflowDialog();
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

    this.openDisableUserWorkflowDialog();
  }

  activateUser(): void {
    if (!this.canActivateSelectedUser || !this.selectedUser) {
      return;
    }

    this.openActivateUserWorkflowDialog();
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

    this.openRemoveUserWorkflowDialog();
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
      this.confirmSaveUser();
    }
  }

  closeDialogs(): void {
    this.dialog.closeAll();
  }

  closeAdminUserWorkflowDialog(): void {
    if (this.adminUserWorkflowState === 'running') {
      return;
    }

    const shouldCloseAllDialogs = this.adminUserWorkflowState === 'success';

    this.closeAdminUserWorkflowDialogOnly();
    this.resetAdminUserWorkflowDialog();

    if (shouldCloseAllDialogs) {
      this.dialog.closeAll();
    }
  }

  async confirmAdminUserWorkflowAction(): Promise<void> {
    if (this.adminUserWorkflowState !== 'confirm' || !this.adminUserWorkflowAction) {
      return;
    }

    if (this.adminUserWorkflowAction === 'createUser') {
      await this.confirmCreateUserWithWorkflow();
      return;
    }

    if (this.adminUserWorkflowAction === 'updateUser') {
      await this.confirmUpdateUserWithWorkflow();
      return;
    }

    if (this.adminUserWorkflowAction === 'disableUser') {
      await this.confirmDisableUserWithWorkflow();
      return;
    }

    if (this.adminUserWorkflowAction === 'activateUser') {
      await this.confirmActivateUserWithWorkflow();
      return;
    }

    if (this.adminUserWorkflowAction === 'removeUser') {
      await this.confirmRemoveUserWithWorkflow();
    }
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

  private async updateClientUser(
    user: MasterAdminUser,
    action: AdminUserWorkflowAction = 'updateUser'
  ): Promise<AdminClientUserUpdateResponse> {
    return this.isDevelopmentMode()
      ? await this.updateClientUserFromMock(user)
      : await this.updateClientUserFromApi(user, action);
  }

  private async updateClientUserFromMock(user: MasterAdminUser): Promise<AdminClientUserUpdateResponse> {
    const response = await firstValueFrom(
      this.http.get<AdminClientUserUpdateResponse>(this.devClientUserUpdateMockPath)
    );

    return {
      ...response,
      success: response.success,
      message: response.message || 'User updated successfully.',
      updatedUser: {
        username: user.username,
        email: user.email,
        fullName: user.fullName,
        clientRole: user.role,
        clientId: user.clientId,
        status: user.status
      }
    };
  }

  private async updateClientUserFromApi(
    user: MasterAdminUser,
    action: AdminUserWorkflowAction
  ): Promise<AdminClientUserUpdateResponse> {
    const apiUrl = this.getClientUserUpdateApiUrl();

    if (!apiUrl) {
      return {
        success: false,
        message: 'Client user update API URL was not found in assets/config.json.'
      };
    }

    const accessToken = await this.getAccessToken();

    if (!accessToken) {
      return {
        success: false,
        message: 'Access token was not found.'
      };
    }

    const normalizedApiStatus = user.status === 'Active' ? 'active' : 'inactive';
    const normalizedAction = action === 'activateUser'
      ? 'activate'
      : action === 'disableUser'
        ? 'deactivate'
        : 'update';

    return await firstValueFrom(
      this.http.post<AdminClientUserUpdateResponse>(
        apiUrl,
        {
          username: user.username,
          email: user.email,
          fullName: user.fullName,
          role: user.role,
          clientRole: user.role,
          status: normalizedApiStatus,
          clientId: user.clientId,
          action: normalizedAction,
          enabled: normalizedApiStatus === 'active'
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

  private getClientUserUpdateApiUrl(): string {
    return (this.runtimeConfig.usermanagementApi?.clientUserUpdate || '').trim();
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

  private confirmSaveUser(): void {
    const nextUsername = this.editableUsername.trim();

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
      this.openCreateUserWorkflowDialog();
      return;
    }

    this.openUpdateUserWorkflowDialog();
  }

  private async confirmCreateUserWithWorkflow(): Promise<void> {
    const username = this.editableUsername.trim();
    const fullName = this.editableFullName.trim();
    const email = this.editableUserEmail.trim();
    const role = this.editableUserRole;
    const status = this.editableUserStatus;

    const newUserRequest: MasterAdminUser = {
      username,
      fullName,
      email,
      role,
      status,
      clientId: this.selectedClient.clientId
    };

    await this.executeCreateUserWorkflow(newUserRequest);
  }

  private async confirmUpdateUserWithWorkflow(): Promise<void> {
    if (!this.selectedUser) {
      return;
    }

    const updatedUserRequest: MasterAdminUser = {
      username: this.editableUsername.trim(),
      fullName: this.editableFullName.trim(),
      email: this.editableUserEmail.trim(),
      role: this.editableUserRole,
      status: this.editableUserStatus,
      clientId: this.selectedClient.clientId
    };

    await this.executeUpdateUserWorkflow(
      updatedUserRequest,
      'Updating user...',
      'Please wait while Trackster updates the user account.',
      'The account is being updated in Cognito and the application database.',
      'User updated',
      'User updated successfully.',
      'The user information was updated successfully.',
      'updateUser'
    );
  }

  private async confirmDisableUserWithWorkflow(): Promise<void> {
    if (!this.selectedUser) {
      return;
    }

    const updatedUserRequest: MasterAdminUser = {
      username: this.selectedUser.username,
      fullName: this.selectedUser.fullName,
      email: this.selectedUser.email,
      role: this.selectedUser.role,
      status: 'Inactive',
      clientId: this.selectedUser.clientId
    };

    await this.executeUpdateUserWorkflow(
      updatedUserRequest,
      'Disabling user...',
      `Please wait while Trackster disables user "${updatedUserRequest.username}".`,
      'The account is being disabled in Cognito and the application database.',
      'User disabled',
      'User disabled successfully.',
      'The user was disabled successfully.',
      'disableUser'
    );
  }

  private async confirmActivateUserWithWorkflow(): Promise<void> {
    if (!this.selectedUser) {
      return;
    }

    const updatedUserRequest: MasterAdminUser = {
      username: this.selectedUser.username,
      fullName: this.selectedUser.fullName,
      email: this.selectedUser.email,
      role: this.selectedUser.role,
      status: 'Active',
      clientId: this.selectedUser.clientId
    };

    await this.executeUpdateUserWorkflow(
      updatedUserRequest,
      'Activating user...',
      `Please wait while Trackster activates user "${updatedUserRequest.username}".`,
      'The account is being activated in Cognito and the application database.',
      'User activated',
      'User activated successfully.',
      'The user was activated successfully.',
      'activateUser'
    );
  }

  private async executeCreateUserWorkflow(newUserRequest: MasterAdminUser): Promise<void> {
    this.isLoadingUsers = true;
    this.adminUserWorkflowState = 'running';
    this.adminUserWorkflowTitle = 'Creating user...';
    this.adminUserWorkflowMessage = 'Please wait while Trackster creates the user account.';
    this.adminUserWorkflowDetails = 'The account is being created in Cognito and the application database.';

    try {
      const response = await this.createClientUser(newUserRequest);

      if (!response.success) {
        this.adminUserWorkflowState = 'error';
        this.adminUserWorkflowTitle = 'User creation failed';
        this.adminUserWorkflowMessage = response.message || response.error || 'Unable to create user.';
        this.adminUserWorkflowDetails = 'Please review the user information and try again.';
        return;
      }

      const createdUser = response.createdUser || {};

      const newUser: MasterAdminUser = {
        username: String(createdUser.username || newUserRequest.username).trim(),
        fullName: String(createdUser.fullName || newUserRequest.fullName).trim(),
        email: String(createdUser.email || newUserRequest.email).trim(),
        role: this.normalizeUserRole(String(createdUser.clientRole || createdUser.role || newUserRequest.role)),
        status: this.normalizeStatus(String(createdUser.status || newUserRequest.status)),
        clientId: String(createdUser.clientId || newUserRequest.clientId).trim()
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

      this.adminUserWorkflowState = 'success';
      this.adminUserWorkflowTitle = 'User created';
      this.adminUserWorkflowMessage = response.message || 'User created successfully.';
      this.adminUserWorkflowDetails = 'The user was created in Cognito and associated with the selected client.';
    } catch (error) {
      console.error('Unable to create client user.', error);

      this.adminUserWorkflowState = 'error';
      this.adminUserWorkflowTitle = 'User creation failed';
      this.adminUserWorkflowMessage = this.getHttpErrorMessage(error, 'Unable to create user.');
      this.adminUserWorkflowDetails = 'Please review the user information and try again.';
    } finally {
      this.isLoadingUsers = false;
    }
  }

  private async executeUpdateUserWorkflow(
    updatedUserRequest: MasterAdminUser,
    runningTitle: string,
    runningMessage: string,
    runningDetails: string,
    successTitle: string,
    successMessage: string,
    successDetails: string,
    workflowAction: AdminUserWorkflowAction = 'updateUser'
  ): Promise<void> {
    const previousUsername = this.selectedUser?.username || updatedUserRequest.username;
    const previousClientId = this.selectedUser?.clientId || updatedUserRequest.clientId;

    this.isLoadingUsers = true;
    this.adminUserWorkflowState = 'running';
    this.adminUserWorkflowTitle = runningTitle;
    this.adminUserWorkflowMessage = runningMessage;
    this.adminUserWorkflowDetails = runningDetails;

    try {
      const response = await this.updateClientUser(updatedUserRequest, workflowAction);

      if (!response.success) {
        this.adminUserWorkflowState = 'error';
        this.adminUserWorkflowTitle = 'User update failed';
        this.adminUserWorkflowMessage = response.message || response.error || 'Unable to update user.';
        this.adminUserWorkflowDetails = 'Please review the user information and try again.';
        return;
      }

      const updatedUserResponse = response.updatedUser || {};

      const updatedUser: MasterAdminUser = {
        username: String(updatedUserResponse.username || updatedUserRequest.username).trim(),
        fullName: String(updatedUserResponse.fullName || updatedUserRequest.fullName).trim(),
        email: String(updatedUserResponse.email || updatedUserRequest.email).trim(),
        role: this.normalizeUserRole(String(updatedUserResponse.clientRole || updatedUserResponse.role || updatedUserRequest.role)),
        status: this.normalizeStatus(String(updatedUserResponse.status || updatedUserRequest.status)),
        clientId: String(updatedUserResponse.clientId || updatedUserRequest.clientId).trim()
      };

      this.users = this.users.map((user) => {
        const isCurrentUser = user.username === previousUsername
          && user.clientId === previousClientId;

        return isCurrentUser ? updatedUser : user;
      });

      this.selectedUser = updatedUser;
      this.isEditingUser = false;
      this.isCreatingUser = false;
      this.syncEditableUserFields();
      this.refreshClientCounters(updatedUser.clientId);

      this.adminUserWorkflowState = 'success';
      this.adminUserWorkflowTitle = successTitle;
      this.adminUserWorkflowMessage = response.message || successMessage;
      this.adminUserWorkflowDetails = successDetails;
    } catch (error) {
      console.error('Unable to update client user.', error);

      this.adminUserWorkflowState = 'error';
      this.adminUserWorkflowTitle = 'User update failed';
      this.adminUserWorkflowMessage = this.getHttpErrorMessage(error, 'Unable to update user.');
      this.adminUserWorkflowDetails = 'Please review the user information and try again.';
    } finally {
      this.isLoadingUsers = false;
    }
  }

  private async confirmRemoveUserWithWorkflow(): Promise<void> {
    if (!this.selectedUser) {
      return;
    }

    const removedUsername = this.selectedUser.username;
    const removedClientId = this.selectedUser.clientId;

    this.isLoadingUsers = true;
    this.adminUserWorkflowState = 'running';
    this.adminUserWorkflowTitle = 'Removing user...';
    this.adminUserWorkflowMessage = 'Please wait while Trackster removes the user account.';
    this.adminUserWorkflowDetails = 'The account is being removed from Cognito and the application database.';

    try {
      const response = await this.deleteClientUser(removedUsername, removedClientId);

      if (!response.success) {
        this.adminUserWorkflowState = 'error';
        this.adminUserWorkflowTitle = 'User removal failed';
        this.adminUserWorkflowMessage = response.message || response.error || 'Unable to remove user.';
        this.adminUserWorkflowDetails = 'Please try again or check the browser console for details.';
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

      this.adminUserWorkflowState = 'success';
      this.adminUserWorkflowTitle = 'User removed';
      this.adminUserWorkflowMessage = response.message || 'User was removed successfully.';
      this.adminUserWorkflowDetails = 'The user was removed from Cognito and the application database.';
    } catch (error) {
      console.error('Unable to remove client user.', error);

      this.adminUserWorkflowState = 'error';
      this.adminUserWorkflowTitle = 'User removal failed';
      this.adminUserWorkflowMessage = this.getHttpErrorMessage(error, 'Unable to remove user.');
      this.adminUserWorkflowDetails = 'Please try again or check the browser console for details.';
    } finally {
      this.isLoadingUsers = false;
    }
  }

  private openCreateUserWorkflowDialog(): void {
    this.openAdminUserWorkflowDialog(
      'createUser',
      'Create user?',
      `Create user "${this.editableUsername.trim()}" for client "${this.selectedClient.name || this.selectedClient.clientId}"?`,
      'The user will be created in Cognito and associated with the selected client.'
    );
  }

  private openUpdateUserWorkflowDialog(): void {
    if (!this.selectedUser) {
      return;
    }

    this.openAdminUserWorkflowDialog(
      'updateUser',
      'Update user?',
      `Update user "${this.editableUsername.trim()}" for client "${this.selectedClient.name || this.selectedClient.clientId}"?`,
      'The user information will be updated in Cognito and the application database.'
    );
  }

  private openDisableUserWorkflowDialog(): void {
    if (!this.selectedUser) {
      return;
    }

    this.openAdminUserWorkflowDialog(
      'disableUser',
      'Disable user?',
      `Disable user "${this.selectedUser.username}"?`,
      'The user will be disabled in Cognito and the application database.'
    );
  }

  private openActivateUserWorkflowDialog(): void {
    if (!this.selectedUser) {
      return;
    }

    this.openAdminUserWorkflowDialog(
      'activateUser',
      'Activate user?',
      `Activate user "${this.selectedUser.username}"?`,
      'The user will be activated in Cognito and the application database.'
    );
  }

  private openRemoveUserWorkflowDialog(): void {
    if (!this.selectedUser) {
      return;
    }

    this.openAdminUserWorkflowDialog(
      'removeUser',
      'Remove user?',
      `Remove user "${this.selectedUser.username}" from client "${this.selectedClient.name || this.selectedClient.clientId}"?`,
      'This action will remove the user from Cognito and the application database.'
    );
  }

  private openAdminUserWorkflowDialog(
    action: AdminUserWorkflowAction,
    title: string,
    message: string,
    details: string
  ): void {
    if (!this.adminUserWorkflowDialog) {
      return;
    }

    this.adminUserWorkflowAction = action;
    this.adminUserWorkflowState = 'confirm';
    this.adminUserWorkflowTitle = title;
    this.adminUserWorkflowMessage = message;
    this.adminUserWorkflowDetails = details;

    this.adminUserWorkflowDialogRef = this.dialog.open(this.adminUserWorkflowDialog, {
      width: '440px',
      panelClass: 'trackster-admin-workflow-dialog-panel',
      disableClose: true
    });
  }

  private closeAdminUserWorkflowDialogOnly(): void {
    if (this.adminUserWorkflowDialogRef) {
      this.adminUserWorkflowDialogRef.close();
      this.adminUserWorkflowDialogRef = undefined;
    }
  }

  private resetAdminUserWorkflowDialog(): void {
    this.adminUserWorkflowState = 'idle';
    this.adminUserWorkflowAction = null;
    this.adminUserWorkflowTitle = '';
    this.adminUserWorkflowMessage = '';
    this.adminUserWorkflowDetails = '';
  }

  private getHttpErrorMessage(error: unknown, fallbackMessage: string): string {
    const errorAsAny = error as any;

    return String(
      errorAsAny?.error?.message ||
      errorAsAny?.error?.error ||
      errorAsAny?.message ||
      fallbackMessage
    );
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

  private openProcessingDialog(title: string, message: string): void {
    if (!this.processingDialog) {
      return;
    }

    this.processingTitle = title;
    this.processingMessage = message;

    this.dialog.open(this.processingDialog, {
      width: '420px',
      panelClass: 'trackster-admin-dialog-panel',
      disableClose: true
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