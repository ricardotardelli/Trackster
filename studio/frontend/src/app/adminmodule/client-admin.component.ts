import { CommonModule } from '@angular/common';
import { HttpClient, HttpClientModule, HttpHeaders } from '@angular/common/http';
import { AfterViewInit, Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatDialog, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
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
  roleName: string;
  status: ClientStatus;
  clientId: string;
}

interface ClientUsersResponse {
  success: boolean;
  clientId: string;
  users: ClientAdminUser[];
  error?: string;
  message?: string;
  client?: {
    clientId?: string;
    client_id?: string;
    name?: string;
    companyName?: string;
    company_name?: string;
    contactName?: string;
    contact_name?: string;
    country?: string;
    status?: string;
  };
}

interface ClientUserAddResponse {
  success: boolean;
  message?: string;
  error?: string;
  createdUser?: any;
}

interface ClientUserUpdateResponse {
  success: boolean;
  message?: string;
  error?: string;
  updatedUser?: any;
}

interface ClientUserDeleteResponse {
  success: boolean;
  message?: string;
  error?: string;
  deletedUser?: any;
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
  selector: 'app-client-admin',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    HttpClientModule,
    MatDialogModule,
    MatIconModule,
    MatSelectModule,
    MatSortModule,
    MatTableModule
  ],
  templateUrl: './client-admin.component.html',
  styleUrl: './client-admin.component.css'
})
export class ClientAdminComponent implements OnInit, AfterViewInit {
  @ViewChild('userDialog') userDialog?: TemplateRef<unknown>;
  @ViewChild('adminUserWorkflowDialog') adminUserWorkflowDialog?: TemplateRef<unknown>;
  @ViewChild(MatSort) userSort?: MatSort;

  readonly userRoles: UserRole[] = ['client_admin', 'client_user'];
  readonly userStatuses: ClientStatus[] = ['Active', 'Inactive', 'Suspended'];
  readonly displayedUserColumns: string[] = ['username', 'fullName', 'roleName', 'status', 'email'];

  usersDataSource = new MatTableDataSource<ClientAdminUser>([]);

  private readonly configPath = 'assets/config.json';
  private readonly devClientUsersMockPath = 'assets/mock/client-users-info-response.json';
  private readonly devClientUserAddMockPath = 'assets/mock/client-users.add-response.json';
  private readonly devClientUserUpdateMockPath = 'assets/mock/client-users.update-response.json';
  private readonly devClientUserDeleteMockPath = 'assets/mock/client-users.delete-response.json';

  private runtimeConfig: TracksterRuntimeConfig = {};
  private adminUserWorkflowDialogRef?: MatDialogRef<unknown>;

  currentClient: ClientAdminTenantSummary = {
    clientId: '',
    name: '',
    contactName: '',
    country: '',
    status: 'Inactive',
    users: 0,
    admins: 0
  };

  users: ClientAdminUser[] = [];
  selectedUser: ClientAdminUser | null = null;

  isLoadingUsers = false;
  isEditingUser = false;
  isCreatingUser = false;

  editableUsername = '';
  editableFullName = '';
  editableUserEmail = '';
  editableUserRole: UserRole = 'client_user';
  editableUserStatus: ClientStatus = 'Active';

  adminUserWorkflowState: AdminUserWorkflowState = 'idle';
  adminUserWorkflowAction: AdminUserWorkflowAction | null = null;
  adminUserWorkflowTitle = '';
  adminUserWorkflowMessage = '';
  adminUserWorkflowDetails = '';

  constructor(
    private readonly dialog: MatDialog,
    private readonly http: HttpClient,
    private readonly authService: AuthService
  ) {
    this.usersDataSource.sortingDataAccessor = (user, property) => {
      if (property === 'roleName') {
        return user.roleName || user.role;
      }

      const value = user[property as keyof ClientAdminUser];

      return typeof value === 'string' ? value.toLowerCase() : value;
    };
  }

  async ngOnInit(): Promise<void> {
    await this.loadRuntimeConfig();
    await this.initializeCurrentClient();
    await this.loadClientUsers();
  }

  ngAfterViewInit(): void {
    this.usersDataSource.sort = this.userSort || null;
  }

  get currentClientUsers(): ClientAdminUser[] {
    return this.users.filter((user) => user.clientId === this.currentClient.clientId);
  }

  get canAddUser(): boolean {
    return !this.isLoadingUsers
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.currentClient.status === 'Active';
  }

  get canEditSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isLoadingUsers
      && !this.isEditingUser
      && !this.isCreatingUser;
  }

  get canDisableSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isLoadingUsers
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status === 'Active';
  }

  get canActivateSelectedUser(): boolean {
    return !!this.selectedUser
      && !this.isLoadingUsers
      && !this.isEditingUser
      && !this.isCreatingUser
      && this.selectedUser.status !== 'Active'
      && this.currentClient.status === 'Active';
  }

  get canRemoveSelectedUser(): boolean {
    if (!this.selectedUser
      || this.isLoadingUsers
      || this.isEditingUser
      || this.isCreatingUser
      || this.selectedUser.status === 'Active') {
      return false;
    }

    if (this.selectedUser.role !== 'client_admin') {
      return true;
    }

    return this.currentClientUsers.some((user) => user.username !== this.selectedUser?.username
      && user.role === 'client_admin'
      && user.status === 'Active');
  }

  selectUser(user: ClientAdminUser): void {
    if (this.isEditingUser || this.isCreatingUser || this.isLoadingUsers) {
      return;
    }

    this.selectedUser = user;
    this.syncEditableUserFields();
  }

  openUserForEdit(user: ClientAdminUser): void {
    if (this.isEditingUser || this.isCreatingUser || this.isLoadingUsers) {
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
        'This user cannot be removed because the client must keep at least one active client administrator.'
      );
      return;
    }

    this.openRemoveUserWorkflowDialog();
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

  private updateUsersDataSource(): void {
    this.usersDataSource.data = this.currentClientUsers;

    if (this.userSort) {
      this.usersDataSource.sort = this.userSort;
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

  private async initializeCurrentClient(): Promise<void> {
    const authProfile = await this.getAuthenticatedProfile();
    const clientId = String(authProfile?.clientId || '').trim();

    this.currentClient = {
      clientId,
      name: clientId || 'Client',
      contactName: String(authProfile?.name || '').trim(),
      country: '',
      status: clientId ? 'Active' : 'Inactive',
      users: 0,
      admins: 0
    };

    if (!this.currentClient.clientId && this.isDevelopmentMode()) {
      this.currentClient = {
        clientId: '00000000',
        name: 'Trackster Demo',
        contactName: 'Ricardo Tardelli',
        country: 'Portugal',
        status: 'Active',
        users: 0,
        admins: 0
      };
    }

    if (!this.currentClient.clientId) {
      this.currentClient.status = 'Inactive';
      this.openMessageDialog(
        'Client Error',
        'Client ID was not found in the authenticated user profile.'
      );
    }
  }

  private async loadClientUsers(): Promise<void> {
    if (!this.currentClient.clientId) {
      this.users = [];
      this.selectedUser = null;
      this.refreshCurrentClientCounters();
      return;
    }

    this.isLoadingUsers = true;

    try {
      const response = this.isDevelopmentMode()
        ? await this.loadClientUsersFromMock()
        : await this.loadClientUsersFromApi();

      if (!response.success) {
        this.openMessageDialog(
          'Client Users Error',
          response.message || response.error || 'Unable to load client users.'
        );
        return;
      }

      const responseClientId = String(response.clientId || this.currentClient.clientId).trim();

      this.users = (response.users || []).map((user) => this.mapUserFromResponse(user, responseClientId));

      if (response.client) {
        this.currentClient = this.mapClientFromResponse(response.client, this.currentClient);
      }

      this.selectedUser = null;
      this.clearEditableUserFields();
      this.refreshCurrentClientCounters();
    } catch (error) {
      console.error('Unable to load client users.', error);

      this.openMessageDialog(
        'Client Users Error',
        this.getHttpErrorMessage(error, 'Unable to load client users.')
      );
    } finally {
      this.isLoadingUsers = false;
    }
  }

  private async loadClientUsersFromMock(): Promise<ClientUsersResponse> {
    const response = await firstValueFrom(
      this.http.get<ClientUsersResponse>(this.devClientUsersMockPath)
    );

    return {
      ...response,
      clientId: this.currentClient.clientId,
      users: (response.users || []).map((user) => ({
        ...user,
        clientId: this.currentClient.clientId
      }))
    };
  }

  private async loadClientUsersFromApi(): Promise<ClientUsersResponse> {
    const apiUrl = this.getClientUsersApiUrl();

    if (!apiUrl) {
      return {
        success: false,
        clientId: this.currentClient.clientId,
        users: [],
        message: 'Client users API URL was not found in assets/config.json.'
      };
    }

    const accessToken = await this.getAccessToken();

    if (!accessToken) {
      return {
        success: false,
        clientId: this.currentClient.clientId,
        users: [],
        message: 'Access token was not found.'
      };
    }

    const url = `${apiUrl}?clientId=${encodeURIComponent(this.currentClient.clientId)}`;

    return await firstValueFrom(
      this.http.get<ClientUsersResponse>(url, {
        headers: new HttpHeaders({
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        })
      })
    );
  }

  private async confirmCreateUserWithWorkflow(): Promise<void> {
    const newUserRequest: ClientAdminUser = {
      username: this.editableUsername.trim(),
      fullName: this.editableFullName.trim(),
      email: this.editableUserEmail.trim(),
      role: this.editableUserRole,
      roleName: this.getKnownRoleName(this.editableUserRole),
      status: this.editableUserStatus,
      clientId: this.currentClient.clientId
    };

    this.closeUserDialogOnly();
    this.ensureAdminWorkflowDialogOpen();

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

      const createdUser = this.mapUserFromResponse(
        {
          ...newUserRequest,
          ...(response.createdUser || {})
        },
        newUserRequest.clientId
      );

      this.users = [
        ...this.users.filter(
          (user) => !(user.username === createdUser.username && user.clientId === createdUser.clientId)
        ),
        createdUser
      ];

      this.selectedUser = createdUser;
      this.isEditingUser = false;
      this.isCreatingUser = false;
      this.syncEditableUserFields();
      this.refreshCurrentClientCounters();

      this.adminUserWorkflowState = 'success';
      this.adminUserWorkflowTitle = 'User created';
      this.adminUserWorkflowMessage = response.message || 'User created successfully.';
      this.adminUserWorkflowDetails = 'The user was created in Cognito and associated with the current client.';
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

  private async confirmUpdateUserWithWorkflow(): Promise<void> {
    if (!this.selectedUser) {
      return;
    }

    const updatedUserRequest: ClientAdminUser = {
      username: this.editableUsername.trim(),
      fullName: this.editableFullName.trim(),
      email: this.editableUserEmail.trim(),
      role: this.editableUserRole,
      roleName: this.getKnownRoleName(this.editableUserRole),
      status: this.editableUserStatus,
      clientId: this.currentClient.clientId
    };

    this.closeUserDialogOnly();

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

    const updatedUserRequest: ClientAdminUser = {
      ...this.selectedUser,
      status: 'Inactive'
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

    const updatedUserRequest: ClientAdminUser = {
      ...this.selectedUser,
      status: 'Active'
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

  private async executeUpdateUserWorkflow(
    updatedUserRequest: ClientAdminUser,
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

    this.ensureAdminWorkflowDialogOpen();

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

      const updatedUser = this.mapUserFromResponse(
        {
          ...updatedUserRequest,
          ...(response.updatedUser || {})
        },
        updatedUserRequest.clientId
      );

      this.users = this.users.map((user) => {
        const isCurrentUser = user.username === previousUsername
          && user.clientId === previousClientId;

        return isCurrentUser ? updatedUser : user;
      });

      this.selectedUser = updatedUser;
      this.isEditingUser = false;
      this.isCreatingUser = false;
      this.syncEditableUserFields();
      this.refreshCurrentClientCounters();

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

    this.ensureAdminWorkflowDialogOpen();

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
      this.refreshCurrentClientCounters();

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
      `Create user "${this.editableUsername.trim()}" for client "${this.currentClient.name || this.currentClient.clientId}"?`,
      'The user will be created in Cognito and associated with the current client.'
    );
  }

  private openUpdateUserWorkflowDialog(): void {
    if (!this.selectedUser) {
      return;
    }

    this.openAdminUserWorkflowDialog(
      'updateUser',
      'Update user?',
      `Update user "${this.editableUsername.trim()}" for client "${this.currentClient.name || this.currentClient.clientId}"?`,
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
      `Remove user "${this.selectedUser.username}" from client "${this.currentClient.name || this.currentClient.clientId}"?`,
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

    this.ensureAdminWorkflowDialogOpen();
  }

  private ensureAdminWorkflowDialogOpen(): void {
    if (!this.adminUserWorkflowDialog || this.adminUserWorkflowDialogRef) {
      return;
    }

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

  private closeUserDialogOnly(): void {
    this.dialog.openDialogs
      .filter((dialogRef) => dialogRef.componentInstance === null)
      .forEach((dialogRef) => {
        if (dialogRef !== this.adminUserWorkflowDialogRef) {
          dialogRef.close();
        }
      });
  }

  private openMessageDialog(title: string, message: string): void {
    this.adminUserWorkflowAction = null;
    this.adminUserWorkflowState = 'error';
    this.adminUserWorkflowTitle = title;
    this.adminUserWorkflowMessage = message;
    this.adminUserWorkflowDetails = '';

    this.ensureAdminWorkflowDialogOpen();
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

  private async createClientUser(user: ClientAdminUser): Promise<ClientUserAddResponse> {
    return this.isDevelopmentMode()
      ? await this.createClientUserFromMock(user)
      : await this.createClientUserFromApi(user);
  }

  private async createClientUserFromMock(user: ClientAdminUser): Promise<ClientUserAddResponse> {
    const response = await firstValueFrom(
      this.http.get<ClientUserAddResponse>(this.devClientUserAddMockPath)
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
        roleName: user.roleName,
        clientId: user.clientId,
        status: user.status
      }
    };
  }

  private async createClientUserFromApi(user: ClientAdminUser): Promise<ClientUserAddResponse> {
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
      this.http.post<ClientUserAddResponse>(
        apiUrl,
        {
          username: user.username,
          email: user.email,
          fullName: user.fullName,
          role: user.role,
          roleName: user.roleName,
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
    user: ClientAdminUser,
    action: AdminUserWorkflowAction = 'updateUser'
  ): Promise<ClientUserUpdateResponse> {
    return this.isDevelopmentMode()
      ? await this.updateClientUserFromMock(user)
      : await this.updateClientUserFromApi(user, action);
  }

  private async updateClientUserFromMock(user: ClientAdminUser): Promise<ClientUserUpdateResponse> {
    const response = await firstValueFrom(
      this.http.get<ClientUserUpdateResponse>(this.devClientUserUpdateMockPath)
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
        roleName: user.roleName,
        clientId: user.clientId,
        status: user.status
      }
    };
  }

  private async updateClientUserFromApi(
    user: ClientAdminUser,
    action: AdminUserWorkflowAction
  ): Promise<ClientUserUpdateResponse> {
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

    const normalizedApiStatus = this.toApiStatus(user.status);
    const normalizedAction = action === 'activateUser'
      ? 'activate'
      : action === 'disableUser'
        ? 'deactivate'
        : 'update';

    return await firstValueFrom(
      this.http.post<ClientUserUpdateResponse>(
        apiUrl,
        {
          username: user.username,
          email: user.email,
          fullName: user.fullName,
          role: user.role,
          clientRole: user.role,
          roleName: user.roleName,
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

  private async deleteClientUser(username: string, clientId: string): Promise<ClientUserDeleteResponse> {
    return this.isDevelopmentMode()
      ? await this.deleteClientUserFromMock()
      : await this.deleteClientUserFromApi(username, clientId);
  }

  private async deleteClientUserFromMock(): Promise<ClientUserDeleteResponse> {
    return await firstValueFrom(
      this.http.get<ClientUserDeleteResponse>(this.devClientUserDeleteMockPath)
    );
  }

  private async deleteClientUserFromApi(username: string, clientId: string): Promise<ClientUserDeleteResponse> {
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
      this.http.post<ClientUserDeleteResponse>(
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

  private async getAuthenticatedProfile(): Promise<any> {
    const authServiceAsAny = this.authService as any;

    if (typeof authServiceAsAny.getUserAccessContext === 'function') {
      return await authServiceAsAny.getUserAccessContext();
    }

    return {};
  }

  private async getAccessToken(): Promise<string> {
    const authServiceAsAny = this.authService as any;

    if (typeof authServiceAsAny.getAccessToken === 'function') {
      const token = await authServiceAsAny.getAccessToken();
      return String(token || '').trim();
    }

    return '';
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

  private refreshCurrentClientCounters(): void {
    const clientUsers = this.users.filter(
      (user) => user.clientId === this.currentClient.clientId
    );

    this.currentClient.users = clientUsers.length;
    this.currentClient.admins = clientUsers.filter(
      (user) => user.role === 'client_admin'
    ).length;

    this.updateUsersDataSource();
  }

  private normalizeStatus(status: string): ClientStatus {
    const normalizedStatus = String(status || '').trim().toLowerCase();

    if (normalizedStatus === 'active') {
      return 'Active';
    }

    if (normalizedStatus === 'suspended') {
      return 'Suspended';
    }

    return 'Inactive';
  }

  private toApiStatus(status: ClientStatus): string {
    if (status === 'Active') {
      return 'active';
    }

    if (status === 'Suspended') {
      return 'suspended';
    }

    return 'inactive';
  }

  private normalizeUserRole(role: string): UserRole {
    return role === 'client_admin' ? 'client_admin' : 'client_user';
  }

  private mapUserFromResponse(user: any, fallbackClientId: string): ClientAdminUser {
    const role = this.normalizeUserRole(String(
      user?.roleCode ||
      user?.role_code ||
      user?.clientRole ||
      user?.role ||
      ''
    ).trim());

    return {
      username: String(user?.username || '').trim(),
      fullName: String(user?.fullName || user?.full_name || '').trim(),
      email: String(user?.email || '').trim(),
      role,
      roleName: String(
        user?.roleName ||
        user?.role_name ||
        user?.clientRoleName ||
        user?.client_role_name ||
        this.getKnownRoleName(role)
      ).trim(),
      status: this.normalizeStatus(String(user?.status || 'Inactive')),
      clientId: String(user?.clientId || user?.client_id || fallbackClientId || '').trim()
    };
  }

  private mapClientFromResponse(client: any, fallbackClient: ClientAdminTenantSummary): ClientAdminTenantSummary {
    return {
      clientId: String(client?.clientId || client?.client_id || fallbackClient.clientId || '').trim(),
      name: String(client?.name || client?.companyName || client?.company_name || fallbackClient.name || '').trim(),
      contactName: String(client?.contactName || client?.contact_name || fallbackClient.contactName || '').trim(),
      country: String(client?.country || fallbackClient.country || '').trim(),
      status: this.normalizeStatus(String(client?.status || fallbackClient.status || 'Inactive')),
      users: fallbackClient.users,
      admins: fallbackClient.admins
    };
  }

  getKnownRoleName(role: UserRole): string {
    if (role === 'client_admin') {
      return 'Client Administrator';
    }

    return 'Client User';
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
}