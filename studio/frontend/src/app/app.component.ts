import { SimulatorComponent } from './simulator/simulator.component';
import { DbcworkspaceComponent } from './dbcworkspace/dbcworkspace.component';
import { DecoderComponent } from './decoder/decoder.component';
import { SignalPlotterComponent } from './signal-plotter/signal-plotter.component';
import { ClientAdminComponent } from './adminmodule/client-admin.component';
import { MasterAdminComponent } from './adminmodule/master-admin.component';
import { MatTabsModule } from '@angular/material/tabs';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { CommonModule } from '@angular/common';
import { Component, ElementRef, HostListener, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterOutlet } from '@angular/router';
import { AuthService, TracksterClientRole, TracksterGlobalRole, TracksterUserAccessContext } from './auth/auth.service';

type WorkspaceModule =
  | 'generator'
  | 'dbc-manager'
  | 'decoder'
  | 'signal-plotter'
  | 'administration';

type ProfileDialogMode = 'view' | 'edit' | 'password';

interface WorkspaceTab {
  id: WorkspaceModule;
  label: string;
  shortLabel: string;
  description: string;
}

interface TracksterRuntimeConfig {
  usermanagementApi?: {
    changePassword?: string;
    userInfoUpdate?: string;
    userInfoGet?: string;
  };
}

interface ChangePasswordResponse {
  success?: boolean;
  message?: string;
  error?: string;
}

interface UserInfoUpdateResponse {
  success?: boolean;
  message?: string;
  error?: string;
  user?: {
    id?: string;
    username?: string;
    email?: string;
    fullName?: string;
    status?: string;
    createdAt?: string;
    updatedAt?: string;
  };
}

interface UserInfoGetResponse {
  success?: boolean;
  message?: string;
  error?: string;
  user?: {
    id?: string;
    username?: string;
    email?: string;
    fullName?: string;
    status?: string;
    globalRole?: TracksterGlobalRole;
    clientRole?: TracksterClientRole;
    clientId?: string;
    companyName?: string;
    companyEmail?: string;
    contactName?: string;
    country?: string;
    phone?: string;
    createdAt?: string;
    updatedAt?: string;
    clientAssociations?: Array<{
      clientId?: string;
      clientRole?: TracksterClientRole;
      status?: string;
      companyName?: string;
      companyEmail?: string;
      contactName?: string;
      country?: string;
      phone?: string;
      clientStatus?: string;
    }>;
  };
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    RouterOutlet,
    MatTabsModule,
    MatDialogModule,
    MatIconModule,
    SimulatorComponent,
    DbcworkspaceComponent,
    DecoderComponent,
    SignalPlotterComponent,
    ClientAdminComponent,
    MasterAdminComponent
  ],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnInit {
  constructor(
    private readonly elementRef: ElementRef<HTMLElement>,
    private readonly authService: AuthService,
    private readonly dialog: MatDialog
  ) {}

  authReady = false;
  isAuthenticated = false;

  username = 'User';
  userMenuOpen = false;
  isLoggingOut = false;

  selectedTabIndex = 0;
  activeModule: WorkspaceModule = 'generator';

  globalRole: TracksterGlobalRole = null;
  clientRole: TracksterClientRole = null;
  clientId = '';

  cognitoSub = '';
  email = '';
  name = '';
  groups: string[] = [];
  idToken: string | null = null;
  accessToken: string | null = null;

  userAccessContext: TracksterUserAccessContext | null = null;

  profileDialogMode: ProfileDialogMode = 'view';

  editableProfileName = '';
  editableProfileEmail = '';

  currentPassword = '';
  newPassword = '';
  confirmNewPassword = '';

  isSavingProfile = false;
  isSavingPassword = false;

  passwordErrorMessage = '';
  passwordSuccessMessage = '';

  messageTitle = '';
  messageText = '';

  private runtimeConfig: TracksterRuntimeConfig | null = null;

  @ViewChild('userMenuContainer', { static: false })
  private userMenuContainer?: ElementRef<HTMLElement>;

  @ViewChild('profileDialog')
  private profileDialog?: TemplateRef<unknown>;

  @ViewChild('messageDialog')
  private messageDialog?: TemplateRef<unknown>;

  ngOnInit(): void {
    void this.initializeApp();
  }

  get isTracksterAdmin(): boolean {
    return this.globalRole === 'trackster_admin';
  }

  get isClientAdmin(): boolean {
    return this.clientRole === 'client_admin';
  }

  get canSeeAdministrationTab(): boolean {
    return this.isTracksterAdmin || this.isClientAdmin;
  }

  get administrationTabLabel(): string {
    if (this.isTracksterAdmin) {
      return 'Trackster Administration';
    }

    if (this.isClientAdmin) {
      return 'User Management';
    }

    return '';
  }

  get activeWorkspaceTab(): WorkspaceTab {
    return this.workspaceTabs.find((tab) => tab.id === this.activeModule) ?? this.workspaceTabs[0];
  }

  get activeWorkspaceTitle(): string {
    return this.activeWorkspaceTab.label;
  }

  get activeWorkspaceDescription(): string {
    return this.activeWorkspaceTab.description;
  }

  readonly workspaceTabs: readonly WorkspaceTab[] = [
    {
      id: 'generator',
      label: 'Simulation Studio',
      shortLabel: 'Studio',
      description: 'Build and generate CAN simulation packages.'
    },
    {
      id: 'dbc-manager',
      label: 'DBC Workspace',
      shortLabel: 'DBC',
      description: 'Organize, validate, and prepare DBC assets.'
    },
    {
      id: 'decoder',
      label: 'Decoder / Exporter',
      shortLabel: 'Decoder',
      description: 'Inspect frames, decode payloads, and export simulation data.'
    },
    {
      id: 'signal-plotter',
      label: 'Signal Plotter',
      shortLabel: 'Plotter',
      description: 'Visualize decoded signals and compare telemetry curves.'
    },
    {
      id: 'administration',
      label: 'Administration',
      shortLabel: 'Admin',
      description: 'Manage Trackster clients and users.'
    }
  ];

  onTabChange(index: number): void {
    this.selectedTabIndex = index;

    if (index === 0) {
      this.activeModule = 'generator';
    } else if (index === 1) {
      this.activeModule = 'dbc-manager';
    } else if (index === 2) {
      this.activeModule = 'decoder';
    } else if (index === 3) {
      this.activeModule = 'signal-plotter';
    } else if (index === 4 && this.canSeeAdministrationTab) {
      this.activeModule = 'administration';
    }

    this.closeUserMenu();
  }

  toggleUserMenu(event?: MouseEvent): void {
    event?.stopPropagation();
    this.userMenuOpen = !this.userMenuOpen;
  }

  closeUserMenu(): void {
    this.userMenuOpen = false;
  }

  openProfileDialog(event: Event): void {
    event.preventDefault();
    this.closeUserMenu();
    this.profileDialogMode = 'view';
    this.syncEditableProfileFields();
    this.clearPasswordFields();
    this.clearPasswordFeedback();

    if (!this.profileDialog) {
      return;
    }

    this.dialog.open(this.profileDialog, {
      width: '520px',
      panelClass: 'trackster-profile-dialog-panel'
    });
  }

  startProfileEdit(): void {
    if (this.isSavingProfile || this.isSavingPassword) {
      return;
    }

    this.syncEditableProfileFields();
    this.clearPasswordFeedback();
    this.profileDialogMode = 'edit';
  }

  cancelProfileEdit(): void {
    if (this.isSavingProfile) {
      return;
    }

    this.syncEditableProfileFields();
    this.profileDialogMode = 'view';
  }

  async saveProfileInformation(): Promise<void> {
    if (this.isSavingProfile) {
      return;
    }

    const validationMessage = this.getProfileValidationMessage();

    if (validationMessage) {
      this.openMessageDialog('Validation Required', validationMessage);
      return;
    }

    this.isSavingProfile = true;

    try {
      const nextName = this.editableProfileName.trim();
      const nextEmail = this.editableProfileEmail.trim();

      const response = await this.updateUserInfo(nextName, nextEmail);

      this.name = response.user?.fullName || nextName;
      this.email = response.user?.email || nextEmail;

      if (response.user?.username) {
        this.username = response.user.username;
      }

      this.syncEditableProfileFields();
      this.profileDialogMode = 'view';

      this.openMessageDialog(
        'Profile Saved',
        response.message || 'Your profile information was saved successfully.'
      );
    } catch (error) {
      this.openMessageDialog(
        'Profile Save Failed',
        this.getUserInfoUpdateErrorMessage(error)
      );
    } finally {
      this.isSavingProfile = false;
    }
  }

  startPasswordChange(): void {
    if (this.isSavingProfile || this.isSavingPassword) {
      return;
    }

    this.clearPasswordFields();
    this.clearPasswordFeedback();
    this.profileDialogMode = 'password';
  }

  cancelPasswordChange(): void {
    if (this.isSavingPassword) {
      return;
    }

    this.clearPasswordFields();
    this.clearPasswordFeedback();
    this.profileDialogMode = 'view';
  }

  async savePasswordChange(): Promise<void> {
    if (this.isSavingPassword) {
      return;
    }

    this.clearPasswordFeedback();

    const validationMessage = this.getPasswordValidationMessage();

    if (validationMessage) {
      this.passwordErrorMessage = validationMessage;
      return;
    }

    this.isSavingPassword = true;

    try {
      const currentPassword = this.currentPassword;
      const newPassword = this.newPassword;

      const response = await this.changePassword(currentPassword, newPassword);

      this.clearPasswordFields();
      this.passwordSuccessMessage = response.message || 'Your password was changed successfully.';
    } catch (error) {
      this.passwordErrorMessage = this.getChangePasswordErrorMessage(error);
      this.passwordSuccessMessage = '';
    } finally {
      this.isSavingPassword = false;
    }
  }

  closeDialogs(): void {
    if (this.isSavingProfile || this.isSavingPassword) {
      return;
    }

    this.dialog.closeAll();
  }

  async onLogout(): Promise<void> {
    if (this.isLoggingOut) {
      return;
    }

    this.isLoggingOut = true;
    this.userMenuOpen = false;

    try {
      await this.authService.logout();
    } finally {
      this.isLoggingOut = false;
    }
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as Node | null;

    if (!target) {
      return;
    }

    if (this.userMenuOpen && this.userMenuContainer?.nativeElement.contains(target)) {
      return;
    }

    this.userMenuOpen = false;
  }

  private async getUserInfo(): Promise<UserInfoGetResponse> {
    if (this.isDevelopmentMode()) {
      return await this.loadDevelopmentUserInfoGetResponse();
    }

    const config = await this.getRuntimeConfig();
    const userInfoGetUrl = config.usermanagementApi?.userInfoGet;

    if (!userInfoGetUrl) {
      throw new Error('User information API URL is not configured.');
    }

    if (!this.accessToken) {
      throw new Error('Authenticated access token is not available.');
    }

    const response = await fetch(userInfoGetUrl, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${this.accessToken}`,
        'Content-Type': 'application/json'
      }
    });

    const payload = await this.readJsonResponse<UserInfoGetResponse>(response);

    if (!response.ok || payload.success === false) {
      throw new Error(payload.error || payload.message || 'Unable to load user information.');
    }

    return payload;
  }

  private async loadDevelopmentUserInfoGetResponse(): Promise<UserInfoGetResponse> {
    const response = await fetch('/assets/mock/user-info-get-response.json', {
      method: 'GET',
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error('Development user information response file was not found.');
    }

    const payload = await this.readJsonResponse<UserInfoGetResponse>(response);

    if (payload.success === false) {
      throw new Error(payload.error || payload.message || 'Unable to load user information.');
    }

    return payload;
  }

  private async updateUserInfo(fullName: string, email: string): Promise<UserInfoUpdateResponse> {
    if (this.isDevelopmentMode()) {
      return await this.loadDevelopmentUserInfoUpdateResponse();
    }

    const config = await this.getRuntimeConfig();
    const userInfoUpdateUrl = config.usermanagementApi?.userInfoUpdate;

    if (!userInfoUpdateUrl) {
      throw new Error('User information update API URL is not configured.');
    }

    if (!this.accessToken) {
      throw new Error('Authenticated access token is not available.');
    }

    const response = await fetch(userInfoUpdateUrl, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.accessToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        fullName,
        email
      })
    });

    const payload = await this.readJsonResponse<UserInfoUpdateResponse>(response);

    if (!response.ok || payload.success === false) {
      throw new Error(payload.error || payload.message || 'Unable to update user profile.');
    }

    return payload;
  }

  private async loadDevelopmentUserInfoUpdateResponse(): Promise<UserInfoUpdateResponse> {
    const response = await fetch('/assets/mock/user-info-update-response.json', {
      method: 'GET',
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error('Development user information update response file was not found.');
    }

    const payload = await this.readJsonResponse<UserInfoUpdateResponse>(response);

    if (payload.success === false) {
      throw new Error(payload.error || payload.message || 'Unable to update user profile.');
    }

    return payload;
  }

  private async changePassword(currentPassword: string, newPassword: string): Promise<ChangePasswordResponse> {
    if (this.isDevelopmentMode()) {
      return await this.loadDevelopmentChangePasswordResponse();
    }

    const config = await this.getRuntimeConfig();
    const changePasswordUrl = config.usermanagementApi?.changePassword;

    if (!changePasswordUrl) {
      throw new Error('Change password API URL is not configured.');
    }

    if (!this.accessToken) {
      throw new Error('Authenticated access token is not available.');
    }

    const response = await fetch(changePasswordUrl, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.accessToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        currentPassword,
        newPassword
      })
    });

    const payload = await this.readJsonResponse<ChangePasswordResponse>(response);

    if (!response.ok || payload.success === false) {
      throw new Error(payload.error || payload.message || 'Unable to change password.');
    }

    return payload;
  }

  private async loadDevelopmentChangePasswordResponse(): Promise<ChangePasswordResponse> {
    const response = await fetch('/assets/mock/change-password-response.json', {
      method: 'GET',
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error('Development change password response file was not found.');
    }

    const payload = await this.readJsonResponse<ChangePasswordResponse>(response);

    if (payload.success === false) {
      throw new Error(payload.error || payload.message || 'Unable to change password.');
    }

    return payload;
  }

  private async getRuntimeConfig(): Promise<TracksterRuntimeConfig> {
    if (this.runtimeConfig) {
      return this.runtimeConfig;
    }

    const response = await fetch('/assets/config.json', {
      method: 'GET',
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error('Trackster runtime configuration could not be loaded.');
    }

    this.runtimeConfig = await this.readJsonResponse<TracksterRuntimeConfig>(response);
    return this.runtimeConfig;
  }

  private async readJsonResponse<T>(response: Response): Promise<T> {
    const text = await response.text();

    if (!text.trim()) {
      return {} as T;
    }

    return JSON.parse(text) as T;
  }

  private isDevelopmentMode(): boolean {
    const authServiceWithDevFlag = this.authService as AuthService & {
      isDevelopmentMode?: () => boolean;
      isDevMode?: () => boolean;
      isLocalDev?: () => boolean;
    };

    if (typeof authServiceWithDevFlag.isDevelopmentMode === 'function') {
      return authServiceWithDevFlag.isDevelopmentMode();
    }

    if (typeof authServiceWithDevFlag.isDevMode === 'function') {
      return authServiceWithDevFlag.isDevMode();
    }

    if (typeof authServiceWithDevFlag.isLocalDev === 'function') {
      return authServiceWithDevFlag.isLocalDev();
    }

    return this.username === 'local-dev';
  }

  private getUserInfoUpdateErrorMessage(error: unknown): string {
    if (error instanceof Error && error.message.trim()) {
      return error.message;
    }

    return 'Unable to save your profile information. Please try again later.';
  }

  private getChangePasswordErrorMessage(error: unknown): string {
    if (error instanceof Error && error.message.trim()) {
      return error.message;
    }

    return 'Unable to change your password. Please verify your current password and try again.';
  }

  private openMessageDialog(title: string, message: string): void {
    if (!this.messageDialog) {
      return;
    }

    this.messageTitle = title;
    this.messageText = message;

    this.dialog.open(this.messageDialog, {
      width: '420px',
      panelClass: 'trackster-profile-dialog-panel'
    });
  }

  private getProfileValidationMessage(): string {
    if (!this.editableProfileEmail.trim()) {
      return 'Email is required.';
    }

    if (!this.editableProfileEmail.includes('@')) {
      return 'Email must contain @.';
    }

    if (!this.editableProfileName.trim()) {
      return 'Full name is required.';
    }

    return '';
  }

  private getPasswordValidationMessage(): string {
    if (!this.currentPassword) {
      return 'Current password is required.';
    }

    if (!this.newPassword) {
      return 'New password is required.';
    }

    if (!this.confirmNewPassword) {
      return 'Password confirmation is required.';
    }

    if (this.newPassword !== this.confirmNewPassword) {
      return 'New password and confirmation do not match.';
    }

    return '';
  }

  private syncEditableProfileFields(): void {
    this.editableProfileName = this.name || '';
    this.editableProfileEmail = this.email || '';
  }

  private clearPasswordFields(): void {
    this.currentPassword = '';
    this.newPassword = '';
    this.confirmNewPassword = '';
  }

  private clearPasswordFeedback(): void {
    this.passwordErrorMessage = '';
    this.passwordSuccessMessage = '';
  }

  private async initializeApp(): Promise<void> {
    try {
      const accessContext = await this.authService.getUserAccessContext();

      if (!accessContext.isAuthenticated) {
        throw new Error('Authenticated session is not available.');
      }

      this.applyAuthenticationContext(accessContext);

      const userInfoResponse = await this.getUserInfo();

      if (!userInfoResponse.user) {
        throw new Error('User information was not returned by the API.');
      }

      this.applyUserInfoFromApi(userInfoResponse);

      this.authReady = true;
    } catch {
      this.isAuthenticated = false;
      this.authReady = true;
      this.resetUserAccessContext();
    }
  }

  private applyAuthenticationContext(accessContext: TracksterUserAccessContext): void {
    this.userAccessContext = accessContext;

    this.isAuthenticated = accessContext.isAuthenticated;

    this.cognitoSub = accessContext.cognitoSub;
    this.groups = accessContext.groups;
    this.idToken = accessContext.idToken;
    this.accessToken = accessContext.accessToken;

    this.username = accessContext.username || 'User';
  }

  private applyUserInfoFromApi(response: UserInfoGetResponse): void {
    const user = response.user;

    if (!user) {
      throw new Error('User information was not returned by the API.');
    }

    this.username = user.username || this.username || 'User';
    this.email = user.email || '';
    this.name = user.fullName || '';

    this.globalRole = user.globalRole ?? null;
    this.clientRole = user.clientRole ?? null;
    this.clientId = user.clientId || '';

    this.syncEditableProfileFields();

    if (!this.canSeeAdministrationTab && this.activeModule === 'administration') {
      this.activeModule = 'generator';
      this.selectedTabIndex = 0;
    }
  }

  private resetUserAccessContext(): void {
    this.userAccessContext = null;

    this.username = 'User';

    this.globalRole = null;
    this.clientRole = null;
    this.clientId = '';

    this.cognitoSub = '';
    this.email = '';
    this.name = '';
    this.groups = [];
    this.idToken = null;
    this.accessToken = null;

    this.profileDialogMode = 'view';
    this.editableProfileName = '';
    this.editableProfileEmail = '';
    this.clearPasswordFields();
    this.clearPasswordFeedback();

    this.activeModule = 'generator';
    this.selectedTabIndex = 0;
  }
}