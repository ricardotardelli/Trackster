import { SimulatorComponent } from './simulator/simulator.component';
import { DbcworkspaceComponent } from './dbcworkspace/dbcworkspace.component';
import { DecoderComponent } from './decoder/decoder.component';
import { SignalPlotterComponent } from './signal-plotter/signal-plotter.component';
import { ClientAdminComponent } from './adminmodule/client-admin.component';
import { MasterAdminComponent } from './adminmodule/master-admin.component';
import { MatTabsModule } from '@angular/material/tabs';
import { CommonModule } from '@angular/common';
import { Component, ElementRef, HostListener, OnInit, ViewChild } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import {
  AuthService,
  TracksterClientRole,
  TracksterGlobalRole,
  TracksterUserAccessContext
} from './auth/auth.service';

type WorkspaceModule =
  | 'generator'
  | 'dbc-manager'
  | 'decoder'
  | 'signal-plotter'
  | 'administration';

interface WorkspaceTab {
  id: WorkspaceModule;
  label: string;
  shortLabel: string;
  description: string;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    RouterOutlet,
    MatTabsModule,
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
    private readonly authService: AuthService
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

  @ViewChild('userMenuContainer', { static: false })
  private userMenuContainer?: ElementRef<HTMLElement>;

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

  private async initializeApp(): Promise<void> {
    try {
      const accessContext = await this.authService.getUserAccessContext();

      if (!accessContext.isAuthenticated) {
        throw new Error('Authenticated session is not available.');
      }

      this.applyUserAccessContext(accessContext);
      this.authReady = true;
    } catch {
      this.isAuthenticated = false;
      this.authReady = true;
      this.resetUserAccessContext();
    }
  }

  private applyUserAccessContext(accessContext: TracksterUserAccessContext): void {
    this.userAccessContext = accessContext;

    this.isAuthenticated = accessContext.isAuthenticated;
    this.username = accessContext.username || 'User';

    this.globalRole = accessContext.globalRole;
    this.clientRole = accessContext.clientRole;
    this.clientId = accessContext.clientId;

    this.cognitoSub = accessContext.cognitoSub;
    this.email = accessContext.email;
    this.name = accessContext.name;
    this.groups = accessContext.groups;
    this.idToken = accessContext.idToken;
    this.accessToken = accessContext.accessToken;

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

    this.activeModule = 'generator';
    this.selectedTabIndex = 0;
  }
}