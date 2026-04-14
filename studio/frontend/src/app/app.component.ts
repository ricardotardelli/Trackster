import { SimulatorComponent } from './simulator/simulator.component';
import { DbcworkspaceComponent } from './dbcworkspace/dbcworkspace.component';
import { DecoderComponent } from './decoder/decoder.component';
import { MatTabsModule } from '@angular/material/tabs';
import { CommonModule } from '@angular/common';
import { Component, ElementRef, HostListener, OnInit, ViewChild } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { AuthService } from './auth/auth.service';

type WorkspaceModule = 'generator' | 'decoder' | 'dbc-manager';

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
    DecoderComponent
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

  readonly workspaceTabs: readonly WorkspaceTab[] = [
    {
      id: 'generator',
      label: 'Simulation Studio',
      shortLabel: 'Studio',
      description: 'Build and generate CAN simulation packages.'
    },
    {
      id: 'decoder',
      label: 'Signal Decoder',
      shortLabel: 'Decoder',
      description: 'Inspect frames, decode payloads, and validate signals.'
    },
    {
      id: 'dbc-manager',
      label: 'DBC Vault',
      shortLabel: 'Vault',
      description: 'Organize, validate, and prepare DBC assets.'
    }
  ];

  @ViewChild('userMenuContainer', { static: false })
  private userMenuContainer?: ElementRef<HTMLElement>;

  ngOnInit(): void {
    void this.initializeApp();
  }

  get isGeneratorModule(): boolean {
    return this.activeModule === 'generator';
  }

  get isDecoderModule(): boolean {
    return this.activeModule === 'decoder';
  }

  get isDbcManagerModule(): boolean {
    return this.activeModule === 'dbc-manager';
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

  onTabChange(index: number): void {
    this.selectedTabIndex = index;

    if (index === 0) {
      this.activeModule = 'generator';
    } else if (index === 1) {
      this.activeModule = 'dbc-manager';
    } else if (index === 2) {
      this.activeModule = 'decoder';
    }

    this.closeUserMenu();
  }

  setActiveModule(module: WorkspaceModule): void {
    if (this.activeModule === module) {
      return;
    }

    this.activeModule = module;

    if (module === 'generator') {
      this.selectedTabIndex = 0;
    } else if (module === 'dbc-manager') {
      this.selectedTabIndex = 1;
    } else if (module === 'decoder') {
      this.selectedTabIndex = 2;
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

    if (!this.elementRef.nativeElement.contains(target)) {
      this.userMenuOpen = false;
      return;
    }

    this.userMenuOpen = false;
  }

  private async initializeApp(): Promise<void> {
    try {
      const authenticated = await this.authService.isAuthenticated();

      if (!authenticated) {
        throw new Error('Authenticated session is not available.');
      }

      this.isAuthenticated = true;
      await this.loadUsername();
      this.authReady = true;
    } catch {
      this.isAuthenticated = false;
      this.authReady = true;
    }
  }

  private async loadUsername(): Promise<void> {
    try {
      const username = await this.authService.getUsername();
      this.username = username?.trim() || 'User';
    } catch {
      this.username = 'User';
    }
  }
}