import { CommonModule } from '@angular/common';
import {
  AbstractControl,
  FormBuilder,
  FormsModule,
  ReactiveFormsModule,
  ValidationErrors,
  Validators
} from '@angular/forms';
import {
  Component,
  ElementRef,
  HostListener,
  OnInit,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { ComponentType } from '@angular/cdk/portal';
import { fetchAuthSession } from 'aws-amplify/auth';
import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';

import { AuthService } from '../auth/auth.service';
import { environment } from '../../environments/environment';
import { PayloadComponent } from '../payloadmodule/payload.component';
import { MapmoduleComponent } from '../mapmodule/mapmodule.component';
import { interpolateGpsPerBlock } from '../interpmodule/interpmodule.util';

interface RoutePoint {
  lat: number;
  lng: number;
  label?: string;
}

interface RoutePayload {
  start: RoutePoint | null;
  waypoints: Record<string, RoutePoint>;
  destination: RoutePoint | null;
}

interface CanFrameOption {
  dbcFile: string;
  canId: string;
  messageName: string;
  label: string;
}

type DistanceUnit = 'Km' | 'Mi';

type OutputFormatValue = 'Trackster BIN';

type SimulationModeValue =
  | 'Time Window'
  | 'Adaptive Blocks'
  | 'Velocity Target'
  | 'Distance Target';

type DriverProfileValue = | 'Balanced' | 'Efficiency' | 'Dynamic' | 'Performance' | 'City Cycle' | 'Cruise' | 'Terrain' | 'Fleet';

type GpsCoordinateRle = [coordinate: string, repeat: number];

interface DriverProfileOption {
  value: DriverProfileValue;
  label: string;
  description: string;
}

interface SimulationModeOption {
  value: SimulationModeValue;
  label: string;
  description: string;
}

interface DevelopmentRunManifestResolvedCanFrame {
  dbcFile?: string;
  canId?: string;
  messageName?: string;
  frame?: {
    n?: string;
  };
}

interface DevelopmentRunManifest {
  dbc?: {
    resolvedCanFrames?: DevelopmentRunManifestResolvedCanFrame[];
    canFrames?: DevelopmentRunManifestResolvedCanFrame[];
  };
}

interface GenerationMonitor {
  bucket: string;
  scenarioKey: string;
  runFolder: string;
  expectedBinCount: number;
}

interface ScenarioProgress {
  stage?: string;
  totalPhases?: number;
  completedPhases?: number;
  failedPhases?: number;
  processingPhases?: number;
  currentPhase?: number;
  percent?: number;
  generatedBinCount?: number;
  expectedBinCount?: number;
}

interface ScenarioLogEntry {
  at?: string;
  level?: string;
  step?: string;
  phaseId?: number | null;
  message?: string;
  validationErrors?: string[] | null;
}

interface ScenarioStatusDocument {
  status?: string;
  currentStep?: string;
  progress?: ScenarioProgress;
  behavior?: {
    phases?: Array<{
      phaseId?: number;
      status?: string;
      error?: {
        message?: string;
        validationErrors?: string[] | null;
        rawModelResponse?: string | null;
      } | null;
    }>;
  };
  error?: {
    message?: string;
    validationErrors?: string[] | null;
    rawModelResponse?: string | null;
  } | null;
  logs?: ScenarioLogEntry[];
  signalKnowledge?: unknown;
  signalKnowledgeS3?: unknown;
  scenario?: {
    phases?: unknown[];
  } | null;
  output?: {
    generatedBinCount?: number;
    expectedBinCount?: number;
  };
  signalBehaviorPlan?: {
    result?: {
      phases?: Array<{
        phaseId?: number;
        status?: string;
      }>;
    };
  };
}

type GenerationWorkflowState = 'idle' | 'running' | 'success' | 'error';

@Component({
  selector: 'app-simulator',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
    MatIconModule,
    MatProgressBarModule
  ],
  templateUrl: './simulator.component.html',
  styleUrl: './simulator.component.css'
})
export class SimulatorComponent implements OnInit {
  @ViewChild('generationWorkflowDialog') generationWorkflowDialog?: TemplateRef<unknown>;

  constructor(
    private readonly fb: FormBuilder,
    private readonly elementRef: ElementRef<HTMLElement>,
    private readonly authService: AuthService,
    private readonly dialog: MatDialog
  ) {}

  readonly outputFormatOptions: readonly OutputFormatValue[] = ['Trackster BIN'];

  readonly driverProfileOptions: readonly DriverProfileOption[] = [
    {
      value: 'Balanced',
      label: 'Balanced',
      description: 'General-purpose driving behavior.'
    },
    {
      value: 'Efficiency',
      label: 'Efficiency',
      description: 'Lower energy and smoother transitions.'
    },
    {
      value: 'Dynamic',
      label: 'Dynamic',
      description: 'Sharper response with more variation.'
    },
    {
      value: 'Performance',
      label: 'Performance',
      description: 'Higher intensity and aggressive pacing.'
    },
    {
      value: 'City Cycle',
      label: 'City Cycle',
      description: 'Urban stop-and-go behavior.'
    },
    {
      value: 'Cruise',
      label: 'Cruise',
      description: 'Stable highway-oriented behavior.'
    },
    {
      value: 'Terrain',
      label: 'Terrain',
      description: 'Irregular path and off-grid emphasis.'
    },
    {
      value: 'Fleet',
      label: 'Fleet',
      description: 'Commercial and delivery-style behavior.'
    }
  ];

  readonly simulationModeOptions: readonly SimulationModeOption[] = [
    {
      value: 'Time Window',
      label: 'Time Window',
      description: 'Resolve output from duration.'
    },
    {
      value: 'Adaptive Blocks',
      label: 'Adaptive Blocks',
      description: 'Resolve output by block count.'
    },
    {
      value: 'Velocity Target',
      label: 'Velocity Target',
      description: 'Resolve output from speed.'
    },
    {
      value: 'Distance Target',
      label: 'Distance Target',
      description: 'Resolve output from route distance.'
    }
  ];

  gpsAreas: string[] = [];
  canFrameOptions: string[] = [];
  dbcOptions: string[] = [];
  customerId = '00000000';
  selectedGpsCoordinates: string[] = [];
  interpGpsCoords: string[] = [];
  routeDataForMap: RoutePayload | null = null;

  isGpsOpen = false;
  gpsFilter = '';
  isCanOpen = false;
  isDbcOpen = false;
  isDriverProfileOpen = false;
  isUnityOpen = false;
  isSimulationModeOpen = false;
  isOutputFormatOpen = false;

  isSubmitting = false;
  isConfigLoaded = false;

  generationWorkflowState: GenerationWorkflowState = 'idle';
  generationWorkflowTitle = '';
  generationWorkflowMessage = '';
  generationWorkflowDetails = '';
  generationWorkflowProgress = 0;

  formStatus: 'pending' | 'awaiting_response' | 'generating' | 'generated' | 'error' = 'pending';
  generationTimestamp = '';

  isLoadingDbcOptions = false;
  isLoadingCanFrameOptions = false;

  private suppressFormReset = false;
  private formValueChangesBound = false;
  private canFrameCatalog: CanFrameOption[] = [];
  private generationWorkflowDialogRef?: MatDialogRef<unknown>;
  private readonly developmentRunManifestUrl = 'assets/mock/run-manifest.json';
  private readonly generationPollIntervalMs = 5000;
  private readonly maxGenerationPollAttempts = 240;
  private s3Region = 'us-east-1';

  readonly form = this.fb.nonNullable.group({
    amountOfVehicles: [1, [Validators.required, Validators.min(0), Validators.pattern(/^\d+$/)]],
    amountOfTime: [1, [Validators.required, Validators.pattern(/^\d+(\.\d+)?$/)]],
    generationTypeAllAtOnce: [true],
    numberOfBlocks: [0, [Validators.required, Validators.min(0), Validators.pattern(/^\d+$/)]],
    sizeOfBlocksBytes: [7560, [Validators.required, Validators.pattern(/^\d+$/)]],
    gpsArea: ['', [Validators.required]],
    canFrames: [[] as string[], [Validators.required]],
    dbcFiles: [[] as string[], [Validators.required]],
    vinPrefix: ['', [Validators.required, Validators.minLength(6), Validators.maxLength(6)]],
    initialDateTime: [this.getCurrentDateTimeLocal(), [Validators.required]],
    vinSuffix: ['', [Validators.required, Validators.minLength(5), Validators.maxLength(5)]],
    latencyTime: [5, [Validators.required, Validators.pattern(/^\d+$/)]],
    speed: [80, [Validators.required, Validators.pattern(/^\d+(\.\d+)?$/)]],
    simulationMode: ['Time Window' as SimulationModeValue, Validators.required],
    unity: ['Km' as DistanceUnit, [Validators.required]],
    outputFormat: ['BIN' as OutputFormatValue, Validators.required],
    s3Bucket: ['', [Validators.required]],
    workQueueUrl: [''],
    engineUrl: ['', [Validators.required, Validators.pattern(/^https?:\/\/.+/i)]],
    payload: ['', [this.jsonValidator]],
    driverProfile: ['Balanced' as DriverProfileValue, Validators.required]
  });

  ngOnInit(): void {
    void this.initializeComponent();
  }

  get f() {
    return this.form.controls;
  }

  get gpsSummary(): string {
    return this.form.controls.gpsArea.value || 'Select country';
  }

  get filteredGpsAreas(): readonly string[] {
    const query = this.gpsFilter.trim().toLowerCase();
    if (!query) {
      return this.gpsAreas;
    }

    return this.gpsAreas.filter((area) => area.toLowerCase().includes(query));
  }

  get canSummary(): string {
    const selected = this.form.controls.canFrames.value;
    if (selected.length === 0) {
      return 'Select CAN frames';
    }

    return `${selected.length} selected`;
  }

  get dbcSummary(): string {
    const selected = this.form.controls.dbcFiles.value;
    if (selected.length === 0) {
      return 'Select DBC files';
    }

    if (selected.length === this.dbcOptions.length) {
      return 'All selected';
    }

    return selected.join(', ');
  }

  get formStatusLabel(): string {
    if (this.formStatus === 'awaiting_response') {
      return 'Awaiting Response';
    }
    if (this.formStatus === 'generating') {
      return 'Generating';
    }
    if (this.formStatus === 'generated') {
      return 'Generated';
    }
    if (this.formStatus === 'error') {
      return 'Error';
    }
    return 'Pending';
  }

  get driverProfileSummary(): string {
    return this.form.controls.driverProfile.value || 'Balanced';
  }

  get unitySummary(): string {
    return this.form.controls.unity.value || 'Km';
  }

  get outputFormatSummary(): string {
    return this.form.controls.outputFormat.value || 'BIN';
  }

  get simulationModeSummary(): string {
    return this.form.controls.simulationMode.value || 'Time Window';
  }

  get generationTypeSummary(): string {
    return this.f.generationTypeAllAtOnce.value ? 'Burst Generation' : 'Progressive Flow';
  }

  get selectedDriverProfileDescription(): string {
    return (
      this.driverProfileOptions.find(
        (option) => option.value === this.form.controls.driverProfile.value
      )?.description ?? ''
    );
  }

  get selectedSimulationModeDescription(): string {
    return (
      this.simulationModeOptions.find(
        (option) => option.value === this.form.controls.simulationMode.value
      )?.description ?? ''
    );
  }

  openPayloadModal(): void {
    this.openTracksterDialog(PayloadComponent, {
      data: {
        payloadText: this.form.controls.payload.value || ''
      }
    });
  }

  openMap(event: MouseEvent): void {
    event.stopPropagation();
    event.preventDefault();

    const routeData = this.rebuildRoutePayloadFromHexCoordinates(this.selectedGpsCoordinates);

    this.isGpsOpen = false;

    const dialogRef = this.openTracksterDialog(MapmoduleComponent, {
      data: {
        country: this.form.controls.gpsArea.value,
        routeData
      },
      width: '1100px',
      height: '86vh'
    });

    dialogRef.afterClosed().subscribe((routeJson?: string) => {
      if (routeJson) {
        this.onSaveRoute(routeJson);
      }
    });
  }

  toggleSimulationModeOpen(): void {
    this.isSimulationModeOpen = !this.isSimulationModeOpen;
    if (this.isSimulationModeOpen) {
      this.closeAllDropdowns();
      this.isSimulationModeOpen = true;
    }
  }

  selectSimulationMode(mode: SimulationModeValue): void {
    this.form.controls.simulationMode.setValue(mode);
    this.form.controls.simulationMode.markAsTouched();
    this.isSimulationModeOpen = false;
    this.updatePayloadPreview();
  }

  toggleCanOpen(): void {
    if (!this.isConfigLoaded || this.isLoadingCanFrameOptions) {
      return;
    }

    this.isCanOpen = !this.isCanOpen;
    if (this.isCanOpen) {
      this.closeAllDropdowns();
      this.isCanOpen = true;
    }
  }

  toggleDbcOpen(): void {
     if (!this.isConfigLoaded || this.isLoadingDbcOptions) {
        return;
      }

    this.isDbcOpen = !this.isDbcOpen;
    if (this.isDbcOpen) {
      this.closeAllDropdowns();
      this.isDbcOpen = true;
    }
  }

  toggleGpsOpen(): void {
    if (!this.isConfigLoaded) {
      return;
    }

    this.isGpsOpen = !this.isGpsOpen;
    if (this.isGpsOpen) {
      this.closeAllDropdowns();
      this.gpsFilter = '';
      this.isGpsOpen = true;
    }
  }

  toggleDriverProfileOpen(): void {
    this.isDriverProfileOpen = !this.isDriverProfileOpen;
    if (this.isDriverProfileOpen) {
      this.closeAllDropdowns();
      this.isDriverProfileOpen = true;
    }
  }

  toggleUnityOpen(): void {
    this.isUnityOpen = !this.isUnityOpen;
    if (this.isUnityOpen) {
      this.closeAllDropdowns();
      this.isUnityOpen = true;
    }
  }

  toggleOutputFormatOpen(): void {
    this.isOutputFormatOpen = !this.isOutputFormatOpen;
    if (this.isOutputFormatOpen) {
      this.closeAllDropdowns();
      this.isOutputFormatOpen = true;
    }
  }

  selectGpsArea(area: string): void {
    this.form.controls.gpsArea.setValue(area);
    this.form.controls.gpsArea.markAsTouched();
    this.gpsFilter = '';
    this.isGpsOpen = false;
    this.updatePayloadPreview();
  }

  selectDriverProfile(value: DriverProfileValue): void {
    this.form.controls.driverProfile.setValue(value);
    this.form.controls.driverProfile.markAsTouched();
    this.isDriverProfileOpen = false;
    this.updatePayloadPreview();
  }

  selectUnity(value: DistanceUnit): void {
    this.form.controls.unity.setValue(value);
    this.form.controls.unity.markAsTouched();
    this.isUnityOpen = false;
    this.updatePayloadPreview();
  }

  selectOutputFormat(value: OutputFormatValue): void {
    this.form.controls.outputFormat.setValue(value);
    this.form.controls.outputFormat.markAsTouched();
    this.isOutputFormatOpen = false;
    this.updatePayloadPreview();
  }

  isCanSelected(option: string): boolean {
    return this.form.controls.canFrames.value.includes(option);
  }

  onCanToggle(option: string, checked: boolean): void {
    const selected = this.form.controls.canFrames.value;
    const next = checked
      ? Array.from(new Set([...selected, option]))
      : selected.filter((item) => item !== option);

    this.form.controls.canFrames.setValue(next);
    this.form.controls.canFrames.markAsTouched();
    this.updatePayloadPreview();
  }

  selectAllCanFrames(): void {
    this.form.controls.canFrames.setValue([...this.canFrameOptions]);
    this.form.controls.canFrames.markAsTouched();
    this.updatePayloadPreview();
  }

  clearAllCanFrames(): void {
    this.form.controls.canFrames.setValue([]);
    this.form.controls.canFrames.markAsTouched();
    this.updatePayloadPreview();
  }

  isDbcSelected(option: string): boolean {
    return this.form.controls.dbcFiles.value.includes(option);
  }

  onDbcToggle(option: string, checked: boolean): void {
    const selected = this.form.controls.dbcFiles.value;
    const next = checked
      ? Array.from(new Set([...selected, option]))
      : selected.filter((item) => item !== option);

    this.form.controls.dbcFiles.setValue(next);
    this.form.controls.dbcFiles.markAsTouched();

    this.updateCanFrameOptionsFromSelectedDbcs();
    this.updatePayloadPreview();
  }

  formatJsonField(field: 'payload'): void {
    const control = this.form.controls[field];
    const value = control.value.trim();
    if (!value) {
      return;
    }

    try {
      const parsed = JSON.parse(value);
      control.setValue(JSON.stringify(parsed, null, 2));
    } catch {
      control.markAsTouched();
    }
  }

  async submit(): Promise<void> {
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }

    const envelope = this.buildEngineEnvelope();
    const request = {
      method: 'POST',
      url: this.form.controls.engineUrl.value,
      body: envelope
    };
    const generationStartedAtMs = performance.now();

    this.closeGenerationWorkflowDialogOnly();
    this.resetGenerationWorkflowDialog();
    this.generationTimestamp = this.makeGenerationTimestamp();
    this.formStatus = 'awaiting_response';
    this.isSubmitting = true;
    this.openGenerationModal(
      'Preparing signal analysis...',
      'Trackster is preparing the selected CAN signals for analysis.'
    );

    try {
      if (this.isAuthDisabled()) {
        const localResult = {
          request: {
            ...request,
            headers: {
              'Content-Type': 'application/json',
              Authorization: '[LOCAL_MODE_DISABLED]'
            }
          },
          response: {
            ok: true,
            status: 200,
            statusText: 'OK',
            headers: {},
            body: {
              localMode: true,
              message: 'Request was not sent because authentication is disabled in local mode.',
              generatedAt: new Date().toISOString()
            }
          }
        };

        this.formStatus = 'generated';
        this.setPayloadValue(JSON.stringify(localResult, null, 2));
        this.closeGenerationModal();
        this.logGenerationDuration(generationStartedAtMs, {
          status: 'generated',
          mode: 'local',
          expectedBinCount: 0,
          generatedBinCount: 0
        });
        this.openGenerationSuccessModal(
          'Signals analyzed successfully.',
          'Local mode completed without sending the signal analysis request.'
        );
        return;
      }

      const authorizationToken = await this.getAuthorizationToken();

      this.setPayloadValue(JSON.stringify(request.body, null, 2));

      const response = await fetch(request.url, {
        method: request.method,
        headers: {
          'Content-Type': 'application/json',
          Authorization: authorizationToken ?? ''
        },
        body: JSON.stringify(request.body)
      });

      let responseBody: unknown;
      try {
        responseBody = await this.parseResponseBody(response);
      } catch (parseError: unknown) {
        responseBody = {
          rawBody: null,
          parseError: this.describeFetchError(parseError)
        };
      }

      const result: Record<string, unknown> = {
        request: {
          ...request,
          headers: {
            'Content-Type': 'application/json',
            Authorization: '[REDACTED]'
          }
        },
        response: {
          ok: response.ok,
          status: response.status,
          statusText: response.statusText,
          headers: this.serializeHeaders(response.headers),
          body: responseBody
        }
      };

      if (!response.ok) {
        this.formStatus = 'error';
        result['error'] = {
          category: 'http_error',
          message: `Request failed with status ${response.status} (${response.statusText || 'no status text'})`,
          httpErrorCode: response.status,
          httpStatus: response.status,
          httpStatusText: response.statusText || null,
          details: this.describeHttpStatus(response.status),
          timestamp: new Date().toISOString()
        };
      }

      if (response.ok) {
        const monitor = this.resolveGenerationMonitor(responseBody, request.body);

        this.setPayloadValue(JSON.stringify(result, null, 2));

        if (monitor) {
          this.formStatus = 'generating';
          this.openGenerationModal(
            'Analyzing selected signals...',
            'Trackster is checking the signal dictionary and identifying any unknown signals.'
          );

          const scenarioDocument = await this.waitForScenarioCompletion(monitor);
          const generatedBinCount = this.resolveGeneratedBinCountFromScenario(scenarioDocument);

          this.formStatus = 'generated';
          this.logGenerationDuration(generationStartedAtMs, {
            status: 'generated',
            mode: 'scenario-json-polling',
            bucket: monitor.bucket,
            scenarioKey: monitor.scenarioKey,
            expectedBinCount: monitor.expectedBinCount,
            generatedBinCount
          });
          this.openGenerationSuccessModal(
            this.buildScenarioSuccessTitle(scenarioDocument),
            this.buildScenarioSuccessMessage(scenarioDocument, monitor)
          );
        } else {
          this.formStatus = 'generated';
          this.closeGenerationModal();
          this.logGenerationDuration(generationStartedAtMs, {
            status: 'generated',
            mode: 'http-response-only',
            expectedBinCount: Number(request.body.amountOfVehicles),
            generatedBinCount: null
          });
          this.openGenerationSuccessModal(
            'Signal analysis request accepted.',
            'The selected signals were submitted successfully for analysis.'
          );
        }
      } else {
        this.setPayloadValue(JSON.stringify(result, null, 2));
        this.openGenerationErrorModal(
          'Signal analysis failed',
          `The signal analysis request failed with status ${response.status}.`,
          this.describeHttpStatus(response.status)
        );
      }
    } catch (error: unknown) {
      this.formStatus = 'error';
      this.closeGenerationModal();
      const details = this.describeFetchError(error);
      this.setPayloadValue(JSON.stringify({
        request: {
          ...request,
          headers: {
            'Content-Type': 'application/json',
            Authorization: '[REDACTED]'
          }
        },
        error: {
          category: 'network_or_runtime_error',
          httpErrorCode: null,
          httpStatus: null,
          httpStatusText: null,
          ...details,
          timestamp: new Date().toISOString(),
          hints: [
            'Check if Engine URL is reachable from the browser.',
            'If it is a different domain, verify CORS configuration on the API.',
            'Check browser DevTools > Network for blocked/preflight requests.',
            'Check if the Cognito session is still valid and contains a token.'
          ]
        }
      }, null, 2));

      this.openGenerationErrorModal(
        'Signal analysis failed',
        String(details['message'] || 'Unable to complete the signal analysis.'),
        'Please check the browser console and try again.'
      );
    } finally {
      this.form.controls.payload.markAsTouched();
      this.isSubmitting = false;
      if (this.formStatus !== 'generated' && this.generationWorkflowState === 'running') {
        this.closeGenerationModal();
      }
    }
  }

  onSaveRoute(routeJson: string): void {
    try {
      const parsed = JSON.parse(routeJson) as RoutePayload;

      this.routeDataForMap = parsed;
      this.selectedGpsCoordinates = this.buildSequentialGpsHexCoordinates(parsed);

      const explicitBlocks = Number(this.form.controls.numberOfBlocks.value);
      const latency = Number(this.form.controls.latencyTime.value);
      const amountOfTime = Number(this.form.controls.amountOfTime.value);

      const resolvedBlocks =
        explicitBlocks > 0
          ? explicitBlocks
          : Math.floor((amountOfTime * 3600) / latency);

      const speed = Number(this.form.controls.speed.value) || 80;
      const unity = this.form.controls.unity.value === 'Mi' ? 'Mi' : 'Km';

      this.interpGpsCoords = interpolateGpsPerBlock(
        this.selectedGpsCoordinates,
        speed,
        unity,
        latency,
        resolvedBlocks
      );
    } catch {
      this.routeDataForMap = null;
      this.selectedGpsCoordinates = [];
      this.interpGpsCoords = [];
      this.updatePayloadPreview();
    }

    this.updatePayloadPreview();
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as Node | null;
    if (!target) {
      return;
    }

    if (!this.elementRef.nativeElement.contains(target)) {
      this.closeAllDropdowns();
    }
  }

  private async initializeComponent(): Promise<void> {
    this.bindFormValueChangesOnce();

    try {
      await this.loadConfig();
      this.updatePayloadPreview();
    } catch (error: unknown) {
      this.formStatus = 'error';
      this.setPayloadValue(JSON.stringify({
        error: {
          category: 'simulator_initialization_error',
          ...this.describeFetchError(error)
        }
      }, null, 2));
      this.form.controls.payload.markAsTouched();
    }
  }

  private isAuthDisabled(): boolean {
    const isLocalhost =
      window.location.hostname === 'localhost' ||
      window.location.hostname === '127.0.0.1';

    return environment.disableAuth && isLocalhost;
  }

  private async getAuthorizationToken(): Promise<string | null> {
    if (this.isAuthDisabled()) {
      return null;
    }

    const idToken = await this.authService.getIdToken();
    if (idToken) {
      return idToken;
    }

    const accessToken = await this.authService.getAccessToken();
    if (accessToken) {
      return accessToken;
    }

    throw new Error('Unable to retrieve Cognito token from current session.');
  }

  private bindFormValueChangesOnce(): void {
    if (this.formValueChangesBound) {
      return;
    }

    this.form.valueChanges.subscribe(() => {
      if (this.suppressFormReset) {
        return;
      }

      this.formStatus = 'pending';
      this.revalidateAllControls();
      this.clearPayload();
    });

    this.formValueChangesBound = true;
  }

  private closeAllDropdowns(): void {
    this.isGpsOpen = false;
    this.isCanOpen = false;
    this.isDbcOpen = false;
    this.isDriverProfileOpen = false;
    this.isUnityOpen = false;
    this.isSimulationModeOpen = false;
    this.isOutputFormatOpen = false;
  }

  private async loadConfig(): Promise<void> {
    const config = await this.fetchRuntimeConfig();
    const useLocalMock = this.isAuthDisabled();

    this.isLoadingDbcOptions = true;

    if (!Array.isArray(config.gpsAreas) || config.gpsAreas.length === 0) {
      throw new Error('gpsAreas missing or empty in config.json');
    }

    if (typeof config.workQueueUrl !== 'string' || !config.workQueueUrl.trim()) {
      throw new Error('workQueueUrl missing or empty in config.json');
    }

    if (typeof config.s3Default !== 'string' || !config.s3Default.trim()) {
      throw new Error('s3Default missing or empty in config.json');
    }

    if (typeof config.s3Region === 'string' && config.s3Region.trim()) {
      this.s3Region = config.s3Region.trim();
    }

    if (typeof config.engineURL !== 'string' || !config.engineURL.trim()) {
      throw new Error('engineURL missing or empty in config.json');
    }

    if (!useLocalMock) {
      if (
        typeof config.dbcApi?.folderCatalogUrl !== 'string' ||
        !config.dbcApi.folderCatalogUrl.trim()
      ) {
        throw new Error('dbcApi.folderCatalogUrl missing or empty in config.json');
      }

      if (
        typeof config.dbcApi?.getDbcCanIds !== 'string' ||
        !config.dbcApi.getDbcCanIds.trim()
      ) {
        throw new Error('dbcApi.getDbcCanIds missing or empty in config.json');
      }
    }

    this.suppressFormReset = true;

    this.gpsAreas = [...config.gpsAreas];

    this.form.controls.workQueueUrl.setValue(config.workQueueUrl.trim(), { emitEvent: false });
    this.form.controls.s3Bucket.setValue(config.s3Default.trim(), { emitEvent: false });
    this.form.controls.engineUrl.setValue(config.engineURL.trim(), { emitEvent: false });

    this.form.controls.workQueueUrl.updateValueAndValidity({ emitEvent: false });
    this.form.controls.s3Bucket.updateValueAndValidity({ emitEvent: false });
    this.form.controls.engineUrl.updateValueAndValidity({ emitEvent: false });

    this.isConfigLoaded = true;

    try {
      const validatedDbcFiles = useLocalMock
        ? await this.loadDevelopmentDbcFiles()
        : await this.loadValidatedDbcFiles(
          config.dbcApi!.folderCatalogUrl!.trim()
        );

      this.dbcOptions = [...validatedDbcFiles];
      this.form.controls.dbcFiles.setValue([...this.dbcOptions], { emitEvent: false });
    }
    catch (error) {
      console.error('Unable to load validated DBC files:', error);

      this.dbcOptions = [];
      this.form.controls.dbcFiles.setValue([], { emitEvent: false });
    }
    finally {
      this.isLoadingDbcOptions = false;
    }

    this.isLoadingCanFrameOptions = true;

    try {
      this.canFrameCatalog = useLocalMock
        ? await this.loadDevelopmentCanFrameCatalog()
        : await this.loadCanFrameCatalog(
          config.dbcApi!.getDbcCanIds!.trim()
        );
    }
    catch (error) {
      console.error('Unable to load CAN frame catalog:', error);

      this.canFrameCatalog = [];
    }
    finally {
      this.isLoadingCanFrameOptions = false;
    }

    this.updateCanFrameOptionsFromSelectedDbcs();

    this.form.controls.dbcFiles.updateValueAndValidity({ emitEvent: false });
    this.form.controls.canFrames.updateValueAndValidity({ emitEvent: false });
    this.form.updateValueAndValidity({ emitEvent: false });

    this.suppressFormReset = false;
  }

  private async fetchRuntimeConfig(): Promise<{
    gpsAreas?: string[];
    canFrames?: string[];
    workQueueUrl?: string;
    s3Default?: string;
    s3Region?: string;
    engineURL?: string;
    dbcApi?: {
      folderCatalogUrl?: string;
      getDbcCanIds?: string;
    };
  }> {
    const stamp = Date.now();
    const candidates = [
      '/assets/config.json',
      'assets/config.json',
      `/assets/config.json?t=${stamp}`,
      `assets/config.json?t=${stamp}`
    ];

    let lastStatus: number | null = null;

    for (const url of candidates) {
      try {
        const response = await fetch(url, { cache: 'no-store' });
        lastStatus = response.status;

        if (!response.ok) {
          continue;
        }

        const text = await response.text();
        return JSON.parse(text) as {
          gpsAreas?: string[];
          canFrames?: string[];
          workQueueUrl?: string;
          s3Default?: string;
          s3Region?: string;
          engineURL?: string;
          dbcApi?: {
            folderCatalogUrl?: string;
            getDbcCanIds?: string;
          };
        };
      } catch {
      }
    }

    throw new Error(
      `Unable to load runtime config from assets/config.json. Last HTTP status: ${lastStatus ?? 'unknown'}`
    );
  }

  private async loadDevelopmentRunManifest(): Promise<DevelopmentRunManifest> {
    const response = await fetch(`${this.developmentRunManifestUrl}?t=${Date.now()}`, {
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error(
        `Unable to load development run-manifest. HTTP ${response.status} ${response.statusText || ''}`.trim()
      );
    }

    return await response.json() as DevelopmentRunManifest;
  }

  private async loadDevelopmentDbcFiles(): Promise<string[]> {
    const manifest = await this.loadDevelopmentRunManifest();
    const frames = this.getDevelopmentResolvedCanFrames(manifest);

    const dbcFiles = frames
      .map((frame) => frame.dbcFile?.trim() ?? '')
      .filter((dbcFile) => dbcFile.length > 0);

    return Array.from(new Set(dbcFiles))
      .sort((a, b) => a.localeCompare(b));
  }

  private async loadDevelopmentCanFrameCatalog(): Promise<CanFrameOption[]> {
    const manifest = await this.loadDevelopmentRunManifest();
    const frames = this.getDevelopmentResolvedCanFrames(manifest);

    return frames
      .filter((frame) => {
        return (
          typeof frame.dbcFile === 'string' && frame.dbcFile.trim().length > 0 &&
          typeof frame.canId === 'string' && frame.canId.trim().length > 0 &&
          (
            typeof frame.messageName === 'string' && frame.messageName.trim().length > 0 ||
            typeof frame.frame?.n === 'string' && frame.frame.n.trim().length > 0
          )
        );
      })
      .map((frame) => {
        const dbcFile = frame.dbcFile!.trim();
        const canId = frame.canId!.trim();
        const messageName = (frame.messageName || frame.frame?.n || '').trim();

        return {
          dbcFile,
          canId,
          messageName,
          label: `${canId} · ${messageName} · ${dbcFile}`
        };
      })
      .sort((a, b) => {
        const dbcCompare = a.dbcFile.localeCompare(b.dbcFile);

        if (dbcCompare !== 0) {
          return dbcCompare;
        }

        return a.canId.localeCompare(b.canId);
      });
  }

  private getDevelopmentResolvedCanFrames(
    manifest: DevelopmentRunManifest
  ): DevelopmentRunManifestResolvedCanFrame[] {
    const resolvedFrames = manifest.dbc?.resolvedCanFrames ?? [];

    if (Array.isArray(resolvedFrames) && resolvedFrames.length > 0) {
      return resolvedFrames;
    }

    const canFrames = manifest.dbc?.canFrames ?? [];

    return Array.isArray(canFrames)
      ? canFrames
      : [];
  }

  private async loadValidatedDbcFiles(folderCatalogUrl: string): Promise<string[]> {
    const url = new URL(folderCatalogUrl);

    url.searchParams.set('customerId', this.customerId);
    url.searchParams.set('status', 'validated');

    const headers: Record<string, string> = {
      'Content-Type': 'application/json'
    };

    const authorizationToken = await this.getAuthorizationToken();

    if (authorizationToken) {
      headers['Authorization'] = authorizationToken;
    }

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers,
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error(
        `Unable to load validated DBC files. HTTP ${response.status} ${response.statusText || ''}`.trim()
      );
    }

    const body = await response.json() as {
      folderName?: string;
      files?: Array<{
        name?: string;
        status?: string;
      }>;
    };

    if (!Array.isArray(body.files)) {
      throw new Error('Invalid DBC list response. Expected files array.');
    }

    return body.files
      .filter((file) => file.status === 'validated' && typeof file.name === 'string')
      .map((file) => file.name as string)
      .sort((a, b) => a.localeCompare(b));
  }

  private async loadCanFrameCatalog(getDbcCanIdsUrl: string): Promise<CanFrameOption[]> {
    const url = new URL(getDbcCanIdsUrl);

    url.searchParams.set('customerId', this.customerId);

    const headers: Record<string, string> = {
      'Content-Type': 'application/json'
    };

    const authorizationToken = await this.getAuthorizationToken();

    if (authorizationToken) {
      headers['Authorization'] = authorizationToken;
    }

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers,
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error(
        `Unable to load CAN frame catalog. HTTP ${response.status} ${response.statusText || ''}`.trim()
      );
    }

    const body = await response.json() as {
      frames?: Array<{
        dbcFile?: string;
        canId?: string;
        messageName?: string;
      }>;
    };

    if (!Array.isArray(body.frames)) {
      throw new Error('Invalid CAN frame catalog response. Expected frames array.');
    }

    return body.frames
      .filter((frame) => {
        return (
          typeof frame.dbcFile === 'string' &&
          typeof frame.canId === 'string' &&
          typeof frame.messageName === 'string'
        );
      })
      .map((frame) => {
        const dbcFile = frame.dbcFile as string;
        const canId = frame.canId as string;
        const messageName = frame.messageName as string;

        return {
          dbcFile,
          canId,
          messageName,
          label: `${canId} · ${messageName} · ${dbcFile}`
        };
      })
      .sort((a, b) => {
        const dbcCompare = a.dbcFile.localeCompare(b.dbcFile);

        if (dbcCompare !== 0) {
          return dbcCompare;
        }

        return a.canId.localeCompare(b.canId);
      });
  }

  private updateCanFrameOptionsFromSelectedDbcs(): void {
    const selectedDbcFiles = new Set(this.form.controls.dbcFiles.value);

    const filteredFrames = this.canFrameCatalog.filter((frame) =>
      selectedDbcFiles.has(frame.dbcFile)
    );

    this.canFrameOptions = filteredFrames.map((frame) => frame.label);

    const availableLabels = new Set(this.canFrameOptions);
    const selectedFrames = this.form.controls.canFrames.value;

    const nextSelectedFrames = selectedFrames.filter((frame) =>
      availableLabels.has(frame)
    );

    if (nextSelectedFrames.length === selectedFrames.length) {
      this.form.controls.canFrames.updateValueAndValidity({ emitEvent: false });
      return;
    }

    this.form.controls.canFrames.setValue(nextSelectedFrames, { emitEvent: false });
    this.form.controls.canFrames.updateValueAndValidity({ emitEvent: false });
  }

  private buildEngineEnvelope() {
    const raw = this.form.getRawValue();

    const selectedCanFrames = raw.canFrames.map((frameLabel) => {
      const frame = this.canFrameCatalog.find((item) => item.label === frameLabel);

      if (!frame) {
        return {
          dbcFile: '',
          canId: frameLabel,
          messageName: ''
        };
      }

      return {
        dbcFile: frame.dbcFile,
        canId: frame.canId,
        messageName: frame.messageName
      };
    });

    return {
      amountOfVehicles: Number(raw.amountOfVehicles),
      amountOfTime: Number(raw.amountOfTime),
      generationType: raw.generationTypeAllAtOnce ? 'all_at_once' : 'over_time',
      numberOfBlocks: Number(raw.numberOfBlocks),
      blocksSize: Number(raw.sizeOfBlocksBytes),
      gpsArea: raw.gpsArea,
      gpsCoordinates: this.compressSequentialGpsCoordinates(
        this.interpGpsCoords.length > 0
          ? this.interpGpsCoords
          : this.selectedGpsCoordinates
      ),
      canFrames: selectedCanFrames,
      dbcFiles: raw.dbcFiles,
      vinPrefix: raw.vinPrefix,
      vinSuffix: raw.vinSuffix,
      initialDateTime: raw.initialDateTime,
      latencyTime: Number(raw.latencyTime),
      speed: Number(raw.speed),
      unity: raw.unity === 'Mi' ? 'Mi' : 'Km',
      simulationMode: raw.simulationMode,
      driverProfile: String(raw.driverProfile || '').trim(),
      outputFormat: raw.outputFormat,
      s3Bucket: raw.s3Bucket.trim(),
      workQueueUrl: raw.workQueueUrl.trim()
    };
  }

  private updatePayloadPreview(): void {
    const envelope = this.buildEngineEnvelope();
    this.setPayloadValue(JSON.stringify(envelope, null, 2));
    this.form.controls.payload.markAsTouched();
  }

  private buildSequentialGpsHexCoordinates(route: RoutePayload): string[] {
    const orderedPoints: RoutePoint[] = [];

    if (this.isValidRoutePoint(route.start)) {
      orderedPoints.push(route.start);
    }

    const orderedWaypointKeys = Object.keys(route.waypoints ?? {}).sort(
      (a, b) => Number(a) - Number(b)
    );

    for (const key of orderedWaypointKeys) {
      const point = route.waypoints[key];
      if (this.isValidRoutePoint(point)) {
        orderedPoints.push(point);
      }
    }

    if (this.isValidRoutePoint(route.destination)) {
      orderedPoints.push(route.destination);
    }

    return orderedPoints.map((point) => this.encodeGpsPointToHex(point));
  }

  private isValidRoutePoint(value: unknown): value is RoutePoint {
    if (!value || typeof value !== 'object') {
      return false;
    }

    const point = value as Record<string, unknown>;

    return (
      typeof point['lat'] === 'number' &&
      Number.isFinite(point['lat']) &&
      typeof point['lng'] === 'number' &&
      Number.isFinite(point['lng'])
    );
  }

  private encodeGpsPointToHex(point: RoutePoint): string {
    const latScaled = Math.round(point.lat * 1_000_000);
    const lngScaled = Math.round(point.lng * 1_000_000);

    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);

    view.setInt32(0, latScaled, false);
    view.setInt32(4, lngScaled, false);

    return Array.from(new Uint8Array(buffer))
      .map((byte) => byte.toString(16).padStart(2, '0'))
      .join('')
      .toUpperCase();
  }

  private decodeGpsHexToRoutePoint(hex: string): RoutePoint | null {
    if (typeof hex !== 'string') {
      return null;
    }

    const normalized = hex.trim().toUpperCase();

    if (!/^[0-9A-F]{16}$/.test(normalized)) {
      return null;
    }

    try {
      const bytes = new Uint8Array(
        normalized.match(/.{1,2}/g)!.map((value) => parseInt(value, 16))
      );

      const view = new DataView(bytes.buffer);

      const latScaled = view.getInt32(0, false);
      const lngScaled = view.getInt32(4, false);

      const lat = latScaled / 1_000_000;
      const lng = lngScaled / 1_000_000;

      if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        return null;
      }

      if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
        return null;
      }

      return {
        lat,
        lng,
        label: `${lat.toFixed(6)}, ${lng.toFixed(6)}`
      };
    } catch {
      return null;
    }
  }

  private rebuildRoutePayloadFromHexCoordinates(hexCoordinates: string[]): RoutePayload | null {
    const decodedPoints = hexCoordinates
      .map((hex) => this.decodeGpsHexToRoutePoint(hex))
      .filter((point): point is RoutePoint => point !== null);

    if (decodedPoints.length === 0) {
      return null;
    }

    if (decodedPoints.length === 1) {
      return {
        start: decodedPoints[0],
        waypoints: {},
        destination: null
      };
    }

    const start = decodedPoints[0];
    const destination = decodedPoints[decodedPoints.length - 1];
    const middlePoints = decodedPoints.slice(1, -1);

    const waypoints: Record<string, RoutePoint> = {};

    middlePoints.forEach((point, index) => {
      waypoints[String(index + 1)] = point;
    });

    return {
      start,
      waypoints,
      destination
    };
  }

  private jsonValidator(control: AbstractControl<string>): ValidationErrors | null {
    const value = control.value?.trim();
    if (!value) {
      return null;
    }

    try {
      JSON.parse(value);
      return null;
    } catch {
      return { jsonInvalid: true };
    }
  }

  private async parseResponseBody(response: Response): Promise<unknown> {
    const contentType = (response.headers.get('content-type') ?? '').toLowerCase();
    const text = await response.text();

    if (!text) {
      return null;
    }

    if (contentType.includes('application/json')) {
      try {
        return JSON.parse(text);
      } catch {
        return {
          rawBody: text,
          parseError: 'Response declared application/json but returned invalid JSON.'
        };
      }
    }

    const trimmed = text.trim();
    if (
      (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
      (trimmed.startsWith('[') && trimmed.endsWith(']'))
    ) {
      try {
        return JSON.parse(trimmed);
      } catch {
        return text;
      }
    }

    return text;
  }

  private serializeHeaders(headers: Headers): Record<string, string> {
    const result: Record<string, string> = {};
    headers.forEach((value, key) => {
      result[key] = value;
    });
    return result;
  }

  private describeFetchError(error: unknown): Record<string, unknown> {
    if (error instanceof Error) {
      return {
        name: error.name,
        message: error.message,
        stack: error.stack ?? null
      };
    }

    return {
      name: 'UnknownError',
      message: String(error)
    };
  }

  private openGenerationModal(title: string, message: string, details = ''): void {
    this.generationWorkflowState = 'running';
    this.generationWorkflowTitle = title;
    this.generationWorkflowMessage = message;
    this.generationWorkflowDetails = '';

    this.ensureGenerationWorkflowDialogOpen();
  }

  private closeGenerationModal(): void {
    this.closeGenerationWorkflowDialogOnly();
  }

  closeGenerationWorkflowDialog(): void {
    if (this.generationWorkflowState === 'running') {
      return;
    }

    this.closeGenerationWorkflowDialogOnly();
    this.resetGenerationWorkflowDialog();
  }

  private openGenerationSuccessModal(title: string, message: string, details = ''): void {
    this.generationWorkflowState = 'success';
    this.generationWorkflowTitle = title;
    this.generationWorkflowMessage = message;
    this.generationWorkflowDetails = '';

    this.ensureGenerationWorkflowDialogOpen();
  }

  private openGenerationErrorModal(title: string, message: string, details = ''): void {
    this.generationWorkflowState = 'error';
    this.generationWorkflowTitle = title;
    this.generationWorkflowMessage = message;
    this.generationWorkflowDetails = '';

    this.ensureGenerationWorkflowDialogOpen();
  }

  private ensureGenerationWorkflowDialogOpen(): void {
    if (!this.generationWorkflowDialog || this.generationWorkflowDialogRef) {
      return;
    }

    this.generationWorkflowDialogRef = this.dialog.open(this.generationWorkflowDialog, {
      width: '440px',
      panelClass: 'trackster-admin-workflow-dialog-panel',
      disableClose: true
    });
  }

  private closeGenerationWorkflowDialogOnly(): void {
    if (!this.generationWorkflowDialogRef) {
      return;
    }

    this.generationWorkflowDialogRef.close();
    this.generationWorkflowDialogRef = undefined;
  }

  private resetGenerationWorkflowDialog(): void {
    this.generationWorkflowState = 'idle';
    this.generationWorkflowTitle = '';
    this.generationWorkflowMessage = '';
    this.generationWorkflowDetails = '';
    this.generationWorkflowProgress = 0;
  }

  private logGenerationDuration(startedAtMs: number, details: Record<string, unknown>): void {
    const durationMs = Math.max(0, performance.now() - startedAtMs);
    const durationSeconds = Number((durationMs / 1000).toFixed(3));

    console.log('[SIMULATOR] Generation completed.', {
      durationMs: Math.round(durationMs),
      durationSeconds,
      ...details
    });
  }

  private async waitForScenarioCompletion(monitor: GenerationMonitor): Promise<ScenarioStatusDocument> {
    for (let attempt = 1; attempt <= this.maxGenerationPollAttempts; attempt += 1) {
      const exists = await this.scenarioObjectExists(monitor.bucket, monitor.scenarioKey);

      if (!exists) {
        this.generationWorkflowTitle = 'Preparing signal analysis...';
        this.generationWorkflowMessage = 'Waiting for Trackster to initialize the signal analysis.';
        this.generationWorkflowDetails = '';
        this.generationWorkflowProgress = 0;
        await this.delay(this.generationPollIntervalMs);
        continue;
      }

      const scenarioDocument = await this.readScenarioJson(monitor.bucket, monitor.scenarioKey);
      this.applyScenarioDocumentToWorkflow(scenarioDocument, monitor);

      if (this.isScenarioFailed(scenarioDocument)) {
        throw new Error(this.buildScenarioFailureMessage(scenarioDocument));
      }

      if (this.isScenarioCompleted(scenarioDocument)) {
        return scenarioDocument;
      }

      await this.delay(this.generationPollIntervalMs);
    }

    throw new Error('Signal analysis status check timed out before the signals were saved.');
  }

  private async scenarioObjectExists(bucket: string, key: string): Promise<boolean> {
    const session = await fetchAuthSession();

    if (!session.credentials) {
      throw new Error('Unable to retrieve AWS credentials for S3 scenario status check.');
    }

    const s3Client = new S3Client({
      region: this.s3Region,
      credentials: session.credentials
    });

    const normalizedKey = this.normalizeS3Prefix(key);
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: normalizedKey,
      MaxKeys: 1
    }));

    return (response.Contents ?? []).some((object) => object.Key === normalizedKey && (object.Size ?? 0) > 0);
  }

  private async readScenarioJson(bucket: string, key: string): Promise<ScenarioStatusDocument> {
    const session = await fetchAuthSession();

    if (!session.credentials) {
      throw new Error('Unable to retrieve AWS credentials for S3 scenario status check.');
    }

    const s3Client = new S3Client({
      region: this.s3Region,
      credentials: session.credentials
    });

    const response = await s3Client.send(new GetObjectCommand({
      Bucket: bucket,
      Key: this.normalizeS3Prefix(key)
    }));

    const text = await this.s3BodyToText(response.Body);

    if (!text.trim()) {
      throw new Error(`scenario.json is empty: s3://${bucket}/${key}`);
    }

    return JSON.parse(text) as ScenarioStatusDocument;
  }

  private async s3BodyToText(body: unknown): Promise<string> {
    if (!body) {
      return '';
    }

    if (typeof body === 'string') {
      return body;
    }

    if (body instanceof Blob) {
      return await body.text();
    }

    if (body instanceof Uint8Array) {
      return new TextDecoder('utf-8').decode(body);
    }

    const candidate = body as {
      transformToString?: (encoding?: string) => Promise<string>;
      getReader?: () => ReadableStreamDefaultReader<Uint8Array>;
    };

    if (typeof candidate.transformToString === 'function') {
      return await candidate.transformToString('utf8');
    }

    if (typeof candidate.getReader === 'function') {
      const reader = candidate.getReader();
      const chunks: Uint8Array[] = [];

      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        if (value) {
          chunks.push(value);
        }
      }

      const totalLength = chunks.reduce((total, chunk) => total + chunk.length, 0);
      const merged = new Uint8Array(totalLength);
      let offset = 0;

      for (const chunk of chunks) {
        merged.set(chunk, offset);
        offset += chunk.length;
      }

      return new TextDecoder('utf-8').decode(merged);
    }

    return String(body);
  }

  private applyScenarioDocumentToWorkflow(
    scenarioDocument: ScenarioStatusDocument,
    monitor: GenerationMonitor
  ): void {
    void monitor;

    this.generationWorkflowProgress = this.resolveScenarioPercent(scenarioDocument);
    this.generationWorkflowTitle = this.resolveScenarioTitle(scenarioDocument);
    this.generationWorkflowMessage = this.resolveScenarioMessage(scenarioDocument);
    this.generationWorkflowDetails = '';

    if (this.isScenarioFailed(scenarioDocument)) {
      this.generationWorkflowState = 'error';
    }
  }

  private resolveScenarioPercent(scenarioDocument: ScenarioStatusDocument): number {
    const progress = scenarioDocument.progress || {};
    const totalBatches = Math.max(0, Number(progress.totalPhases || 0));
    const completedBatches = Math.max(0, Number(progress.completedPhases || 0));

    if (this.isScenarioCompleted(scenarioDocument)) {
      return 100;
    }

    if (totalBatches <= 0) {
      return 0;
    }

    return Math.min(100, Math.max(0, Math.round((completedBatches / totalBatches) * 100)));
  }

  private resolveScenarioTitle(scenarioDocument: ScenarioStatusDocument): string {
    if (this.isScenarioFailed(scenarioDocument)) {
      return 'Signal generation failed';
    }

    if (this.isScenarioCompleted(scenarioDocument)) {
      return 'Signals generated successfully.';
    }

    const totalBatches = Math.max(0, Number(scenarioDocument.progress?.totalPhases || 0));

    return totalBatches > 0
      ? 'Generating signals...'
      : 'Preparing signal analysis...';
  }

  private resolveScenarioMessage(scenarioDocument: ScenarioStatusDocument): string {
    if (this.isScenarioFailed(scenarioDocument)) {
      return 'Trackster stopped signal generation because an error occurred.';
    }

    if (this.isScenarioCompleted(scenarioDocument)) {
      return 'The selected signals were generated and saved successfully.';
    }

    const progress = scenarioDocument.progress || {};
    const totalBatches = Math.max(0, Number(progress.totalPhases || 0));
    const currentBatch = Math.max(
      0,
      Number(progress.currentPhase || 0),
      Math.min(totalBatches, Number(progress.completedPhases || 0) + 1)
    );

    if (totalBatches > 0 && currentBatch > 0) {
      return `Generating signals batch ${Math.min(currentBatch, totalBatches)}/${totalBatches}`;
    }

    return 'Trackster is preparing the selected CAN signals for analysis.';
  }

  private buildScenarioDetails(
    scenarioDocument: ScenarioStatusDocument,
    monitor: GenerationMonitor
  ): string {
    void scenarioDocument;
    void monitor;
    return '';
  }

  private isScenarioCompleted(scenarioDocument: ScenarioStatusDocument): boolean {
    const normalizedStatus = String(scenarioDocument.status || '').toLowerCase().trim();
    const progress = scenarioDocument.progress || {};
    const totalBatches = Math.max(0, Number(progress.totalPhases || 0));
    const completedBatches = Math.max(0, Number(progress.completedPhases || 0));
    const failedBatches = Math.max(0, Number(progress.failedPhases || 0));
    const processingBatches = Math.max(0, Number(progress.processingPhases || 0));

    if (
      normalizedStatus === 'completed' ||
      normalizedStatus === 'signals_saved' ||
      normalizedStatus === 'signal_knowledge_saved' ||
      normalizedStatus === 'signal_knowledge_completed'
    ) {
      return failedBatches === 0 && processingBatches === 0;
    }

    return (
      totalBatches > 0 &&
      completedBatches >= totalBatches &&
      failedBatches === 0 &&
      processingBatches === 0
    );
  }

  private isScenarioBehaviorCompleted(scenarioDocument?: ScenarioStatusDocument): boolean {
    const progress = scenarioDocument?.progress || {};
    const totalPhases = Number(progress.totalPhases || 0);
    const completedPhases = Number(progress.completedPhases || 0);
    const failedPhases = Number(progress.failedPhases || 0);
    const processingPhases = Number(progress.processingPhases || 0);
    const percent = Number(progress.percent || 0);
    const behaviorPhases = scenarioDocument?.behavior?.phases || [];
    const completedBehaviorPhases = behaviorPhases.filter((phase) =>
      String(phase?.status || '').toLowerCase() === 'completed'
    ).length;
    const status = String(scenarioDocument?.status || '').toLowerCase().trim();

    return (
      totalPhases > 0 &&
      completedPhases >= totalPhases &&
      completedBehaviorPhases >= totalPhases &&
      failedPhases === 0 &&
      processingPhases === 0 &&
      percent >= 100 &&
      (
        status === 'all behavior phases completed.' ||
        status === 'behavior_completed' ||
        status === 'ai_behavior_completed' ||
        status.includes('behavior phases completed') ||
        status.includes('behavior completed')
      )
    );
  }

  private isBehaviorWorkflow(scenarioDocument?: ScenarioStatusDocument, normalizedStatus = ''): boolean {
    const progress = scenarioDocument?.progress || {};
    const hasScenarioPlan = Array.isArray(scenarioDocument?.scenario?.phases) && scenarioDocument.scenario.phases.length > 0;

    return (
      String(progress.stage || '').toLowerCase() === 'behavior' ||
      normalizedStatus.includes('behavior') ||
      normalizedStatus.includes('behaviour') ||
      normalizedStatus.includes('waiting for behavior phase generation') ||
      hasScenarioPlan
    );
  }

  private isScenarioFailed(scenarioDocument: ScenarioStatusDocument): boolean {
    const progress = scenarioDocument.progress || {};
    const hasPhaseError = !!scenarioDocument.behavior?.phases?.some((phase) => !!phase?.error);

    return (
      this.isFailedStatus(String(scenarioDocument.status || '')) ||
      Number(progress.failedPhases || 0) > 0 ||
      hasPhaseError ||
      !!scenarioDocument.error
    );
  }

  private isCompletedStatus(status: string): boolean {
    const normalizedStatus = status.toLowerCase().trim();

    return (
      normalizedStatus === 'completed' ||
      normalizedStatus === 'success' ||
      normalizedStatus === 'done' ||
      normalizedStatus === 'finished' ||
      normalizedStatus === 'bin_completed' ||
      normalizedStatus === 'generation_completed' ||
      normalizedStatus === 'worker_completed' ||
      normalizedStatus === 'simulation_completed' ||
      normalizedStatus.endsWith('_completed')
    );
  }

  private isFailedStatus(status: string): boolean {
    const normalizedStatus = status.toLowerCase();
    return normalizedStatus.includes('failed') || normalizedStatus.includes('error');
  }

  private buildScenarioFailureMessage(scenarioDocument: ScenarioStatusDocument): string {
    const phaseError = scenarioDocument.behavior?.phases
      ?.find((phase) => phase?.error?.message)
      ?.error?.message;

    if (phaseError) {
      return phaseError;
    }

    if (scenarioDocument.error?.message) {
      return scenarioDocument.error.message;
    }

    const latestError = [...(scenarioDocument.logs || [])]
      .reverse()
      .find((entry) => String(entry.level || '').toLowerCase() === 'error');

    return latestError?.message || scenarioDocument.currentStep || scenarioDocument.status || 'Simulation generation failed.';
  }

  private buildScenarioSuccessTitle(scenarioDocument: ScenarioStatusDocument): string {
    return 'Signals analyzed successfully.';
  }

  private buildScenarioSuccessMessage(scenarioDocument: ScenarioStatusDocument, monitor: GenerationMonitor): string {
    return 'The selected signals were analyzed and saved successfully in the scenario.';
  }

  private buildGeneratedFilesSuccessMessage(generatedFiles: number): string {
    const safeGeneratedFiles = Number.isFinite(generatedFiles)
      ? Math.max(0, Math.floor(generatedFiles))
      : 0;

    const fileWord = safeGeneratedFiles === 1 ? 'file was' : 'files were';

    return `${safeGeneratedFiles} simulation ${fileWord} generated successfully.`;
  }

  private resolveGeneratedBinCountFromScenario(scenarioDocument: ScenarioStatusDocument): number | null {
    const value =
      scenarioDocument.progress?.generatedBinCount ??
      scenarioDocument.output?.generatedBinCount;

    if (typeof value === 'number' && Number.isFinite(value)) {
      return Math.max(0, Math.floor(value));
    }

    return null;
  }

  private resolveExpectedBinCountFromScenario(scenarioDocument: ScenarioStatusDocument, monitor: GenerationMonitor): number {
    const value =
      scenarioDocument.progress?.expectedBinCount ??
      scenarioDocument.output?.expectedBinCount ??
      monitor.expectedBinCount;

    if (typeof value === 'number' && Number.isFinite(value)) {
      return Math.max(0, Math.floor(value));
    }

    return monitor.expectedBinCount;
  }

  private resolveGenerationMonitor(responseBody: unknown, requestBody: ReturnType<typeof this.buildEngineEnvelope>): GenerationMonitor | null {
    const bucket =
      this.extractFirstString(responseBody, ['bucket', 'bucketName', 's3Bucket', 'outputBucket', 'scenarioBucketName']) ||
      requestBody.s3Bucket;

    const scenarioKey = this.extractFirstString(responseBody, ['scenarioKey', 'scenarioJsonKey']);
    const runFolder =
      this.extractFirstString(responseBody, ['runFolder', 'outputFolder', 'destinationPrefix']) ||
      this.extractPrefixFromKey(scenarioKey);

    const expectedBinCount =
      this.extractFirstNumber(responseBody, [
        'expectedBinCount',
        'expectedFiles',
        'expectedFileCount',
        'expectedVehicles',
        'enqueued_vehicles',
        'enqueuedVehicles',
        'binCount',
        'fileCount',
        'amountOfVehicles'
      ]) ?? Number(requestBody.amountOfVehicles);

    if (!bucket || !scenarioKey || !runFolder || !Number.isFinite(expectedBinCount) || expectedBinCount <= 0) {
      return null;
    }

    return {
      bucket: bucket.trim(),
      scenarioKey: this.normalizeS3Prefix(scenarioKey),
      runFolder: this.normalizeS3Prefix(runFolder),
      expectedBinCount: Math.max(1, Math.floor(expectedBinCount))
    };
  }

  private extractFirstString(value: unknown, keys: string[]): string | null {
    const match = this.findFirstValueByKey(value, new Set(keys));

    if (typeof match === 'string' && match.trim()) {
      return match.trim();
    }

    return null;
  }

  private extractFirstNumber(value: unknown, keys: string[]): number | null {
    const match = this.findFirstValueByKey(value, new Set(keys));

    if (typeof match === 'number' && Number.isFinite(match)) {
      return match;
    }

    if (typeof match === 'string' && match.trim() && Number.isFinite(Number(match))) {
      return Number(match);
    }

    return null;
  }

  private findFirstValueByKey(value: unknown, keys: Set<string>): unknown {
    if (!value || typeof value !== 'object') {
      return null;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        const found = this.findFirstValueByKey(item, keys);
        if (found !== null && found !== undefined) {
          return found;
        }
      }
      return null;
    }

    const record = value as Record<string, unknown>;

    for (const key of Object.keys(record)) {
      if (keys.has(key)) {
        return record[key];
      }
    }

    for (const key of Object.keys(record)) {
      const found = this.findFirstValueByKey(record[key], keys);
      if (found !== null && found !== undefined) {
        return found;
      }
    }

    return null;
  }

  private extractPrefixFromKey(key: string | null): string | null {
    if (!key) {
      return null;
    }

    const normalized = key.replace(/^\/+/, '');
    const lastSlashIndex = normalized.lastIndexOf('/');

    if (lastSlashIndex < 0) {
      return '';
    }

    return normalized.slice(0, lastSlashIndex + 1);
  }

  private normalizeS3Prefix(prefix: string): string {
    return prefix.replace(/^\/+/, '');
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
  }

  private describeHttpStatus(status: number): string {
    if (status >= 500) {
      return 'Server error. The API received the request but failed while processing it.';
    }
    if (status === 404) {
      return 'Endpoint not found. Check the Engine URL path.';
    }
    if (status === 401 || status === 403) {
      return 'Authentication/authorization error. The API denied this request.';
    }
    if (status >= 400) {
      return 'Client/request error. Validate request payload and required headers.';
    }
    return 'Unexpected HTTP status.';
  }

  private revalidateAllControls(): void {
    Object.values(this.form.controls).forEach((control) => {
      control.updateValueAndValidity({ onlySelf: true, emitEvent: false });
    });
    this.form.updateValueAndValidity({ emitEvent: false });
  }

  private clearPayload(): void {
    if (!this.form.controls.payload.value) {
      return;
    }

    this.setPayloadValue('');
    this.form.controls.payload.markAsUntouched();
  }

  private setPayloadValue(value: string): void {
    this.suppressFormReset = true;
    this.form.controls.payload.setValue(value, { emitEvent: false });
    this.suppressFormReset = false;
  }

  private getCurrentDateTimeLocal(): string {
    const now = new Date();
    now.setSeconds(0, 0);

    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    return `${year}-${month}-${day}T${hours}:${minutes}`;
  }

  private makeGenerationTimestamp(): string {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');

    return `${year}${month}${day}T${hours}${minutes}${seconds}`;
  }

  private openTracksterDialog<T>(
    component: ComponentType<T>,
    options?: {
      data?: unknown;
      width?: string;
      height?: string;
      maxWidth?: string;
      maxHeight?: string;
      panelClass?: string;
      backdropClass?: string;
    }
  ): MatDialogRef<T> {
    return this.dialog.open(component, {
      data: options?.data,
      width: options?.width ?? '1040px',
      height: options?.height ?? '82vh',
      maxWidth: options?.maxWidth ?? '95vw',
      maxHeight: options?.maxHeight ?? '90vh',
      panelClass: options?.panelClass ?? 'trackster-dialog',
      backdropClass: options?.backdropClass ?? 'trackster-dialog-backdrop',
      autoFocus: false,
      restoreFocus: true
    });
  }

  private compressSequentialGpsCoordinates(coordinates: string[]): GpsCoordinateRle[] {
    const compressed: GpsCoordinateRle[] = [];

    for (const coordinate of coordinates) {
      const last = compressed[compressed.length - 1];

      if (last && last[0] === coordinate) {
        last[1] += 1;
        continue;
      }

      compressed.push([coordinate, 1]);
    }

    return compressed;
  }
}
