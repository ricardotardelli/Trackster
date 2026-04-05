import { GpsplotterComponent } from './gpsplotter/gpsplotter.component';
import { interpolateGpsPerBlock } from './interpmodule/interpmodule.util';
import { CommonModule } from '@angular/common';
import {
  AbstractControl,
  FormBuilder,
  FormsModule,
  ReactiveFormsModule,
  ValidationErrors,
  Validators
} from '@angular/forms';
import { MapmoduleComponent } from './mapmodule/mapmodule.component';
import {
  AfterViewChecked,
  Component,
  ElementRef,
  HostListener,
  OnDestroy,
  OnInit,
  ViewChild
} from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { fetchAuthSession, getCurrentUser } from 'aws-amplify/auth';

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

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
    MapmoduleComponent,
    GpsplotterComponent,
    RouterOutlet
  ],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnInit, AfterViewChecked, OnDestroy {
  constructor(
    private readonly fb: FormBuilder,
    private readonly elementRef: ElementRef<HTMLElement>
  ) {}

  authReady = false;
  isAuthenticated = false;

  @ViewChild('modalHeader', { static: false })
  private modalHeader?: ElementRef<HTMLDivElement>;

  @ViewChild('mapModal', { static: false })
  private mapModal?: ElementRef<HTMLDivElement>;

  @ViewChild('mapModule')
  mapModule!: MapmoduleComponent;

  routeDataForMap: RoutePayload | null = null;

  gpsAreas: string[] = [];
  canFrameOptions: string[] = [];
  dbcOptions: string[] = [];
  selectedGpsCoordinates: string[] = [];
  interpGpsCoords: string[] = [];

  isGpsOpen = false;
  gpsFilter = '';
  isCanOpen = false;
  isDbcOpen = false;
  isSubmitting = false;
  isConfigLoaded = false;
  formStatus: 'pending' | 'awaiting_response' | 'generated' | 'error' = 'pending';
  generationTimestamp = '';
  copyPayloadState: 'idle' | 'copied' | 'error' = 'idle';
  private suppressFormReset = false;
  private formValueChangesBound = false;

  isMapModalOpen = false;
  isGpsPlotterModalOpen = false;
  gpsPlotterHexCoordinates: string[] = [];

  private dragInitialized = false;
  private isDragging = false;
  private dragOffsetX = 0;
  private dragOffsetY = 0;
  private boundMouseMove?: (event: MouseEvent) => void;
  private boundMouseUp?: () => void;

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
    unity: ['Km' as 'Km' | 'Mi', [Validators.required]],
    s3Bucket: ['', [Validators.required]],
    workQueueUrl: [''],
    engineUrl: ['', [Validators.required, Validators.pattern(/^https?:\/\/.+/i)]],
    payload: ['', [this.jsonValidator]]
  });

  ngOnInit(): void {
    void this.initializeApp();
  }

  ngAfterViewChecked(): void {
    if (this.isMapModalOpen || this.isGpsPlotterModalOpen) {
      this.initializeModalDrag();
    }
  }

  ngOnDestroy(): void {
    this.removeDragListeners();
  }

  get f() {
    return this.form.controls;
  }

  get gpsSummary(): string {
    return this.form.controls.gpsArea.value || 'Select a region';
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
    if (this.formStatus === 'generated') {
      return 'Generated';
    }
    if (this.formStatus === 'error') {
      return 'Error';
    }
    return 'Pending';
  }

  openMapModal(): void {
    this.routeDataForMap = this.rebuildRoutePayloadFromHexCoordinates(this.selectedGpsCoordinates);
    this.isMapModalOpen = true;
  }

  openMap(event: MouseEvent): void {
    event.stopPropagation();
    event.preventDefault();

    this.routeDataForMap = this.rebuildRoutePayloadFromHexCoordinates(this.selectedGpsCoordinates);

    this.isMapModalOpen = true;
    this.isGpsOpen = false;
    this.dragInitialized = false;
  }

  closeMapModal(): void {
    this.isMapModalOpen = false;
    this.isDragging = false;
    document.body.style.userSelect = '';
    this.dragInitialized = false;
  }

  toggleCanOpen(): void {
    if (!this.isConfigLoaded) {
      return;
    }

    this.isCanOpen = !this.isCanOpen;
    if (this.isCanOpen) {
      this.isGpsOpen = false;
      this.isDbcOpen = false;
    }
  }

  toggleDbcOpen(): void {
    if (!this.isConfigLoaded) {
      return;
    }

    this.isDbcOpen = !this.isDbcOpen;
    if (this.isDbcOpen) {
      this.isGpsOpen = false;
      this.isCanOpen = false;
    }
  }

  toggleGpsOpen(): void {
    if (!this.isConfigLoaded) {
      return;
    }

    this.isGpsOpen = !this.isGpsOpen;
    if (this.isGpsOpen) {
      this.gpsFilter = '';
      this.isCanOpen = false;
      this.isDbcOpen = false;
    }
  }

  selectGpsArea(area: string): void {
    this.form.controls.gpsArea.setValue(area);
    this.form.controls.gpsArea.markAsTouched();
    this.gpsFilter = '';
    this.isGpsOpen = false;
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
  }

  selectAllCanFrames(): void {
    this.form.controls.canFrames.setValue([...this.canFrameOptions]);
    this.form.controls.canFrames.markAsTouched();
  }

  clearAllCanFrames(): void {
    this.form.controls.canFrames.setValue([]);
    this.form.controls.canFrames.markAsTouched();
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

  private async initializeApp(): Promise<void> {
    this.bindFormValueChangesOnce();

    try {
      await this.ensureAuthenticatedSession();
      this.isAuthenticated = true;

      await this.loadConfig();
      this.isConfigLoaded = true;
      this.authReady = true;
    } catch (error: unknown) {
      this.isAuthenticated = false;
      this.formStatus = 'error';
      this.setPayloadValue(JSON.stringify({
        error: {
          category: 'app_initialization_error',
          ...this.describeFetchError(error)
        }
      }, null, 2));
      this.form.controls.payload.markAsTouched();
      this.authReady = true;
    }
  }

  private async ensureAuthenticatedSession(): Promise<void> {
    await getCurrentUser();

    const session = await fetchAuthSession();
    const idToken = session.tokens?.idToken?.toString();

    if (!idToken) {
      throw new Error('Authenticated session does not contain an ID token.');
    }
  }

  private async getAuthorizationToken(): Promise<string> {
    const session = await fetchAuthSession();

    const idToken = session.tokens?.idToken?.toString();
    if (idToken) {
      return idToken;
    }

    const accessToken = session.tokens?.accessToken?.toString();
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

  async copyPayload(): Promise<void> {
    const payload = this.form.controls.payload.value;
    if (!payload) {
      this.copyPayloadState = 'error';
      return;
    }

    try {
      await navigator.clipboard.writeText(payload);
      this.copyPayloadState = 'copied';
    } catch {
      this.copyPayloadState = 'error';
    }

    window.setTimeout(() => {
      this.copyPayloadState = 'idle';
    }, 1500);
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

    this.generationTimestamp = this.makeGenerationTimestamp();
    this.formStatus = 'awaiting_response';
    this.isSubmitting = true;

    try {
      const authorizationToken = await this.getAuthorizationToken();

      this.setPayloadValue(JSON.stringify(request.body, null, 2));

      // const response = await fetch(request.url, {
      //   method: request.method,
      //   headers: {
      //     'Content-Type': 'application/json',
      //     Authorization: authorizationToken
      //   },
      //   body: JSON.stringify(request.body)
      // });

      const response = {
        ok: true,
        status: 200,
        statusText: 'OK',
        headers: new Headers(),
        text: async () => JSON.stringify({ mock: true })
      } as Response;

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
        this.formStatus = 'generated';
      }

      this.setPayloadValue(JSON.stringify(result, null, 2));
    } catch (error: unknown) {
      this.formStatus = 'error';
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
    } finally {
      this.form.controls.payload.markAsTouched();
      this.isSubmitting = false;
    }
  }

  public onSaveRoute(routeJson: string): void {
    try {
      const parsed = JSON.parse(routeJson) as RoutePayload;

      this.routeDataForMap = parsed;
      this.selectedGpsCoordinates = this.buildSequentialGpsHexCoordinates(parsed);

      const raw = this.form.getRawValue();

      const explicitBlocks = Number(this.form.controls.numberOfBlocks.value);
      const latency = Number(this.form.controls.latencyTime.value);
      const amountOfTime = Number(this.form.controls.amountOfTime.value);

      const resolvedBlocks =
        explicitBlocks > 0
          ? explicitBlocks
          : Math.floor((amountOfTime * 3600) / latency);

      const speed = Number(this.form.controls.speed.value) || 80;

      const unity =
        this.form.controls.unity.value === 'Mi' ? 'Mi' : 'Km';

      this.interpGpsCoords = interpolateGpsPerBlock(
        this.selectedGpsCoordinates,
        speed,
        unity,
        latency,
        resolvedBlocks
      );
    } 
    catch {
      this.routeDataForMap = null;
      this.selectedGpsCoordinates = [];
      this.interpGpsCoords = [];
      this.updatePayloadPreview();
    }

    this.closeMapModal();
  }

  private initializeModalDrag(): void {
    if (this.dragInitialized || !this.mapModal || !this.modalHeader) {
      return;
    }

    const modal = this.mapModal.nativeElement;
    const header = this.modalHeader.nativeElement;

    header.style.cursor = 'move';
    header.style.userSelect = 'none';

    header.onmousedown = (event: MouseEvent) => {
      const target = event.target as HTMLElement | null;

      if (target?.closest('.map-modal-close')) {
        return;
      }

      event.preventDefault();

      const rect = modal.getBoundingClientRect();
      this.isDragging = true;
      this.dragOffsetX = event.clientX - rect.left;
      this.dragOffsetY = event.clientY - rect.top;

      modal.style.position = 'fixed';
      modal.style.left = `${rect.left}px`;
      modal.style.top = `${rect.top}px`;
      modal.style.transform = 'none';

      document.body.style.userSelect = 'none';
    };

    this.boundMouseMove = (event: MouseEvent) => {
      if (!this.isDragging) {
        return;
      }

      const modalWidth = modal.offsetWidth;
      const modalHeight = modal.offsetHeight;
      const viewportWidth = window.innerWidth;
      const viewportHeight = window.innerHeight;

      let nextLeft = event.clientX - this.dragOffsetX;
      let nextTop = event.clientY - this.dragOffsetY;

      nextLeft = Math.max(0, Math.min(nextLeft, viewportWidth - modalWidth));
      nextTop = Math.max(0, Math.min(nextTop, viewportHeight - modalHeight));

      modal.style.left = `${nextLeft}px`;
      modal.style.top = `${nextTop}px`;
    };

    this.boundMouseUp = () => {
      this.isDragging = false;
      document.body.style.userSelect = '';
    };

    document.addEventListener('mousemove', this.boundMouseMove);
    document.addEventListener('mouseup', this.boundMouseUp);

    this.dragInitialized = true;
  }

  private removeDragListeners(): void {
    if (this.boundMouseMove) {
      document.removeEventListener('mousemove', this.boundMouseMove);
      this.boundMouseMove = undefined;
    }

    if (this.boundMouseUp) {
      document.removeEventListener('mouseup', this.boundMouseUp);
      this.boundMouseUp = undefined;
    }

    if (this.modalHeader?.nativeElement) {
      this.modalHeader.nativeElement.onmousedown = null;
    }
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

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as Node | null;
    if (!target) {
      return;
    }

    if (!this.elementRef.nativeElement.contains(target)) {
      this.isGpsOpen = false;
      this.isCanOpen = false;
      this.isDbcOpen = false;
    }
  }

  private async loadConfig(): Promise<void> {
    const config = await this.fetchRuntimeConfig();

    if (!Array.isArray(config.gpsAreas) || config.gpsAreas.length === 0) {
      throw new Error('gpsAreas missing or empty in config.json');
    }

    if (!Array.isArray(config.canFrames) || config.canFrames.length === 0) {
      throw new Error('canFrames missing or empty in config.json');
    }

    if (!Array.isArray(config.dbcFiles) || config.dbcFiles.length === 0) {
      throw new Error('dbcFiles missing or empty in config.json');
    }

    if (typeof config.workQueueUrl !== 'string' || !config.workQueueUrl.trim()) {
      throw new Error('workQueueUrl missing or empty in config.json');
    }

    if (typeof config.s3Default !== 'string' || !config.s3Default.trim()) {
      throw new Error('s3Default missing or empty in config.json');
    }

    if (typeof config.engineURL !== 'string' || !config.engineURL.trim()) {
      throw new Error('engineURL missing or empty in config.json');
    }

    this.suppressFormReset = true;

    this.gpsAreas = [...config.gpsAreas];
    this.canFrameOptions = [...config.canFrames];
    this.dbcOptions = [...config.dbcFiles];

    this.form.controls.canFrames.setValue([...this.canFrameOptions], { emitEvent: false });
    this.form.controls.dbcFiles.setValue([...this.dbcOptions], { emitEvent: false });
    this.form.controls.workQueueUrl.setValue(config.workQueueUrl.trim(), { emitEvent: false });
    this.form.controls.s3Bucket.setValue(config.s3Default.trim(), { emitEvent: false });
    this.form.controls.engineUrl.setValue(config.engineURL.trim(), { emitEvent: false });

    this.form.controls.canFrames.updateValueAndValidity({ emitEvent: false });
    this.form.controls.dbcFiles.updateValueAndValidity({ emitEvent: false });
    this.form.controls.workQueueUrl.updateValueAndValidity({ emitEvent: false });
    this.form.controls.s3Bucket.updateValueAndValidity({ emitEvent: false });
    this.form.controls.engineUrl.updateValueAndValidity({ emitEvent: false });
    this.form.updateValueAndValidity({ emitEvent: false });

    this.suppressFormReset = false;
  }

  private async fetchRuntimeConfig(): Promise<{
    gpsAreas?: string[];
    canFrames?: string[];
    dbcFiles?: string[];
    workQueueUrl?: string;
    s3Default?: string;
    engineURL?: string;
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
          dbcFiles?: string[];
          workQueueUrl?: string;
          s3Default?: string;
          engineURL?: string;
        };
      } catch {
        
      }
    }

    throw new Error(`Unable to load runtime config from assets/config.json. Last HTTP status: ${lastStatus ?? 'unknown'}`);
  }

  private buildEngineEnvelope() {
    const raw = this.form.getRawValue();

    return {
      amountOfVehicles: Number(raw.amountOfVehicles),
      amountOfTime: Number(raw.amountOfTime),
      generationType: raw.generationTypeAllAtOnce ? 'all_at_once' : 'over_time',
      numberOfBlocks: Number(raw.numberOfBlocks),
      blocksSize: Number(raw.sizeOfBlocksBytes),
      gpsArea: raw.gpsArea,
      gpsCoordinates: this.interpGpsCoords.length > 0 ? [...this.interpGpsCoords] : [...this.selectedGpsCoordinates],
      canFrames: raw.canFrames.map((frame) => frame.split(' - ')[0].trim()),
      dbcFiles: raw.dbcFiles,
      vinPrefix: raw.vinPrefix,
      vinSuffix: raw.vinSuffix,
      initialDateTime: raw.initialDateTime,
      latencyTime: Number(raw.latencyTime),
      s3Bucket: raw.s3Bucket.trim(),
      workQueueUrl: raw.workQueueUrl.trim()
    };
  }

  private buildSequentialGpsHexCoordinates(route: RoutePayload): string[] {
    const orderedPoints: RoutePoint[] = [];

    if (this.isValidRoutePoint(route.start)) {
      orderedPoints.push(route.start);
    }

    const orderedWaypointKeys = Object.keys(route.waypoints ?? {}).sort((a, b) => Number(a) - Number(b));

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

  private updatePayloadPreview(): void {
    const envelope = this.buildEngineEnvelope();
    this.setPayloadValue(JSON.stringify(envelope, null, 2));
    this.form.controls.payload.markAsTouched();
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
    if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
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

  openGpsPlotter(): void {
    this.gpsPlotterHexCoordinates =
      this.interpGpsCoords.length > 0
        ? [...this.interpGpsCoords]
        : [...this.selectedGpsCoordinates];

    this.isGpsPlotterModalOpen = true;
    this.isGpsOpen = false;
    this.dragInitialized = false;
  }

  closeGpsPlotterModal(): void {
    this.isGpsPlotterModalOpen = false;
    this.isDragging = false;
    document.body.style.userSelect = '';
    this.dragInitialized = false;
  }
}