import { CommonModule } from '@angular/common';
import { Component, ElementRef, HostListener } from '@angular/core';
import { AbstractControl, FormBuilder, FormsModule, ReactiveFormsModule, ValidationErrors, Validators } from '@angular/forms';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent {
  constructor(
    private readonly fb: FormBuilder,
    private readonly elementRef: ElementRef<HTMLElement>
  ) {}

  gpsAreas: string[] = [];
  canFrameOptions: string[] = [];
  dbcOptions: string[] = [];
  isGpsOpen = false;
  gpsFilter = '';
  isCanOpen = false;
  isDbcOpen = false;
  isSubmitting = false;
  formStatus: 'pending' | 'awaiting_response' | 'generated' | 'error' = 'pending';
  generationTimestamp = '';
  copyPayloadState: 'idle' | 'copied' | 'error' = 'idle';
  private suppressFormReset = false;
  readonly form = this.fb.nonNullable.group({
    amountOfVehicles: [1, [Validators.required, Validators.min(0), Validators.pattern(/^\d+$/)]],
    amountOfTime: [1, [Validators.required, Validators.pattern(/^\d+(\.\d+)?$/)]],
    generationTypeAllAtOnce: [true],
    numberOfBlocks: [0, [Validators.required, Validators.min(0), Validators.pattern(/^\d+$/)]],
    sizeOfBlocksBytes: [7560, [Validators.required, Validators.pattern(/^\d+$/)]],
    gpsArea: ['', [Validators.required]],
    canFrames: [[...this.canFrameOptions], [Validators.required]],
    dbcFiles: [[...this.dbcOptions], [Validators.required]],
    vinPrefix: ['', [Validators.required, Validators.minLength(6), Validators.maxLength(6)]],
    initialDateTime: [this.getCurrentDateTimeLocal(), [Validators.required]],
    vinSuffix: ['', [Validators.required, Validators.minLength(5), Validators.maxLength(5)]],
    latencyTime: [5, [Validators.required, Validators.pattern(/^\d+$/)]],
    s3Bucket: ['', [Validators.required]],
    workQueueUrl: [''],
    engineUrl: ['', [Validators.required, Validators.pattern(/^https?:\/\/.+/i)]],
    payload: ['', [this.jsonValidator]]
  });

  ngOnInit(): void {
    this.form.valueChanges.subscribe(() => {
      if (this.suppressFormReset) {
        return;
      }

      this.formStatus = 'pending';
      this.revalidateAllControls();
      this.clearPayload();
    });

    void this.loadConfig();
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

    if (selected.length === this.canFrameOptions.length) {
      return `${selected.length} selected`;
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

  toggleCanOpen(): void {
    this.isCanOpen = !this.isCanOpen;
    if (this.isCanOpen) {
      this.isGpsOpen = false;
      this.isDbcOpen = false;
    }
  }

  toggleDbcOpen(): void {
    this.isDbcOpen = !this.isDbcOpen;
    if (this.isDbcOpen) {
      this.isGpsOpen = false;
      this.isCanOpen = false;
    }
  }

  toggleGpsOpen(): void {
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
      const response = await fetch(request.url, {
        method: request.method,
        headers: { 'Content-Type': 'application/json' },
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
        request,
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
        request,
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
            'Check browser DevTools > Network for blocked/preflight requests.'
          ]
        }
      }, null, 2));
    } finally {
      this.form.controls.payload.markAsTouched();
      this.isSubmitting = false;
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

  private buildCanFrameOptions(): string[] {
    const options: string[] = [];

    for (let i = 0; i < 150; i++) {
      const hex = i.toString(16).toUpperCase().padStart(3, '0');
      options.push(`0x${hex}`);
    }

    return options;
  }

  private async loadConfig(): Promise<void> {
    const defaultGpsAreas: string[] = [];
    const defaultCanFrames = this.buildCanFrameOptions();
    const defaultDbcFiles = ['dbc_a', 'dbc_b', 'dbc_c', 'dbc_d', 'dbc_e', 'dbc_f'];

    try {
      const config = await this.fetchRuntimeConfig();

      this.gpsAreas = Array.isArray(config.gpsAreas) && config.gpsAreas.length > 0 ? config.gpsAreas : defaultGpsAreas;
      this.canFrameOptions = Array.isArray(config.canFrames) && config.canFrames.length > 0 ? config.canFrames : defaultCanFrames;
      this.dbcOptions = Array.isArray(config.dbcFiles) && config.dbcFiles.length > 0 ? config.dbcFiles : defaultDbcFiles;
      this.form.controls.workQueueUrl.setValue(typeof config.workQueueUrl === 'string' ? config.workQueueUrl : '', { emitEvent: false });
      this.form.controls.s3Bucket.setValue(typeof config.s3Default === 'string' && config.s3Default.trim() ? config.s3Default.trim() : '', { emitEvent: false });
      this.form.controls.engineUrl.setValue(typeof config.engineURL === 'string' && config.engineURL.trim() ? config.engineURL.trim() : '', { emitEvent: false });
    } catch {
      this.gpsAreas = defaultGpsAreas;
      this.canFrameOptions = defaultCanFrames;
      this.dbcOptions = defaultDbcFiles;
      this.form.controls.workQueueUrl.setValue('', { emitEvent: false });
      this.form.controls.s3Bucket.setValue('', { emitEvent: false });
      this.form.controls.engineUrl.setValue('', { emitEvent: false });
    }

    this.suppressFormReset = true;
    this.form.controls.canFrames.setValue([...this.canFrameOptions], { emitEvent: false });
    this.form.controls.dbcFiles.setValue([...this.dbcOptions], { emitEvent: false });
    this.form.controls.canFrames.updateValueAndValidity({ emitEvent: false });
    this.form.controls.dbcFiles.updateValueAndValidity({ emitEvent: false });
    this.form.updateValueAndValidity({ emitEvent: false });
    this.suppressFormReset = false;
  }

  private async fetchRuntimeConfig(): Promise<{ gpsAreas?: string[]; canFrames?: string[]; dbcFiles?: string[]; workQueueUrl?: string; s3Default?: string; engineURL?: string }> {
    const stamp = Date.now();
    const candidates = [
      `config.json?t=${stamp}`,
      `assets/config.json?t=${stamp}`
    ];

    for (const url of candidates) {
      try {
        const response = await fetch(url, { cache: 'no-store' });
        if (response.ok) {
          const text = await response.text();
          return JSON.parse(text) as { gpsAreas?: string[]; canFrames?: string[]; dbcFiles?: string[]; workQueueUrl?: string; s3Default?: string; engineURL?: string };
        }
      } catch {
        // Try next candidate.
      }
    }

    throw new Error('Unable to load runtime config');
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

}

