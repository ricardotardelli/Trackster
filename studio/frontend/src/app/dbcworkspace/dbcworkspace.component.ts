import { HttpClient, HttpHeaders } from '@angular/common/http';
import { fetchAuthSession } from 'aws-amplify/auth';
import { firstValueFrom } from 'rxjs';
import { DbcEditorComponent } from '../dbceditor/dbceditor.component';
import { DbcParser, type DbcFullReport } from '../dbceditor/dbcparser';
import { MatDialog } from '@angular/material/dialog';
import { DialogShellComponent } from '../dialogshell/dialogshell.component';
import { CommonModule } from '@angular/common';
import { AfterViewInit, Component, OnInit, ViewChild } from '@angular/core';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { environment } from '../../environments/environment';

type OriginalDbcStatus = 'pending' | 'validated' | 'rejected';
type ValidationLogLevel = 'info' | 'warning' | 'error';

interface DbcFolderFile {
  name: string;
  sizeBytes: number;
  lastModified: string;
  status: OriginalDbcStatus;
}

interface DbcFolderResponse {
  folderName: string;
  files: DbcFolderFile[];
}

interface DbcFolderApiFile {
  key: string;
  fileName: string;
  size: number;
  lastModified: string | null;
}

interface DbcFolderApiResponse {
  customerId: string;
  path: string;
  files: DbcFolderApiFile[];
}

interface OriginalDbcFile {
  name: string;
  sizeBytes: number;
  lastModified: string;
  status: OriginalDbcStatus;
  content?: string;
}

interface ValidationLogEntry {
  level: ValidationLogLevel;
  code: string;
  message: string;
  context?: string;
}

interface ValidationSignalPreview {
  name: string;
  startBit: number;
  length: number;
  endianness: 'little_endian' | 'big_endian';
  signed: boolean;
  factor: number;
  offset: number;
  min: number;
  max: number;
  unit?: string;
}

interface ValidationMessagePreview {
  id: string;
  name: string;
  dlc: number;
  transmitter: string;
  signals: ValidationSignalPreview[];
}

interface ValidationPreviewSummary {
  messages: number;
  signals: number;
  warnings: number;
  errors: number;
}

interface ValidationPreview {
  summary: ValidationPreviewSummary;
  logEntries: ValidationLogEntry[];
  messages: ValidationMessagePreview[];
}

interface DbcApiConfig {
  folderCatalogUrl: string;
  contentUrl: string;
}

interface AppConfig {
  dbcApi?: DbcApiConfig;
}

interface DbcContentResponse {
  fileName: string;
  content: string;
}

@Component({
  selector: 'app-dbcworkspace',
  standalone: true,
  imports: [
    CommonModule,
    MatTableModule,
    MatSortModule,
    MatCheckboxModule,
    DialogShellComponent,
    DbcEditorComponent
  ],
  templateUrl: './dbcworkspace.component.html',
  styleUrls: ['./dbcworkspace.component.css']
})
export class DbcworkspaceComponent implements OnInit, AfterViewInit {
  @ViewChild('intakeSort') intakeSort!: MatSort;

  constructor(
    private readonly dialog: MatDialog,
    private readonly http: HttpClient
  ) {}

  selectedFiles: File[] = [];
  isUploading = false;

  selectedOriginalFileName: string | null = null;
  selectedOriginalFile: OriginalDbcFile | null = null;
  selectedValidationPreview: ValidationPreview | null = null;

  folderName = '';

  customerId = "00000000";

  readonly intakeDisplayedColumns: string[] = [
    'select',
    'name',
    'status',
    'size',
    'lastModified'
  ];

  originalFiles: OriginalDbcFile[] = [];

  originalFilesDataSource = new MatTableDataSource<OriginalDbcFile>([]);
  checkedOriginalFileNames = new Set<string>();

  private appConfig: AppConfig | null = null;

  private async loadAppConfig(): Promise<AppConfig> {
    if (this.appConfig) {
      return this.appConfig;
    }

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
        this.appConfig = JSON.parse(text) as AppConfig;
        return this.appConfig;
      } catch {
      }
    }

    throw new Error(
      `Unable to load runtime config from assets/config.json. Last HTTP status: ${lastStatus ?? 'unknown'}`
    );
  }

  private shouldUseLocalMock(): boolean {
    const isLocalhost =
      window.location.hostname === 'localhost' ||
      window.location.hostname === '127.0.0.1';

    return environment.disableAuth && isLocalhost;
  }

  async ngOnInit(): Promise<void> {
    this.originalFilesDataSource.data = this.originalFiles;
    await this.loadDbcFolderCatalog();
  }

  ngAfterViewInit(): void {
    this.originalFilesDataSource.sort = this.intakeSort;

    this.originalFilesDataSource.sortingDataAccessor = (
      item: OriginalDbcFile,
      property: string
    ): string | number => {
      switch (property) {
        case 'name':
          return item.name.toLowerCase();
        case 'status':
          return item.status.toLowerCase();
        case 'size':
          return item.sizeBytes;
        case 'lastModified':
          return this.toSortableTimestamp(item.lastModified);
        default:
          return '';
      }
    };
  }

  onSelectFiles(files: File[]): void {
    const dbcFiles = files.filter((file) => file.name.toLowerCase().endsWith('.dbc'));

    const uniqueFiles = dbcFiles.filter((newFile) => {
      return !this.selectedFiles.some(
        (existingFile) =>
          existingFile.name === newFile.name &&
          existingFile.size === newFile.size &&
          existingFile.lastModified === newFile.lastModified
      );
    });

    this.selectedFiles = [...this.selectedFiles, ...uniqueFiles];
  }

  onRemoveSelectedFile(fileToRemove: File): void {
    this.selectedFiles = this.selectedFiles.filter((file) => file !== fileToRemove);
  }

  clearSelectedFiles(): void {
    this.selectedFiles = [];
  }

  async uploadSelectedFiles(): Promise<void> {
    if (this.selectedFiles.length === 0 || this.isUploading) {
      return;
    }

    this.isUploading = true;

    try {
      const now = this.getCurrentTimestamp();

      const uploadedEntries: OriginalDbcFile[] = await Promise.all(
        this.selectedFiles.map(async (file) => ({
          name: file.name,
          sizeBytes: file.size,
          lastModified: now,
          status: 'rejected' as OriginalDbcStatus,
          content: await file.text()
        }))
      );

      this.originalFiles = [...uploadedEntries, ...this.originalFiles];
      this.originalFilesDataSource.data = this.originalFiles;
      this.selectedFiles = [];

      const newSelected = uploadedEntries[0] ?? null;

      if (newSelected) {
        void this.selectOriginalFile(newSelected);
      }
    } finally {
      this.isUploading = false;
    }
  }

  hasPendingSelection(): boolean {
    return this.originalFiles.some(
      (file) =>
        (file.status === 'pending' || file.status === 'rejected') &&
        this.checkedOriginalFileNames.has(file.name)
    );
  }

  async validateSelectedFiles(): Promise<void> {
    if (!this.hasPendingSelection()) {
      return;
    }

    const selectedNameBeforeUpdate = this.selectedOriginalFileName;

    if (this.shouldUseLocalMock()) {
      await this.validateSelectedFilesLocally();
    } else {
      await this.validateSelectedFilesApi();
    }

    this.checkedOriginalFileNames.clear();
    this.originalFilesDataSource.data = this.originalFiles;

    if (selectedNameBeforeUpdate) {
      const updatedSelected = this.originalFiles.find(
        (file) => file.name === selectedNameBeforeUpdate
      );

      if (updatedSelected) {
        await this.selectOriginalFile(updatedSelected);
      }
    }
  }

  private async validateSelectedFilesLocally(): Promise<void> {
    const updatedFiles: OriginalDbcFile[] = await Promise.all(
      this.originalFiles.map(async (file): Promise<OriginalDbcFile> => {
        if (
          this.checkedOriginalFileNames.has(file.name) &&
          this.isFileValidatable(file)
        ) {
          const content = await this.resolveDbcContent(file);
          const report = DbcParser.parse(content);

          return {
            ...file,
            status:
              report.errors.length === 0
                ? 'validated'
                : 'rejected',
            lastModified: this.getCurrentTimestamp()
          };
        }

        return file;
      })
    );

    this.originalFiles = updatedFiles;
  }

  private async validateSelectedFilesApi(): Promise<void> {
    throw new Error('Production validation via API is not implemented yet.');
  }

  removeSelectedFiles(): void {
    const remaining = this.originalFiles.filter(
      (file) => !this.checkedOriginalFileNames.has(file.name)
    );

    this.originalFiles = remaining;
    this.originalFilesDataSource.data = remaining;
    this.checkedOriginalFileNames.clear();

    if (
      this.selectedOriginalFileName &&
      !remaining.some((file) => file.name === this.selectedOriginalFileName)
    ) {
      const nextSelected = remaining[0] ?? null;

      if (nextSelected) {
        void this.selectOriginalFile(nextSelected);
      } else {
        this.selectedOriginalFileName = null;
        this.selectedOriginalFile = null;
        this.selectedValidationPreview = null;
      }
    }
  }

  async selectOriginalFile(file: OriginalDbcFile): Promise<void> {
    this.selectedOriginalFileName = file.name;
    this.selectedOriginalFile = file;
    await this.refreshSelectedValidationPanel(file);
  }

  isOriginalSelected(file: OriginalDbcFile): boolean {
    return this.selectedOriginalFileName === file.name;
  }

  isFileValidatable(file: OriginalDbcFile): boolean {
    return file.status === 'pending' || file.status === 'rejected';
  }

  isFileSelectable(file: OriginalDbcFile): boolean {
    return true;
  }

  isOriginalChecked(file: OriginalDbcFile): boolean {
    return this.checkedOriginalFileNames.has(file.name);
  }

  toggleOriginalChecked(file: OriginalDbcFile, checked: boolean): void {
    if (!this.isFileSelectable(file)) {
      return;
    }

    if (checked) {
      this.checkedOriginalFileNames.add(file.name);
    } else {
      this.checkedOriginalFileNames.delete(file.name);
    }
  }

  toggleAllPendingSelections(checked: boolean): void {
    if (checked) {
      this.originalFiles.forEach((file) => this.checkedOriginalFileNames.add(file.name));
      return;
    }

    this.originalFiles.forEach((file) => this.checkedOriginalFileNames.delete(file.name));
  }

  areAllPendingSelected(): boolean {
    if (this.originalFiles.length === 0) {
      return false;
    }

    return this.originalFiles.every((file) =>
      this.checkedOriginalFileNames.has(file.name)
    );
  }

  isPendingSelectionIndeterminate(): boolean {
    const checkedCount = this.originalFiles.filter((file) =>
      this.checkedOriginalFileNames.has(file.name)
    ).length;

    return checkedCount > 0 && checkedCount < this.originalFiles.length;
  }

  hasAnySelection(): number {
    return this.checkedOriginalFileNames.size;
  }

  hasPendingFiles(): boolean {
    return this.originalFiles.some((file) => this.isFileValidatable(file));
  }

  getPendingCount(): number {
    return this.originalFiles.filter((file) => file.status === 'pending').length;
  }

  getValidatedCount(): number {
    return this.originalFiles.filter((file) => file.status === 'validated').length;
  }

  getRejectedCount(): number {
    return this.originalFiles.filter((file) => file.status === 'rejected').length;
  }

  getLastUploadLabel(): string {
    if (this.originalFiles.length === 0) {
      return '-';
    }

    const sorted = [...this.originalFiles].sort(
      (a, b) =>
        this.toSortableTimestamp(b.lastModified) -
        this.toSortableTimestamp(a.lastModified)
    );

    return sorted[0].lastModified;
  }

  formatFileSize(sizeBytes: number): string {
    if (sizeBytes < 1024) {
      return `${sizeBytes} B`;
    }

    if (sizeBytes < 1024 * 1024) {
      return `${(sizeBytes / 1024).toFixed(1)} KB`;
    }

    return `${(sizeBytes / (1024 * 1024)).toFixed(2)} MB`;
  }

  async onNativeFilesSelected(event: Event): Promise<void> {
    const input = event.target as HTMLInputElement;
    const files = input.files ? Array.from(input.files) : [];

    if (files.length > 0) {
      this.onSelectFiles(files);
      await this.uploadFilesFake();
    }

    input.value = '';
  }

  private async uploadFilesFake(): Promise<void> {
    await this.uploadSelectedFiles();
  }

  private mapFolderFileToOriginalFile(file: DbcFolderFile): OriginalDbcFile {
    return {
      name: file.name,
      sizeBytes: file.sizeBytes,
      lastModified: file.lastModified,
      status: file.status
    };
  }

  async loadDbcFolderCatalog(): Promise<void> {
    try {
      const response = await this.getDbcFolderCatalog();
      const mappedFiles = response.files.map((file) => this.mapFolderFileToOriginalFile(file));

      this.folderName = response.folderName;
      this.originalFiles = mappedFiles;
      this.originalFilesDataSource.data = mappedFiles;
      this.checkedOriginalFileNames.clear();

      const selectedByName =
        (this.selectedOriginalFileName &&
          mappedFiles.find((file) => file.name === this.selectedOriginalFileName)) ||
        null;

      const nextSelected = selectedByName ?? mappedFiles[0] ?? null;

      if (nextSelected) {
        void this.selectOriginalFile(nextSelected);
      } else {
        this.selectedOriginalFileName = null;
        this.selectedOriginalFile = null;
        this.selectedValidationPreview = null;
      }
    } catch (error) {
      console.error('Failed to load DBC folder catalog.', error);
      this.folderName = '';
      this.originalFiles = [];
      this.originalFilesDataSource.data = [];
      this.checkedOriginalFileNames.clear();
      this.selectedOriginalFileName = null;
      this.selectedOriginalFile = null;
      this.selectedValidationPreview = null;
    }
  }

  private async getDbcFolderCatalog(): Promise<DbcFolderResponse> {
    if (this.shouldUseLocalMock()) {
      return await firstValueFrom(
        this.http.get<DbcFolderResponse>('assets/mock/dbc-folder.json')
      );
    }

    const config = await this.loadAppConfig();

    if (!config.dbcApi?.folderCatalogUrl?.trim()) {
      throw new Error('dbcApi.folderCatalogUrl missing or empty in config.json');
    }

    const headers = await this.getAuthorizationHeaders();

    const apiResponse = await firstValueFrom(
      this.http.get<DbcFolderApiResponse>(config.dbcApi.folderCatalogUrl.trim(), {
        headers,
        params: {
          customerId: this.customerId
        }
      })
    );

    return this.mapApiFolderResponse(apiResponse);
  }

  private async resolveDbcContent(file: OriginalDbcFile): Promise<string> {
    try {
      if (file.content != null) {
        return file.content;
      }

      if (this.shouldUseLocalMock()) {
        return await firstValueFrom(
          this.http.get(`assets/mock/dbc/${file.name}`, {
            responseType: 'text'
          })
        );
      }

      const config = await this.loadAppConfig();

      if (!config.dbcApi?.contentUrl?.trim()) {
        throw new Error('dbcApi.contentUrl missing or empty in config.json');
      }

      const headers = await this.getAuthorizationHeaders();

      const response = await firstValueFrom(
        this.http.get<DbcContentResponse>(config.dbcApi.contentUrl.trim(), {
          headers,
          params: {
            customerId: '00000000',
            fileName: file.name
          }
        })
      );

      return response.content ?? '';
    } 
    catch (error) {
      console.error('Failed to load DBC content:', file.name, error);
      return '';
    }
  }

  private async refreshSelectedValidationPanel(
    file: OriginalDbcFile
  ): Promise<void> {
    try {
      const content = await this.resolveDbcContent(file);
      const report = DbcParser.parse(content);

      this.selectedValidationPreview = this.mapParserReportToPreview(report, file);
    } catch (error) {
      console.error('Failed to refresh validation panel:', file.name, error);

      this.selectedValidationPreview = {
        summary: {
          messages: 0,
          signals: 0,
          warnings: 0,
          errors: 1
        },
        logEntries: [
          {
            level: 'error',
            code: 'DBC_PANEL_LOAD_ERROR',
            message: 'Failed to load or parse the selected DBC file.',
            context: file.name
          }
        ],
        messages: []
      };
    }
  }

  private mapParserReportToPreview(
    report: DbcFullReport,
    file: OriginalDbcFile
  ): ValidationPreview {
    const messages: ValidationMessagePreview[] = report.data.map((message) => ({
      id: message.hexId,
      name: message.name,
      dlc: message.sizeBytes,
      transmitter: message.transmitter || '-',
      signals: message.signals.map((signal) => ({
        name: signal.name,
        startBit: signal.startBit,
        length: signal.sizeBits,
        endianness:
          signal.endianness === 'Big Endian'
            ? 'big_endian'
            : 'little_endian',
        signed: signal.isSigned,
        factor: signal.factor,
        offset: signal.offset,
        min: signal.range.min,
        max: signal.range.max,
        unit: signal.unit || undefined
      }))
    }));

    const warningEntries: ValidationLogEntry[] = report.warnings.map((warning) => ({
      level: 'warning',
      code: warning.messageCode ?? warning.type,
      message: warning.message,
      context: `Line ${warning.line}`
    }));

    const errorEntries: ValidationLogEntry[] = report.errors.map((error) => ({
      level: 'error',
      code: error.messageCode ?? error.type,
      message: error.message,
      context: `Line ${error.line}`
    }));

    const infoEntries: ValidationLogEntry[] = [
      {
        level: 'info',
        code: 'DBC_PARSE_RESULT',
        message: report.isValid
          ? 'DBC parsed successfully.'
          : 'DBC parsed with validation errors.',
        context: file.name
      },
      {
        level: 'info',
        code: 'DBC_MESSAGES_TOTAL',
        message: `Messages: ${report.stats.messages.total} total, ${report.stats.messages.valid} valid, ${report.stats.messages.invalid} invalid.`,
        context: file.name
      },
      {
        level: 'info',
        code: 'DBC_SIGNALS_TOTAL',
        message: `Signals: ${report.stats.signals.total} total, ${report.stats.signals.valid} valid, ${report.stats.signals.invalid} invalid.`,
        context: file.name
      }
    ];

    return {
      summary: {
        messages: report.stats.messages.total,
        signals: report.stats.signals.total,
        warnings: report.warnings.length,
        errors: report.errors.length
      },
      logEntries: [...infoEntries, ...warningEntries, ...errorEntries],
      messages
    };
  }

  private getCurrentTimestamp(): string {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}`;
  }

  private toSortableTimestamp(value: string): number {
    return new Date(value.replace(' ', 'T')).getTime();
  }

  async openDbcEditor(file: OriginalDbcFile): Promise<void> {
    const content = await this.resolveDbcContent(file);

    const dialogRef = this.dialog.open(DbcEditorComponent, {
      width: '1500px',
      height: '820px',
      panelClass: 'trackster-dialog',
      autoFocus: false,
      restoreFocus: false,
      data: {
        file,
        title: 'DBC Editor',
        subtitle: file.name,
        content,
        storageMode: this.shouldUseLocalMock() ? 'local' : 'api'
      }
    });

    dialogRef.afterClosed().subscribe((result?: { saved: boolean; content: string }) => {
      if (!result?.saved) {
        return;
      }

      this.originalFiles = this.originalFiles.map((currentFile) => {
        if (currentFile.name !== file.name) {
          return currentFile;
        }

        return {
          ...currentFile,
          content: result.content,
          status: 'pending',
          lastModified: this.getCurrentTimestamp()
        };
      });

      this.originalFilesDataSource.data = this.originalFiles;

      const updatedSelected =
        this.originalFiles.find((currentFile) => currentFile.name === file.name) ?? null;

      if (updatedSelected) {
        void this.selectOriginalFile(updatedSelected);
      }

      console.log('Updated DBC content:', result.content);
    });
  }

  private async getAuthorizationHeaders(): Promise<HttpHeaders> {
    const session = await fetchAuthSession();
    const accessToken = session.tokens?.accessToken?.toString();

    if (!accessToken) {
      throw new Error('Missing access token for authenticated API call.');
    }

    return new HttpHeaders({
      Authorization: `Bearer ${accessToken}`
    });
  }

  private mapApiFolderResponse(apiResponse: DbcFolderApiResponse): DbcFolderResponse {
    return {
      folderName: apiResponse.path,
      files: apiResponse.files.map((file) => ({
        name: file.fileName,
        sizeBytes: file.size,
        lastModified: file.lastModified ?? this.getCurrentTimestamp(),
        status: 'pending' as OriginalDbcStatus
      }))
    };
  }
}