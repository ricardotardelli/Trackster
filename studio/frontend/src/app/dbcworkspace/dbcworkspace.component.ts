import { DbcEditorComponent } from '../dbceditor/dbceditor.component';
import { MatDialog } from '@angular/material/dialog';
import { DialogShellComponent } from '../dialogshell/dialogshell.component';
import { CommonModule } from '@angular/common';
import { AfterViewInit, Component, OnInit, ViewChild } from '@angular/core';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { HttpClient } from '@angular/common/http';

type OriginalDbcStatus = 'pending' | 'validated' | 'rejected';
type ValidationLogLevel = 'info' | 'warning' | 'error';

interface DbcFolderFile {
  name: string;
  sizeBytes: number;
  lastModified: string;
  status: OriginalDbcStatus;
  content?: string;
}

interface DbcFolderResponse {
  folderName: string;
  files: DbcFolderFile[];
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

  ngOnInit(): void {
    this.originalFilesDataSource.data = this.originalFiles;
    this.loadDbcFolderCatalog();
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
        this.selectOriginalFile(newSelected);
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

  validateSelectedFiles(): void {
    if (!this.hasPendingSelection()) {
      return;
    }

    const selectedNameBeforeUpdate = this.selectedOriginalFileName;

    this.originalFiles = this.originalFiles.map((file) => {
      if (
        this.checkedOriginalFileNames.has(file.name) &&
        this.isFileValidatable(file)
      ) {
        return {
          ...file,
          status: 'validated',
          lastModified: this.getCurrentTimestamp()
        };
      }

      return file;
    });

    this.checkedOriginalFileNames.clear();
    this.originalFilesDataSource.data = this.originalFiles;

    if (selectedNameBeforeUpdate) {
      const updatedSelected = this.originalFiles.find(
        (file) => file.name === selectedNameBeforeUpdate
      );

      if (updatedSelected) {
        this.selectOriginalFile(updatedSelected);
      }
    }
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
        this.selectOriginalFile(nextSelected);
      } else {
        this.selectedOriginalFileName = null;
        this.selectedOriginalFile = null;
        this.selectedValidationPreview = null;
      }
    }
  }

  selectOriginalFile(file: OriginalDbcFile): void {
    this.selectedOriginalFileName = file.name;
    this.selectedOriginalFile = file;
    this.selectedValidationPreview = this.buildFakeValidationPreview(file);
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
      (a, b) => this.toSortableTimestamp(b.lastModified) - this.toSortableTimestamp(a.lastModified)
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

  onNativeFilesSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    const files = input.files ? Array.from(input.files) : [];

    if (files.length > 0) {
      this.onSelectFiles(files);
    }

    input.value = '';
  }

  loadDbcFolderCatalog(): void {
    this.getDbcFolderCatalog().subscribe({
      next: (response) => {
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
          this.selectOriginalFile(nextSelected);
        } else {
          this.selectedOriginalFileName = null;
          this.selectedOriginalFile = null;
          this.selectedValidationPreview = null;
        }
      },
      error: (error) => {
        console.error('Failed to load DBC folder catalog.', error);
        this.folderName = '';
        this.originalFiles = [];
        this.originalFilesDataSource.data = [];
        this.checkedOriginalFileNames.clear();
        this.selectedOriginalFileName = null;
        this.selectedOriginalFile = null;
        this.selectedValidationPreview = null;
      }
    });
  }

  private getDbcFolderCatalog() {
    return this.http.get<DbcFolderResponse>('assets/mock/dbc-folder.json');
  }

  private mapFolderFileToOriginalFile(file: DbcFolderFile): OriginalDbcFile {
    return {
      name: file.name,
      sizeBytes: file.sizeBytes,
      lastModified: file.lastModified,
      status: file.status,
      content: file.content
    };
  }

  private async resolveDbcContent(file: OriginalDbcFile): Promise<string> {
    if (typeof file.content === 'string') {
      return file.content;
    }

    return '';
  }

  private buildFakeValidationPreview(file: OriginalDbcFile): ValidationPreview {
    if (file.status === 'validated') {
      return this.buildValidatedPreview(file);
    }

    if (file.status === 'rejected') {
      return this.buildRejectedPreview(file);
    }

    return this.buildPendingPreview(file);
  }

  private buildValidatedPreview(file: OriginalDbcFile): ValidationPreview {
    const messages: ValidationMessagePreview[] = [
      {
        id: '0x120',
        name: 'VehicleSpeedStatus',
        dlc: 8,
        transmitter: 'VCU',
        signals: [
          {
            name: 'VehicleSpeed',
            startBit: 0,
            length: 16,
            endianness: 'little_endian',
            signed: false,
            factor: 0.01,
            offset: 0,
            min: 0,
            max: 250,
            unit: 'km/h'
          },
          {
            name: 'WheelBasedSpeed',
            startBit: 16,
            length: 16,
            endianness: 'little_endian',
            signed: false,
            factor: 0.01,
            offset: 0,
            min: 0,
            max: 250,
            unit: 'km/h'
          },
          {
            name: 'SpeedValidity',
            startBit: 32,
            length: 2,
            endianness: 'little_endian',
            signed: false,
            factor: 1,
            offset: 0,
            min: 0,
            max: 3
          }
        ]
      },
      {
        id: '0x221',
        name: 'EngineData',
        dlc: 8,
        transmitter: 'ECM',
        signals: [
          {
            name: 'EngineRpm',
            startBit: 0,
            length: 16,
            endianness: 'little_endian',
            signed: false,
            factor: 0.25,
            offset: 0,
            min: 0,
            max: 8000,
            unit: 'rpm'
          },
          {
            name: 'ThrottlePosition',
            startBit: 16,
            length: 8,
            endianness: 'little_endian',
            signed: false,
            factor: 0.4,
            offset: 0,
            min: 0,
            max: 100,
            unit: '%'
          },
          {
            name: 'EngineCoolantTemp',
            startBit: 24,
            length: 8,
            endianness: 'little_endian',
            signed: true,
            factor: 1,
            offset: -40,
            min: -40,
            max: 215,
            unit: '°C'
          }
        ]
      },
      {
        id: '0x305',
        name: 'GpsPose',
        dlc: 8,
        transmitter: 'TCU',
        signals: [
          {
            name: 'LatitudeRaw',
            startBit: 0,
            length: 32,
            endianness: 'big_endian',
            signed: true,
            factor: 0.000001,
            offset: 0,
            min: -90,
            max: 90,
            unit: 'deg'
          },
          {
            name: 'LongitudeRaw',
            startBit: 32,
            length: 32,
            endianness: 'big_endian',
            signed: true,
            factor: 0.000001,
            offset: 0,
            min: -180,
            max: 180,
            unit: 'deg'
          }
        ]
      }
    ];

    const totalSignals = messages.reduce((sum, message) => sum + message.signals.length, 0);

    return {
      summary: {
        messages: messages.length,
        signals: totalSignals,
        warnings: 1,
        errors: 0
      },
      logEntries: [
        {
          level: 'info',
          code: 'DBC-001',
          message: 'File loaded successfully.',
          context: file.name
        },
        {
          level: 'info',
          code: 'DBC-010',
          message: 'Message definitions parsed without structural errors.',
          context: '3 messages detected'
        },
        {
          level: 'warning',
          code: 'DBC-021',
          message: 'One signal does not define an explicit unit.',
          context: 'VehicleSpeedStatus.SpeedValidity'
        },
        {
          level: 'info',
          code: 'DBC-099',
          message: 'Validation completed successfully.',
          context: 'File is ready for simulation catalog usage'
        }
      ],
      messages
    };
  }

  private buildPendingPreview(file: OriginalDbcFile): ValidationPreview {
    return {
      summary: {
        messages: 0,
        signals: 0,
        warnings: 0,
        errors: 0
      },
      logEntries: [
        {
          level: 'info',
          code: 'DBC-000',
          message: 'File uploaded and queued for validation.',
          context: file.name
        },
        {
          level: 'info',
          code: 'DBC-002',
          message: 'Awaiting validation execution.',
          context: 'No parser output available yet'
        }
      ],
      messages: []
    };
  }

  private buildRejectedPreview(file: OriginalDbcFile): ValidationPreview {
    return {
      summary: {
        messages: 1,
        signals: 0,
        warnings: 1,
        errors: 2
      },
      logEntries: [
        {
          level: 'info',
          code: 'DBC-001',
          message: 'File loaded successfully.',
          context: file.name
        },
        {
          level: 'warning',
          code: 'DBC-014',
          message: 'Message declaration found with incomplete metadata.',
          context: 'Message BrakeStatus'
        },
        {
          level: 'error',
          code: 'DBC-031',
          message: 'Signal bit range exceeds message payload length.',
          context: 'BrakeStatus.BrakePressure'
        },
        {
          level: 'error',
          code: 'DBC-041',
          message: 'Validation failed due to structural inconsistency.',
          context: 'Signal extraction aborted'
        }
      ],
      messages: []
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
        content
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
          lastModified: this.getCurrentTimestamp()
        };
      });

      this.originalFilesDataSource.data = this.originalFiles;

      const updatedSelected =
        this.originalFiles.find((currentFile) => currentFile.name === file.name) ?? null;

      if (updatedSelected) {
        this.selectOriginalFile(updatedSelected);
      }

      console.log('Updated DBC content:', result.content);
    });
  }
}