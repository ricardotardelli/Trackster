import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { MatTableModule } from '@angular/material/table';
import { NgxDropzoneModule } from 'ngx-dropzone';

interface OriginalDbcFile {
  name: string;
  sizeBytes: number;
  lastModified: string;
  status: 'uploaded' | 'pending' | 'validated' | 'rejected';
}

interface ValidatedDbcFile {
  name: string;
  state: string;
  lastModified: string;
}

@Component({
  selector: 'app-dbcworkspace',
  standalone: true,
  imports: [CommonModule, NgxDropzoneModule, MatTableModule],
  templateUrl: './dbcworkspace.component.html',
  styleUrl: './dbcworkspace.component.css'
})
export class DbcworkspaceComponent implements OnInit {
  selectedFiles: File[] = [];

  originalFiles: OriginalDbcFile[] = [
    {
      name: 'Bus_A.dbc',
      sizeBytes: 1004825,
      lastModified: '2026-04-14 00:00',
      status: 'uploaded'
    },
    {
      name: 'Body_Control.dbc',
      sizeBytes: 582144,
      lastModified: '2026-04-13 18:42',
      status: 'validated'
    },
    {
      name: 'Powertrain.dbc',
      sizeBytes: 760729,
      lastModified: '2026-04-13 10:15',
      status: 'pending'
    }
  ];

  validatedFiles: ValidatedDbcFile[] = [
    {
      name: 'Body_Control.compiled.json',
      state: 'validated',
      lastModified: '2026-04-13 18:44'
    },
    {
      name: 'Bus_A.compiled.json',
      state: 'validated',
      lastModified: '2026-04-14 00:02'
    }
  ];

  isUploading = false;

  selectedOriginalFileName: string | null = 'Bus_A.dbc';
  selectedValidatedFileName: string | null = null;

  readonly intakeDisplayedColumns: string[] = ['name', 'size', 'lastModified'];
  readonly validatedDisplayedColumns: string[] = ['name', 'state', 'lastModified'];

  ngOnInit(): void {
    // Later this is where you can load the real original/ folder list from backend.
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

      const uploadedEntries: OriginalDbcFile[] = this.selectedFiles.map((file) => ({
        name: file.name,
        sizeBytes: file.size,
        lastModified: now,
        status: 'uploaded'
      }));

      this.originalFiles = [...uploadedEntries, ...this.originalFiles];
      this.selectedFiles = [];
      this.selectedOriginalFileName = uploadedEntries[0]?.name ?? this.selectedOriginalFileName;
    } finally {
      this.isUploading = false;
    }
  }

  selectOriginalFile(file: OriginalDbcFile): void {
    this.selectedOriginalFileName = file.name;
  }

  selectValidatedFile(file: ValidatedDbcFile): void {
    this.selectedValidatedFileName = file.name;
  }

  isOriginalSelected(file: OriginalDbcFile): boolean {
    return this.selectedOriginalFileName === file.name;
  }

  isValidatedSelected(file: ValidatedDbcFile): boolean {
    return this.selectedValidatedFileName === file.name;
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

  trackOriginalFile(_: number, file: OriginalDbcFile): string {
    return `${file.name}-${file.lastModified}-${file.sizeBytes}`;
  }

  trackValidatedFile(_: number, file: ValidatedDbcFile): string {
    return `${file.name}-${file.lastModified}`;
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
}