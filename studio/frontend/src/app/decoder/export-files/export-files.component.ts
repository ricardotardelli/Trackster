import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Output } from '@angular/core';
import { LocalFileSaveService } from './local-file-save.service';

export type ExportFileFormat =
  | 'json'
  | 'csv'
  | 'vectorasc'
  | 'blf'
  | 'mf4'
  | 'parquet';

export interface ExportFilePayload {
  fileName: string;
  blob: Blob;
  format: ExportFileFormat;
  mimeType?: string;
}

export interface ExportFilesResult {
  fileName: string;
  format: ExportFileFormat;
  sizeBytes: number;
}

@Component({
  selector: 'app-export-files',
  standalone: true,
  imports: [
    CommonModule
  ],
  template: '',
  styles: []
})
export class ExportFilesComponent {

  @Output() public exportStarted = new EventEmitter<ExportFileFormat>();
  @Output() public exportCompleted = new EventEmitter<ExportFilesResult>();
  @Output() public exportFailed = new EventEmitter<string>();

  public isExporting = false;
  public lastError: string | null = null;

  constructor(
    private readonly localFileSaveService: LocalFileSaveService
  ) {}

  public async saveFile(payload: ExportFilePayload): Promise<void> {

    if (this.isExporting) {
      return;
    }

    this.lastError = null;

    const validationError = this.validatePayload(payload);

    if (validationError) {
      this.lastError = validationError;
      this.exportFailed.emit(validationError);
      return;
    }

    try {
      this.isExporting = true;
      this.exportStarted.emit(payload.format);

      await this.localFileSaveService.saveFile({
        fileName: payload.fileName,
        blob: payload.blob,
        mimeType: payload.mimeType
      });

      this.exportCompleted.emit({
        fileName: payload.fileName,
        format: payload.format,
        sizeBytes: payload.blob.size
      });

    } catch (error: unknown) {
      const message = this.getErrorMessage(error);

      this.lastError = message;
      this.exportFailed.emit(message);

    } finally {
      this.isExporting = false;
    }
  }

  public buildBlob(
    content: string | ArrayBuffer | Uint8Array,
    mimeType: string
    ): Blob {

    if (typeof content === 'string') {
        return new Blob(
        [content],
        { type: mimeType }
        );
    }

    if (content instanceof Uint8Array) {
        const arrayBuffer = new ArrayBuffer(content.byteLength);
        const view = new Uint8Array(arrayBuffer);

        view.set(content);

        return new Blob(
        [arrayBuffer],
        { type: mimeType }
        );
    }

    return new Blob(
        [content],
        { type: mimeType }
    );
  }

  public getMimeType(format: ExportFileFormat): string {

    switch (format) {
      case 'json':
        return 'application/json;charset=utf-8';

      case 'csv':
        return 'text/csv;charset=utf-8';

      case 'vectorasc':
        return 'text/plain;charset=utf-8';

      case 'blf':
        return 'application/octet-stream';

      case 'mf4':
        return 'application/octet-stream';

      case 'parquet':
        return 'application/octet-stream';

      default:
        return 'application/octet-stream';
    }
  }

  public normalizeFileName(
    baseName: string,
    format: ExportFileFormat
  ): string {

    const cleanBaseName = baseName
      .trim()
      .replace(/[\\/:*?"<>|]+/g, '_');

    const safeBaseName = cleanBaseName || 'trackster-export';
    const extension = this.getExtension(format);

    if (safeBaseName.toLowerCase().endsWith(`.${extension}`)) {
      return safeBaseName;
    }

    return `${safeBaseName}.${extension}`;
  }

  private getExtension(format: ExportFileFormat): string {

    switch (format) {
      case 'json':
        return 'json';

      case 'csv':
        return 'csv';

      case 'vectorasc':
        return 'asc';

      case 'blf':
        return 'blf';

      case 'mf4':
        return 'mf4';

      case 'parquet':
        return 'parquet';

      default:
        return 'bin';
    }
  }

  private validatePayload(payload: ExportFilePayload): string | null {

    if (!payload.fileName || !payload.fileName.trim()) {
      return 'Export file name is required.';
    }

    if (!payload.blob) {
      return 'Export file content is required.';
    }

    if (payload.blob.size <= 0) {
      return 'Export file is empty.';
    }

    return null;
  }

  private getErrorMessage(error: unknown): string {

    if (error instanceof Error && error.message.trim()) {
      return error.message;
    }

    return 'Unable to export file.';
  }
}