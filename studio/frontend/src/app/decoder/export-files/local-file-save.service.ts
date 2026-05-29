import { Injectable } from '@angular/core';

export interface ExportFile {
  fileName: string;
  blob: Blob;
}

interface SaveFileOptions {
  fileName: string;
  blob: Blob;
  mimeType?: string;
}

interface FilePickerWindow extends Window {
  showSaveFilePicker?: (options?: unknown) => Promise<{
    createWritable(): Promise<{
      write(data: Blob): Promise<void>;
      close(): Promise<void>;
    }>;
  }>;
}

@Injectable({
  providedIn: 'root'
})
export class LocalFileSaveService {

  public async saveFile(options: SaveFileOptions): Promise<void> {
    const fileName = options.fileName.trim();

    if (!fileName) {
      throw new Error('File name is required.');
    }

    const browserWindow = window as FilePickerWindow;

    if (typeof browserWindow.showSaveFilePicker === 'function') {
      try {
        await this.saveUsingFilePicker(
          browserWindow,
          fileName,
          options.blob
        );
        return;
      } catch (error: unknown) {
        if (this.isUserCancel(error)) {
          return;
        }

        console.warn(
          '[Trackster] showSaveFilePicker failed. Falling back to download.',
          error
        );
      }
    }

    this.saveUsingDownload(fileName, options.blob);
  }

  public async saveFiles(
    files: ExportFile[],
    zipFileName: string
  ): Promise<void> {
    throw new Error(
      `ZIP export is not implemented yet. Requested ZIP: ${zipFileName} (${files.length} files).`
    );
  }

  private async saveUsingFilePicker(
    browserWindow: FilePickerWindow,
    fileName: string,
    blob: Blob
  ): Promise<void> {

    const picker = await browserWindow.showSaveFilePicker?.({
      suggestedName: fileName
    });

    if (!picker) {
      throw new Error('File picker was not created.');
    }

    const writable = await picker.createWritable();

    await writable.write(blob);
    await writable.close();
  }

  private saveUsingDownload(
    fileName: string,
    blob: Blob
  ): void {

    const objectUrl = URL.createObjectURL(blob);

    try {
      const anchor = document.createElement('a');

      anchor.href = objectUrl;
      anchor.download = fileName;
      anchor.style.display = 'none';

      document.body.appendChild(anchor);

      anchor.click();

      document.body.removeChild(anchor);
    } finally {
      setTimeout(() => {
        URL.revokeObjectURL(objectUrl);
      }, 1000);
    }
  }

  private isUserCancel(error: unknown): boolean {

    if (!(error instanceof Error)) {
      return false;
    }

    return (
      error.name === 'AbortError' ||
      error.message.toLowerCase().includes('abort')
    );
  }
}