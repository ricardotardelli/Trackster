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

interface SaveFileProviderOptions {
  fileName: string;
  mimeType?: string;
  blobProvider: () => Promise<Blob>;
}

interface FilePickerWindow extends Window {
  showSaveFilePicker?: (options?: unknown) => Promise<{
    createWritable(): Promise<{
      write(data: Blob): Promise<void>;
      close(): Promise<void>;
    }>;
  }>;
}

interface ZipCentralDirectoryEntry {
  fileNameBytes: Uint8Array;
  crc32: number;
  compressedSize: number;
  uncompressedSize: number;
  localHeaderOffset: number;
  dosTime: number;
  dosDate: number;
}

@Injectable({
  providedIn: 'root'
})
export class LocalFileSaveService {

  private static crcTable: Uint32Array | null = null;

  public async saveFile(options: SaveFileOptions): Promise<void> {
    await this.saveFileFromProvider({
      fileName: options.fileName,
      mimeType: options.mimeType,
      blobProvider: async () => options.blob
    });
  }

  public async saveFileFromProvider(
    options: SaveFileProviderOptions
  ): Promise<void> {

    const fileName = options.fileName.trim();

    if (!fileName) {
      throw new Error('File name is required.');
    }

    const browserWindow = window as FilePickerWindow;

    if (typeof browserWindow.showSaveFilePicker === 'function') {
      try {
        const picker = await browserWindow.showSaveFilePicker({
          suggestedName: fileName
        });

        const blob = await options.blobProvider();

        const writable = await picker.createWritable();

        await writable.write(blob);
        await writable.close();

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

    const blob = await options.blobProvider();

    this.saveUsingDownload(fileName, blob);
  }

  public async saveFiles(
    filesProvider: () => Promise<ExportFile[]>,
    zipFileName: string
  ): Promise<void> {

    const normalizedZipFileName =
      this.normalizeZipFileName(zipFileName);

    await this.saveFileFromProvider({
      fileName: normalizedZipFileName,
      mimeType: 'application/zip',
      blobProvider: async () => {
        const files = await filesProvider();

        if (files.length === 0) {
          throw new Error('No files selected for export.');
        }

        return await this.createZipBlob(files);
      }
    });
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

  private async createZipBlob(
    files: ExportFile[]
  ): Promise<Blob> {

    const chunks: BlobPart[] = [];
    const centralDirectoryEntries: ZipCentralDirectoryEntry[] = [];
    const encoder = new TextEncoder();

    let offset = 0;

    for (const file of files) {
      const fileName =
        this.normalizeZipEntryName(file.fileName);

      const fileNameBytes =
        encoder.encode(fileName);

      const fileBytes =
        new Uint8Array(await file.blob.arrayBuffer());

      const crc32 =
        this.calculateCrc32(fileBytes);

      const { dosTime, dosDate } =
        this.getCurrentDosDateTime();

      const localHeader =
        this.buildLocalFileHeader(
          fileNameBytes,
          crc32,
          fileBytes.byteLength,
          dosTime,
          dosDate
        );

      chunks.push(localHeader.buffer as ArrayBuffer);
      chunks.push(fileBytes.buffer as ArrayBuffer);

      centralDirectoryEntries.push({
        fileNameBytes,
        crc32,
        compressedSize: fileBytes.byteLength,
        uncompressedSize: fileBytes.byteLength,
        localHeaderOffset: offset,
        dosTime,
        dosDate
      });

      offset += localHeader.byteLength + fileBytes.byteLength;
    }

    const centralDirectoryOffset = offset;

    for (const entry of centralDirectoryEntries) {
      const centralDirectoryHeader =
        this.buildCentralDirectoryHeader(entry);

      chunks.push(centralDirectoryHeader.buffer as ArrayBuffer);

      offset += centralDirectoryHeader.byteLength;
    }

    const centralDirectorySize =
      offset - centralDirectoryOffset;

    const endOfCentralDirectory =
      this.buildEndOfCentralDirectoryRecord(
        centralDirectoryEntries.length,
        centralDirectorySize,
        centralDirectoryOffset
      );

    chunks.push(endOfCentralDirectory.buffer as ArrayBuffer);

    return new Blob(
      chunks,
      { type: 'application/zip' }
    );
  }

  private buildLocalFileHeader(
    fileNameBytes: Uint8Array,
    crc32: number,
    fileSize: number,
    dosTime: number,
    dosDate: number
  ): Uint8Array {

    const header =
      new Uint8Array(30 + fileNameBytes.byteLength);

    const view =
      new DataView(header.buffer);

    this.writeUint32(view, 0, 0x04034b50);
    this.writeUint16(view, 4, 20);
    this.writeUint16(view, 6, 0x0800);
    this.writeUint16(view, 8, 0);
    this.writeUint16(view, 10, dosTime);
    this.writeUint16(view, 12, dosDate);
    this.writeUint32(view, 14, crc32);
    this.writeUint32(view, 18, fileSize);
    this.writeUint32(view, 22, fileSize);
    this.writeUint16(view, 26, fileNameBytes.byteLength);
    this.writeUint16(view, 28, 0);

    header.set(fileNameBytes, 30);

    return header;
  }

  private buildCentralDirectoryHeader(
    entry: ZipCentralDirectoryEntry
  ): Uint8Array {

    const header =
      new Uint8Array(46 + entry.fileNameBytes.byteLength);

    const view =
      new DataView(header.buffer);

    this.writeUint32(view, 0, 0x02014b50);
    this.writeUint16(view, 4, 20);
    this.writeUint16(view, 6, 20);
    this.writeUint16(view, 8, 0x0800);
    this.writeUint16(view, 10, 0);
    this.writeUint16(view, 12, entry.dosTime);
    this.writeUint16(view, 14, entry.dosDate);
    this.writeUint32(view, 16, entry.crc32);
    this.writeUint32(view, 20, entry.compressedSize);
    this.writeUint32(view, 24, entry.uncompressedSize);
    this.writeUint16(view, 28, entry.fileNameBytes.byteLength);
    this.writeUint16(view, 30, 0);
    this.writeUint16(view, 32, 0);
    this.writeUint16(view, 34, 0);
    this.writeUint16(view, 36, 0);
    this.writeUint32(view, 38, 0);
    this.writeUint32(view, 42, entry.localHeaderOffset);

    header.set(entry.fileNameBytes, 46);

    return header;
  }

  private buildEndOfCentralDirectoryRecord(
    entryCount: number,
    centralDirectorySize: number,
    centralDirectoryOffset: number
  ): Uint8Array {

    const header =
      new Uint8Array(22);

    const view =
      new DataView(header.buffer);

    this.writeUint32(view, 0, 0x06054b50);
    this.writeUint16(view, 4, 0);
    this.writeUint16(view, 6, 0);
    this.writeUint16(view, 8, entryCount);
    this.writeUint16(view, 10, entryCount);
    this.writeUint32(view, 12, centralDirectorySize);
    this.writeUint32(view, 16, centralDirectoryOffset);
    this.writeUint16(view, 20, 0);

    return header;
  }

  private calculateCrc32(bytes: Uint8Array): number {
    const table =
      this.getCrcTable();

    let crc = 0xffffffff;

    for (const byte of bytes) {
      crc = (crc >>> 8) ^ table[(crc ^ byte) & 0xff];
    }

    return (crc ^ 0xffffffff) >>> 0;
  }

  private getCrcTable(): Uint32Array {
    if (LocalFileSaveService.crcTable) {
      return LocalFileSaveService.crcTable;
    }

    const table =
      new Uint32Array(256);

    for (let index = 0; index < 256; index += 1) {
      let value = index;

      for (let bit = 0; bit < 8; bit += 1) {
        value =
          (value & 1) !== 0
            ? 0xedb88320 ^ (value >>> 1)
            : value >>> 1;
      }

      table[index] = value >>> 0;
    }

    LocalFileSaveService.crcTable = table;

    return table;
  }

  private getCurrentDosDateTime(): {
    dosTime: number;
    dosDate: number;
  } {

    const now = new Date();

    const year =
      Math.max(1980, now.getFullYear());

    const dosTime =
      (now.getHours() << 11) |
      (now.getMinutes() << 5) |
      Math.floor(now.getSeconds() / 2);

    const dosDate =
      ((year - 1980) << 9) |
      ((now.getMonth() + 1) << 5) |
      now.getDate();

    return {
      dosTime,
      dosDate
    };
  }

  private normalizeZipEntryName(
    fileName: string
  ): string {

    const parts = fileName
      .replace(/\\/g, '/')
      .split('/')
      .map(part =>
        part
          .trim()
          .replace(/[\\:*?"<>|]+/g, '_')
      )
      .filter(part =>
        part.length > 0 &&
        part !== '.' &&
        part !== '..'
      );

    return parts.join('/') || 'trackster-export.bin';
  }

  private normalizeZipFileName(
    zipFileName: string
  ): string {

    const cleanName =
      zipFileName
        .trim()
        .replace(/[\\/:*?"<>|]+/g, '_');

    const safeName =
      cleanName || 'trackster-selected-files.zip';

    if (safeName.toLowerCase().endsWith('.zip')) {
      return safeName;
    }

    return `${safeName}.zip`;
  }

  private writeUint16(
    view: DataView,
    offset: number,
    value: number
  ): void {
    view.setUint16(offset, value, true);
  }

  private writeUint32(
    view: DataView,
    offset: number,
    value: number
  ): void {
    view.setUint32(offset, value >>> 0, true);
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