import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { NestedTreeControl } from '@angular/cdk/tree';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule, MatTreeNestedDataSource } from '@angular/material/tree';
import { FormsModule } from '@angular/forms';
import { MatMenuModule } from '@angular/material/menu';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { environment } from '../../environments/environment';
import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { fetchAuthSession } from 'aws-amplify/auth';
import { TracksterBinViewerComponent } from './viewers/trackster-bin-viewer/trackster-bin-viewer.component';
import { DecodedSignalsViewerComponent } from './viewers/decodedsignals-viewer/decodedsignals-viewer.component';
import { MatDividerModule } from '@angular/material/divider';
import { JsonViewerComponent } from './viewers/json-viewer/json-viewer.component';
import { CsvViewerComponent } from './viewers/csv-viewer/csv-viewer.component';
import { HexDumpViewerComponent } from './viewers/hex-dump-viewer/hex-dump-viewer.component';
import { VectorAscViewerComponent } from './viewers/vector-asc-viewer/vector-asc-viewer.component';
import { CandumpViewerComponent } from './viewers/candump-viewer/candump-viewer.component';
import { BlfViewerComponent } from './viewers/blf-viewer/blf-viewer.component';
import { Mf4ViewerComponent } from './viewers/mf4-viewer/mf4-viewer.component';
import { ParquetViewerComponent } from './viewers/parquet-viewer/parquet-viewer.component';
import { RunmanifestViewerComponent } from './viewers/runmanifest-viewer/runmanifest-viewer.component';
import { ExportFile, LocalFileSaveService } from './export-files/local-file-save.service';
import { parseTracksterBin, ParsedTracksterBin } from './parser/decoder.bin.parser';

export interface S3TreeNode {
  name: string;
  key: string;
  children?: S3TreeNode[];
}

export type ExportFileFormat =
  | 'bin'
  | 'txt'
  | 'json'
  | 'csv'
  | 'vectorasc'
  | 'blf'
  | 'mf4'
  | 'parquet';

type ExportDialogState =
  | 'idle'
  | 'running'
  | 'success'
  | 'error';

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  s3CsvBucket?: string;
  s3VectorAscBucket?: string;
  customerId?: string;
  clientId?: string;
  decoderApi?: {
    csvExportUrl?: string;
    vectorAscExportUrl?: string;
  };
}

interface DecodedSignalExportRow {
  blockIndex: string;
  frameOffset: string;
  canId: string;
  messageName: string;
  signalName: string;
  value: string;
  raw: string;
  unit: string;
}

interface CsvExportManifest {
  outputKey?: string;
}

interface HexDumpExportRow {
  offset: string;
  hex: string;
  ascii: string;
}

@Component({
  selector: 'app-decoder',
  standalone: true,
  imports: [
    CommonModule,
    MatTreeModule,
    MatIconModule,
    FormsModule,
    MatMenuModule,
    MatFormFieldModule,
    MatSelectModule,
    MatCheckboxModule,
    TracksterBinViewerComponent,
    DecodedSignalsViewerComponent,
    MatDividerModule,
    JsonViewerComponent,
    CsvViewerComponent,
    HexDumpViewerComponent,
    VectorAscViewerComponent,
    CandumpViewerComponent,
    BlfViewerComponent,
    Mf4ViewerComponent,
    ParquetViewerComponent,
    RunmanifestViewerComponent
  ],
  templateUrl: './decoder.component.html',
  styleUrl: './decoder.component.css'
})
export class DecoderComponent implements OnInit {

  selectedViewerMode = 'trackster-bin';

  isDecoderFilterOpen = false;

  isLoadingBinCatalog = false;

  selectedS3Key: string | null = null;

  selectedBinNode: S3TreeNode | null = null;

  selectedBinKeys: string[] = [];

  exportDialogVisible = false;

  exportDialogState: ExportDialogState = 'idle';

  exportDialogTitle = '';

  exportDialogMessage = '';

  exportDialogDetails = '';

  private readonly hexDumpBytesPerRow = 16;

  readonly s3TreeControl = new NestedTreeControl<S3TreeNode>(
    node => node.children ?? []
  );

  readonly s3TreeDataSource =
    new MatTreeNestedDataSource<S3TreeNode>();

  constructor(
    private readonly localFileSaveService: LocalFileSaveService
  ) {}

  async ngOnInit(): Promise<void> {
    await this.loadS3GeneratedFilesTree();
  }

  hasChild = (_: number, node: S3TreeNode): boolean => {
    return !!node.children && node.children.length > 0;
  };

  async selectS3Node(node: S3TreeNode): Promise<void> {
    this.selectedS3Key = node.key;

    if (!this.isBinFile(node)) {
      return;
    }

    this.selectedBinNode = node;
  }

  isBinFile(node: S3TreeNode): boolean {
    return node.name.toLowerCase().endsWith('.bin');
  }

  isJsonFile(node: S3TreeNode): boolean {
    return node.name.toLowerCase().endsWith('.json');
  }

  isBinSelected(node: S3TreeNode): boolean {
    return this.selectedBinKeys.includes(node.key);
  }

  toggleBinSelection(node: S3TreeNode, checked: boolean): void {
    if (!this.isBinFile(node)) {
      return;
    }

    if (checked) {
      if (!this.selectedBinKeys.includes(node.key)) {
        this.selectedBinKeys = [
          ...this.selectedBinKeys,
          node.key
        ];
      }

      return;
    }

    this.selectedBinKeys = this.selectedBinKeys
      .filter(key => key !== node.key);
  }

  isFolderFullySelected(node: S3TreeNode): boolean {
    const binKeys = this.getBinKeysFromFolder(node);

    if (binKeys.length === 0) {
      return false;
    }

    return binKeys.every(key =>
      this.selectedBinKeys.includes(key)
    );
  }

  isFolderPartiallySelected(node: S3TreeNode): boolean {
    const binKeys = this.getBinKeysFromFolder(node);

    if (binKeys.length === 0) {
      return false;
    }

    const selectedCount = binKeys
      .filter(key => this.selectedBinKeys.includes(key))
      .length;

    return (
      selectedCount > 0 &&
      selectedCount < binKeys.length
    );
  }

  toggleFolderSelection(
    node: S3TreeNode,
    checked: boolean
  ): void {

    const folderBinKeys =
      this.getBinKeysFromFolder(node);

    if (folderBinKeys.length === 0) {
      return;
    }

    if (checked) {
      const mergedKeys = new Set<string>([
        ...this.selectedBinKeys,
        ...folderBinKeys
      ]);

      this.selectedBinKeys = [...mergedKeys];

      return;
    }

    const folderKeys = new Set<string>(
      folderBinKeys
    );

    this.selectedBinKeys = this.selectedBinKeys
      .filter(key => !folderKeys.has(key));
  }

  public async exportCurrentFileWithDialog(): Promise<void> {
    await this.runExportWithDialog(
      async () => await this.exportCurrentFile(),
      'File saved successfully.'
    );
  }

  public async exportSelectedFilesWithDialog(): Promise<void> {
    await this.runExportWithDialog(
      async () => await this.exportSelectedFiles(),
      'ZIP file saved successfully.'
    );
  }

  public async exportSelectedFoldersWithDialog(): Promise<void> {
    await this.runExportWithDialog(
      async () => await this.exportSelectedFolders(),
      'ZIP file saved successfully.'
    );
  }

  public closeExportDialog(): void {
    if (this.exportDialogState === 'running') {
      return;
    }

    this.exportDialogVisible = false;
    this.exportDialogState = 'idle';
    this.exportDialogTitle = '';
    this.exportDialogMessage = '';
    this.exportDialogDetails = '';
  }

  public async exportCurrentFile(): Promise<boolean> {
    if (this.selectedViewerMode === 'trackster-bin') {
      return await this.exportCurrentTracksterBinFile();
    }

    if (this.selectedViewerMode === 'decoded-signals') {
      return await this.exportCurrentDecodedSignalsFile();
    }

    if (this.selectedViewerMode === 'json') {
      return await this.exportCurrentJsonFile();
    }

    if (this.selectedViewerMode === 'csv') {
      return await this.exportCurrentCsvFile();
    }

    if (this.selectedViewerMode === 'hex-dump') {
      return await this.exportCurrentHexDumpFile();
    }

    if (this.selectedViewerMode === 'vector-asc') {
      return await this.exportCurrentVectorAscFile();
    }

    throw new Error(
      `Export for viewer mode "${this.selectedViewerMode}" is not integrated yet.`
    );
  }

  public async exportSelectedFiles(): Promise<boolean> {
    if (this.selectedViewerMode === 'trackster-bin') {
      return await this.exportSelectedTracksterBinFiles();
    }

    if (this.selectedViewerMode === 'decoded-signals') {
      return await this.exportSelectedDecodedSignalsFiles();
    }

    if (this.selectedViewerMode === 'json') {
      return await this.exportSelectedJsonFiles();
    }

    if (this.selectedViewerMode === 'csv') {
      return await this.exportSelectedCsvFiles();
    }

    if (this.selectedViewerMode === 'hex-dump') {
      return await this.exportSelectedHexDumpFiles();
    }

    if (this.selectedViewerMode === 'vector-asc') {
      return await this.exportSelectedVectorAscFiles();
    }

    throw new Error(
      `Selected files export for viewer mode "${this.selectedViewerMode}" is not integrated yet.`
    );
  }

  public async exportSelectedFolders(): Promise<boolean> {
    if (this.selectedViewerMode === 'trackster-bin') {
      return await this.exportSelectedTracksterBinFolders();
    }

    if (this.selectedViewerMode === 'decoded-signals') {
      return await this.exportSelectedDecodedSignalsFolders();
    }

    if (this.selectedViewerMode === 'json') {
      return await this.exportSelectedJsonFolders();
    }

    if (this.selectedViewerMode === 'csv') {
      return await this.exportSelectedCsvFolders();
    }

    if (this.selectedViewerMode === 'hex-dump') {
      return await this.exportSelectedHexDumpFolders();
    }

    if (this.selectedViewerMode === 'vector-asc') {
      return await this.exportSelectedVectorAscFolders();
    }

    throw new Error(
      `Folder export for viewer mode "${this.selectedViewerMode}" is not integrated yet.`
    );
  }

  public async exportCurrentTracksterBinFile(): Promise<boolean> {
    if (!this.selectedBinNode) {
      throw new Error('No BIN file selected for export.');
    }

    if (!this.isBinFile(this.selectedBinNode)) {
      throw new Error('Selected node is not a BIN file.');
    }

    const fileName =
      this.normalizeExportFileName(
        this.selectedBinNode.name,
        'bin'
      );

    return await this.localFileSaveService.saveFileFromProvider({
      fileName,
      mimeType: 'application/octet-stream',
      blobProvider: async () => {
        const config =
          await this.loadRuntimeConfig();

        const bucket =
          config.s3Default?.trim();

        if (!bucket) {
          throw new Error(
            'Missing s3Default in assets/config.json'
          );
        }

        const s3Client =
          await this.getS3Client();

        const response =
          await s3Client.send(
            new GetObjectCommand({
              Bucket: bucket,
              Key: this.selectedBinNode!.key
            })
          );

        const content =
          await this.readS3BodyAsArrayBuffer(
            response.Body
          );

        return this.buildExportBlob(
          content,
          'application/octet-stream'
        );
      }
    });
  }

  public async exportCurrentDecodedSignalsFile(): Promise<boolean> {
    if (!this.selectedBinNode) {
      throw new Error('No BIN file selected for decoded signals export.');
    }

    const fileName =
      this.getDecodedSignalsFileNameFromBinName(
        this.selectedBinNode.name
      );

    return await this.localFileSaveService.saveFileFromProvider({
      fileName,
      mimeType: 'text/plain;charset=utf-8',
      blobProvider: async () => {
        const decodedText =
          await this.buildDecodedSignalsTextForKey(
            this.selectedBinNode!.key,
            this.selectedBinNode!.name
          );

        return this.buildExportBlob(
          decodedText,
          'text/plain;charset=utf-8'
        );
      }
    });
  }

  public async exportCurrentJsonFile(): Promise<boolean> {
    if (!this.selectedBinNode) {
      throw new Error('No BIN file selected for JSON export.');
    }

    const fileName =
      this.getJsonFileNameFromBinName(
        this.selectedBinNode.name
      );

    return await this.localFileSaveService.saveFileFromProvider({
      fileName,
      mimeType: 'application/json;charset=utf-8',
      blobProvider: async () => {
        const jsonText =
          await this.buildJsonTextForKey(
            this.selectedBinNode!.key
          );

        return this.buildExportBlob(
          jsonText,
          'application/json;charset=utf-8'
        );
      }
    });
  }

  public async exportCurrentCsvFile(): Promise<boolean> {
    if (!this.selectedBinNode) {
      throw new Error('No BIN file selected for CSV export.');
    }

    const fileName =
      this.getCsvFileNameFromBinName(
        this.selectedBinNode.name
      );

    return await this.localFileSaveService.saveFileFromProvider({
      fileName,
      mimeType: 'text/csv;charset=utf-8',
      blobProvider: async () => {
        const csvBuffer =
          await this.loadCsvOutputBufferForKey(
            this.selectedBinNode!.key
          );

        return this.buildExportBlob(
          csvBuffer,
          'text/csv;charset=utf-8'
        );
      }
    });
  }

  public async exportCurrentHexDumpFile(): Promise<boolean> {
    if (!this.selectedBinNode) {
      throw new Error('No BIN file selected for HEX dump export.');
    }

    const fileName =
      this.getHexDumpFileNameFromBinName(
        this.selectedBinNode.name
      );

    return await this.localFileSaveService.saveFileFromProvider({
      fileName,
      mimeType: 'text/plain;charset=utf-8',
      blobProvider: async () => {
        const hexDumpText =
          await this.buildHexDumpTextForKey(
            this.selectedBinNode!.key,
            this.selectedBinNode!.name
          );

        return this.buildExportBlob(
          hexDumpText,
          'text/plain;charset=utf-8'
        );
      }
    });
  }

  public async exportCurrentVectorAscFile(): Promise<boolean> {
    if (!this.selectedBinNode) {
      throw new Error('No BIN file selected for Vector ASC export.');
    }

    const fileName =
      this.getVectorAscFileNameFromBinName(
        this.selectedBinNode.name
      );

    return await this.localFileSaveService.saveFileFromProvider({
      fileName,
      mimeType: 'text/plain;charset=utf-8',
      blobProvider: async () => {
        const ascBuffer =
          await this.loadVectorAscOutputBufferForKey(
            this.selectedBinNode!.key
          );

        return this.buildExportBlob(
          ascBuffer,
          'text/plain;charset=utf-8'
        );
      }
    });
  }

  public async exportSelectedTracksterBinFiles(): Promise<boolean> {
    const uniqueSelectedKeys =
      this.getUniqueSelectedBinKeys();

    if (uniqueSelectedKeys.length === 0) {
      throw new Error('No BIN files selected for export.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadBinFilesForZip(uniqueSelectedKeys);
      },
      'trackster-selected-bin-files.zip'
    );
  }

  public async exportSelectedTracksterBinFolders(): Promise<boolean> {
    const folderBinKeys =
      this.getSelectedFolderBinKeys();

    if (folderBinKeys.length === 0) {
      throw new Error('Selected folders do not contain BIN files.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadBinFilesForZip(folderBinKeys);
      },
      'trackster-selected-bin-folders.zip'
    );
  }

  public async exportSelectedDecodedSignalsFiles(): Promise<boolean> {
    const uniqueSelectedKeys =
      this.getUniqueSelectedBinKeys();

    if (uniqueSelectedKeys.length === 0) {
      throw new Error('No BIN files selected for decoded signals export.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadDecodedSignalTextFilesForZip(
          uniqueSelectedKeys
        );
      },
      'trackster-selected-decoded-signals.zip'
    );
  }

  public async exportSelectedDecodedSignalsFolders(): Promise<boolean> {
    const folderBinKeys =
      this.getSelectedFolderBinKeys();

    if (folderBinKeys.length === 0) {
      throw new Error('Selected folders do not contain BIN files.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadDecodedSignalTextFilesForZip(
          folderBinKeys
        );
      },
      'trackster-selected-decoded-signal-folders.zip'
    );
  }

  public async exportSelectedJsonFiles(): Promise<boolean> {
    const uniqueSelectedKeys =
      this.getUniqueSelectedBinKeys();

    if (uniqueSelectedKeys.length === 0) {
      throw new Error('No BIN files selected for JSON export.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadJsonFilesForZip(
          uniqueSelectedKeys
        );
      },
      'trackster-selected-json-files.zip'
    );
  }

  public async exportSelectedJsonFolders(): Promise<boolean> {
    const folderBinKeys =
      this.getSelectedFolderBinKeys();

    if (folderBinKeys.length === 0) {
      throw new Error('Selected folders do not contain BIN files.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadJsonFilesForZip(
          folderBinKeys
        );
      },
      'trackster-selected-json-folders.zip'
    );
  }

  public async exportSelectedCsvFiles(): Promise<boolean> {
    const uniqueSelectedKeys =
      this.getUniqueSelectedBinKeys();

    if (uniqueSelectedKeys.length === 0) {
      throw new Error('No BIN files selected for CSV export.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadCsvFilesForZip(
          uniqueSelectedKeys
        );
      },
      'trackster-selected-csv-files.zip'
    );
  }

  public async exportSelectedCsvFolders(): Promise<boolean> {
    const folderBinKeys =
      this.getSelectedFolderBinKeys();

    if (folderBinKeys.length === 0) {
      throw new Error('Selected folders do not contain BIN files.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadCsvFilesForZip(
          folderBinKeys
        );
      },
      'trackster-selected-csv-folders.zip'
    );
  }

  public async exportSelectedHexDumpFiles(): Promise<boolean> {
    const uniqueSelectedKeys =
      this.getUniqueSelectedBinKeys();

    if (uniqueSelectedKeys.length === 0) {
      throw new Error('No BIN files selected for HEX dump export.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadHexDumpTextFilesForZip(
          uniqueSelectedKeys
        );
      },
      'trackster-selected-hexdump-files.zip'
    );
  }

  public async exportSelectedHexDumpFolders(): Promise<boolean> {
    const folderBinKeys =
      this.getSelectedFolderBinKeys();

    if (folderBinKeys.length === 0) {
      throw new Error('Selected folders do not contain BIN files.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadHexDumpTextFilesForZip(
          folderBinKeys
        );
      },
      'trackster-selected-hexdump-folders.zip'
    );
  }

  public async exportSelectedVectorAscFiles(): Promise<boolean> {
    const uniqueSelectedKeys =
      this.getUniqueSelectedBinKeys();

    if (uniqueSelectedKeys.length === 0) {
      throw new Error('No BIN files selected for Vector ASC export.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadVectorAscFilesForZip(
          uniqueSelectedKeys
        );
      },
      'trackster-selected-vectorasc-files.zip'
    );
  }

  public async exportSelectedVectorAscFolders(): Promise<boolean> {
    const folderBinKeys =
      this.getSelectedFolderBinKeys();

    if (folderBinKeys.length === 0) {
      throw new Error('Selected folders do not contain BIN files.');
    }

    return await this.localFileSaveService.saveFiles(
      async () => {
        return await this.loadVectorAscFilesForZip(
          folderBinKeys
        );
      },
      'trackster-selected-vectorasc-folders.zip'
    );
  }

  public async saveGeneratedExportFile(
    fileNameBase: string,
    format: ExportFileFormat,
    content: string | ArrayBuffer | Uint8Array
  ): Promise<boolean> {

    const mimeType =
      this.getExportMimeType(format);

    const blob =
      this.buildExportBlob(
        content,
        mimeType
      );

    const fileName =
      this.normalizeExportFileName(
        fileNameBase,
        format
      );

    return await this.localFileSaveService.saveFile({
      fileName,
      blob,
      mimeType
    });
  }

  private async runExportWithDialog(
    action: () => Promise<boolean>,
    successMessage: string
  ): Promise<void> {

    this.exportDialogVisible = true;
    this.exportDialogState = 'running';
    this.exportDialogTitle = 'Exporting...';
    this.exportDialogMessage =
      'Please wait while Trackster prepares the export.';
    this.exportDialogDetails =
      'Do not close this window.';

    try {
      const completed =
        await action();

      if (!completed) {
        this.closeExportDialog();
        return;
      }

      this.exportDialogState = 'success';
      this.exportDialogTitle = 'Export completed';
      this.exportDialogMessage = successMessage;
      this.exportDialogDetails = '';

    } catch (error: unknown) {

      this.exportDialogState = 'error';
      this.exportDialogTitle = 'Export failed';
      this.exportDialogMessage =
        this.getErrorMessage(error);
      this.exportDialogDetails =
        'Please try again or check the browser console for details.';

      console.error(
        '[Trackster] Export failed.',
        error
      );
    }
  }

  private async loadBinFilesForZip(
    keys: string[]
  ): Promise<ExportFile[]> {

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Default?.trim();

    if (!bucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    const s3Client =
      await this.getS3Client();

    const files: ExportFile[] = [];

    for (const key of keys) {
      const response =
        await s3Client.send(
          new GetObjectCommand({
            Bucket: bucket,
            Key: key
          })
        );

      const content =
        await this.readS3BodyAsArrayBuffer(
          response.Body
        );

      files.push({
        fileName: this.getZipEntryNameFromBinKey(key),
        blob: this.buildExportBlob(
          content,
          'application/octet-stream'
        )
      });
    }

    return files;
  }

  private async loadDecodedSignalTextFilesForZip(
    keys: string[]
  ): Promise<ExportFile[]> {

    const files: ExportFile[] = [];

    for (const key of keys) {
      const sourceFileName =
        this.getFileNameFromS3Key(key);

      const content =
        await this.buildDecodedSignalsTextForKey(
          key,
          sourceFileName
        );

      files.push({
        fileName: this.getDecodedSignalsZipEntryNameFromBinKey(key),
        blob: this.buildExportBlob(
          content,
          'text/plain;charset=utf-8'
        )
      });
    }

    return files;
  }

  private async loadJsonFilesForZip(
    keys: string[]
  ): Promise<ExportFile[]> {

    const files: ExportFile[] = [];

    for (const key of keys) {
      const jsonText =
        await this.buildJsonTextForKey(key);

      files.push({
        fileName: this.getJsonZipEntryNameFromBinKey(key),
        blob: this.buildExportBlob(
          jsonText,
          'application/json;charset=utf-8'
        )
      });
    }

    return files;
  }

  private async loadCsvFilesForZip(
    keys: string[]
  ): Promise<ExportFile[]> {

    const files: ExportFile[] = [];

    for (const key of keys) {
      const csvBuffer =
        await this.loadCsvOutputBufferForKey(key);

      files.push({
        fileName: this.getCsvZipEntryNameFromBinKey(key),
        blob: this.buildExportBlob(
          csvBuffer,
          'text/csv;charset=utf-8'
        )
      });
    }

    return files;
  }

  private async loadHexDumpTextFilesForZip(
    keys: string[]
  ): Promise<ExportFile[]> {

    const files: ExportFile[] = [];

    for (const key of keys) {
      const sourceFileName =
        this.getFileNameFromS3Key(key);

      const content =
        await this.buildHexDumpTextForKey(
          key,
          sourceFileName
        );

      files.push({
        fileName: this.getHexDumpZipEntryNameFromBinKey(key),
        blob: this.buildExportBlob(
          content,
          'text/plain;charset=utf-8'
        )
      });
    }

    return files;
  }

  private async loadVectorAscFilesForZip(
    keys: string[]
  ): Promise<ExportFile[]> {

    const files: ExportFile[] = [];

    for (const key of keys) {
      const ascBuffer =
        await this.loadVectorAscOutputBufferForKey(key);

      files.push({
        fileName: this.getVectorAscZipEntryNameFromBinKey(key),
        blob: this.buildExportBlob(
          ascBuffer,
          'text/plain;charset=utf-8'
        )
      });
    }

    return files;
  }

  private async buildDecodedSignalsTextForKey(
    binKey: string,
    sourceFileName: string
  ): Promise<string> {

    const parsed =
      await this.parseBinKeyWithManifest(binKey);

    return this.buildDecodedSignalsTextExport(
      sourceFileName,
      parsed
    );
  }

  private async buildJsonTextForKey(
    binKey: string
  ): Promise<string> {

    const parsed =
      await this.parseBinKeyWithManifest(binKey);

    const messages =
      this.buildDecodedMessagesJson(parsed);

    return JSON.stringify(
      messages,
      null,
      2
    );
  }

  private async buildHexDumpTextForKey(
    binKey: string,
    sourceFileName: string
  ): Promise<string> {

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Default?.trim();

    if (!bucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    const buffer =
      await this.getS3ObjectBuffer(
        bucket,
        binKey
      );

    return this.buildHexDumpTextExport(
      sourceFileName,
      buffer
    );
  }

  private buildHexDumpTextExport(
    sourceFileName: string,
    buffer: ArrayBuffer
  ): string {

    const bytes =
      new Uint8Array(buffer);

    const rows =
      this.buildHexDumpRows(bytes);

    const lines: string[] = [];

    lines.push('Trackster HEX Dump');
    lines.push(`Source file: ${sourceFileName}`);
    lines.push(`File size: ${bytes.byteLength.toLocaleString()} bytes`);
    lines.push(`Rows: ${rows.length.toLocaleString()}`);
    lines.push(`Generated at: ${new Date().toISOString()}`);
    lines.push('');

    if (rows.length === 0) {
      lines.push('No HEX dump content available.');
      lines.push('');
      return lines.join('\n');
    }

    for (const row of rows) {
      lines.push(
        `${row.offset}  ${row.hex.padEnd(47, ' ')}  ${row.ascii}`
      );
    }

    lines.push('');

    return lines.join('\n');
  }

  private buildHexDumpRows(
    bytes: Uint8Array
  ): HexDumpExportRow[] {

    const rows: HexDumpExportRow[] = [];

    for (
      let offset = 0;
      offset < bytes.length;
      offset += this.hexDumpBytesPerRow
    ) {

      const chunk =
        bytes.slice(
          offset,
          Math.min(
            offset + this.hexDumpBytesPerRow,
            bytes.length
          )
        );

      rows.push({
        offset:
          this.formatHexDumpOffset(offset),
        hex:
          this.formatHexDumpBytes(chunk),
        ascii:
          this.formatHexDumpAscii(chunk)
      });
    }

    return rows;
  }

  private formatHexDumpOffset(
    offset: number
  ): string {

    return `0x${offset
      .toString(16)
      .toUpperCase()
      .padStart(8, '0')}`;
  }

  private formatHexDumpBytes(
    bytes: Uint8Array
  ): string {

    return Array.from(bytes)
      .map(byte =>
        byte
          .toString(16)
          .toUpperCase()
          .padStart(2, '0')
      )
      .join(' ');
  }

  private formatHexDumpAscii(
    bytes: Uint8Array
  ): string {

    return Array.from(bytes)
      .map(byte => {
        if (
          byte >= 32 &&
          byte <= 126
        ) {
          return String.fromCharCode(byte);
        }

        return '.';
      })
      .join('');
  }

  private async parseBinKeyWithManifest(
    binKey: string
  ): Promise<ParsedTracksterBin> {

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Default?.trim();

    if (!bucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    const binBuffer =
      await this.getS3ObjectBuffer(
        bucket,
        binKey
      );

    const manifest =
      await this.loadRunManifestForBinKey(
        bucket,
        binKey,
        config
      );

    return parseTracksterBin(
      binBuffer,
      manifest
    );
  }

  private async loadRunManifestForBinKey(
    bucket: string,
    binKey: string,
    config: RuntimeConfig
  ): Promise<unknown> {

    const clientId =
      this.resolveClientId(config);

    const runId =
      this.getRunIdFromKey(binKey);

    if (!runId) {
      return null;
    }

    const manifestKey =
      `${clientId}/${runId}/run-manifest.json`;

    try {
      const manifestBuffer =
        await this.getS3ObjectBuffer(
          bucket,
          manifestKey
        );

      const manifestText =
        new TextDecoder('utf-8')
          .decode(manifestBuffer);

      return JSON.parse(manifestText);

    } catch (error) {

      console.warn(
        '[Trackster] Run manifest not available for export.',
        error
      );

      return null;
    }
  }

  private buildDecodedMessagesJson(
    parsed: ParsedTracksterBin
  ): any[] {

    const messages: any[] = [];

    const blocks =
      Array.isArray(parsed.blocks)
        ? parsed.blocks
        : [];

    const firstTimestampNs =
      blocks[0]?.timestampNs ?? '0';

    for (const block of blocks) {

      const blockTimestampNs =
        block.timestampNs ?? firstTimestampNs;

      for (const frame of block.frames ?? []) {

        const signals:
          Record<string, unknown> = {};

        for (const signal of frame.signals ?? []) {
          signals[signal.name] = signal.value;
        }

        messages.push({
          timestamp:
            this.calculateFrameTimestampSeconds(
              firstTimestampNs,
              blockTimestampNs,
              frame.timestampDeltaNs
            ),

          canId:
            frame.canIdHex,

          name:
            frame.messageName ||
            `CAN_${frame.canIdHex}`,

          dlc:
            frame.payloadLength,

          data:
            this.normalizePayloadHex(
              frame.payloadBytes
            ),

          signals
        });
      }
    }

    return messages;
  }

  private calculateFrameTimestampSeconds(
    firstTimestampNs: string,
    blockTimestampNs: string,
    frameDeltaNs: string | number
  ): number {

    const baseNs =
      BigInt(
        firstTimestampNs || '0'
      );

    const blockNs =
      BigInt(
        blockTimestampNs || '0'
      );

    const deltaNs =
      BigInt(
        frameDeltaNs ?? 0
      );

    const absoluteNs =
      blockNs + deltaNs;

    const relativeNs =
      absoluteNs - baseNs;

    return Number(relativeNs) /
      1_000_000_000;
  }

  private normalizePayloadHex(
    payload: string
  ): string {

    if (!payload) {
      return '';
    }

    return payload
      .replace(/\s+/g, '')
      .toUpperCase();
  }

  private async loadCsvOutputBufferForKey(
    binKey: string
  ): Promise<ArrayBuffer> {

    const config =
      await this.loadRuntimeConfig();

    const outputBucket =
      config.s3CsvBucket?.trim();

    if (!outputBucket) {
      throw new Error(
        'Missing s3CsvBucket in assets/config.json'
      );
    }

    await this.exportCsvWithLambda(
      binKey,
      config
    );

    const manifest =
      await this.loadCsvManifest(
        binKey,
        outputBucket
      );

    const outputKey =
      manifest.outputKey?.trim();

    if (!outputKey) {
      throw new Error(
        `CSV manifest does not contain outputKey for ${binKey}.`
      );
    }

    return await this.getS3ObjectBuffer(
      outputBucket,
      outputKey
    );
  }

  private async exportCsvWithLambda(
    binKey: string,
    config: RuntimeConfig
  ): Promise<void> {

    const exportUrl =
      config.decoderApi?.csvExportUrl?.trim();

    if (!exportUrl) {
      throw new Error(
        'Missing decoderApi.csvExportUrl in assets/config.json'
      );
    }

    const inputBucketName =
      config.s3Default?.trim() ||
      's3-trackster-can-bucket';

    const outputBucketName =
      config.s3CsvBucket?.trim();

    if (!outputBucketName) {
      throw new Error(
        'Missing s3CsvBucket in assets/config.json'
      );
    }

    const clientId =
      this.resolveClientId(config);

    const token =
      await this.getIdToken();

    const response =
      await fetch(
        exportUrl,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`
          },
          body: JSON.stringify({
            inputBucketName,
            outputBucketName,
            clientId,
            inputKeys: [
              binKey
            ]
          })
        }
      );

    const responseText =
      await response.text();

    const result =
      responseText
        ? JSON.parse(responseText)
        : {};

    if (!response.ok) {
      throw new Error(
        result.message ||
        `CSV export failed. HTTP ${response.status}`
      );
    }
  }

  private async loadCsvManifest(
    binKey: string,
    bucket: string
  ): Promise<CsvExportManifest> {

    const manifestKey =
      this.buildCsvManifestKey(binKey);

    const buffer =
      await this.getS3ObjectBuffer(
        bucket,
        manifestKey
      );

    const manifestText =
      new TextDecoder('utf-8')
        .decode(buffer);

    return JSON.parse(manifestText);
  }

  private buildCsvManifestKey(
    inputKey: string
  ): string {

    return inputKey
      .replace(/\.[^.]+$/, '.csv.json');
  }

  private async loadVectorAscOutputBufferForKey(
    binKey: string
  ): Promise<ArrayBuffer> {

    const config =
      await this.loadRuntimeConfig();

    const outputBucket =
      config.s3VectorAscBucket?.trim();

    if (!outputBucket) {
      throw new Error(
        'Missing s3VectorAscBucket in assets/config.json'
      );
    }

    const outputKey =
      await this.exportVectorAscWithLambda(
        binKey,
        config
      );

    return await this.getS3ObjectBuffer(
      outputBucket,
      outputKey
    );
  }

  private async exportVectorAscWithLambda(
    binKey: string,
    config: RuntimeConfig
  ): Promise<string> {

    const exportUrl =
      config.decoderApi?.vectorAscExportUrl?.trim();

    if (!exportUrl) {
      throw new Error(
        'Missing decoderApi.vectorAscExportUrl in assets/config.json'
      );
    }

    const inputBucket =
      config.s3Default?.trim();

    if (!inputBucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    const outputBucket =
      config.s3VectorAscBucket?.trim();

    if (!outputBucket) {
      throw new Error(
        'Missing s3VectorAscBucket in assets/config.json'
      );
    }

    const clientId =
      this.resolveClientId(config);

    const runId =
      this.getRunIdFromKey(binKey);

    const outputFileName =
      this.getVectorAscFileNameFromBinName(
        this.getFileNameFromS3Key(binKey)
      );

    const outputKey =
      `${clientId}/${runId}/${outputFileName}`;

    const manifestKey =
      `${clientId}/${runId}/run-manifest.json`;

    const token =
      await this.getIdToken();

    const response =
      await fetch(
        exportUrl,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`
          },
          body: JSON.stringify({
            clientId,
            runId,
            inputBucket,
            inputKey: binKey,
            manifestBucket: inputBucket,
            manifestKey,
            outputBucket,
            outputKey,
            outputFileName
          })
        }
      );

    const responseText =
      await response.text();

    const result =
      responseText
        ? JSON.parse(responseText)
        : {};

    if (!response.ok) {
      throw new Error(
        result.message ||
        `Vector ASC export failed. HTTP ${response.status}`
      );
    }

    return outputKey;
  }

  private buildDecodedSignalsTextExport(
    sourceFileName: string,
    parsed: ParsedTracksterBin
  ): string {

    const rows =
      this.buildDecodedSignalExportRows(parsed);

    const headers = [
      'Block',
      'Frame Offset',
      'CAN ID',
      'Message',
      'Signal',
      'Value',
      'Raw',
      'Unit'
    ];

    const tableRows =
      rows.map(row => [
        row.blockIndex,
        row.frameOffset,
        row.canId,
        row.messageName,
        row.signalName,
        row.value,
        row.raw,
        row.unit
      ]);

    const columnWidths =
      this.calculateTextTableColumnWidths(
        headers,
        tableRows
      );

    const lines: string[] = [];

    lines.push('Trackster Decoded Signals');
    lines.push(`Source file: ${sourceFileName}`);
    lines.push(`Blocks: ${parsed.blockCount.toLocaleString()}`);
    lines.push(`Frames: ${parsed.totalFrameCount.toLocaleString()}`);
    lines.push(`Decoded signal samples: ${rows.length.toLocaleString()}`);
    lines.push(`Generated at: ${new Date().toISOString()}`);
    lines.push('');

    if (rows.length === 0) {
      lines.push('No decoded signal samples were found.');
      lines.push('');
      return lines.join('\n');
    }

    lines.push(
      this.formatTextTableRow(
        headers,
        columnWidths
      )
    );

    lines.push(
      this.formatTextTableSeparator(
        columnWidths
      )
    );

    for (const row of tableRows) {
      lines.push(
        this.formatTextTableRow(
          row,
          columnWidths
        )
      );
    }

    lines.push('');

    return lines.join('\n');
  }

  private buildDecodedSignalExportRows(
    parsed: ParsedTracksterBin
  ): DecodedSignalExportRow[] {

    const rows: DecodedSignalExportRow[] = [];

    for (const block of parsed.blocks) {
      for (const frame of block.frames) {
        for (const signal of frame.signals) {
          rows.push({
            blockIndex: block.blockIndex.toString(),
            frameOffset: frame.offset.toString(),
            canId: frame.canIdHex,
            messageName: frame.messageName,
            signalName: signal.name,
            value: signal.value,
            raw: signal.raw,
            unit: signal.unit || ''
          });
        }
      }
    }

    return rows;
  }

  private calculateTextTableColumnWidths(
    headers: string[],
    rows: string[][]
  ): number[] {

    return headers.map((header, columnIndex) => {
      const rowWidths =
        rows.map(row =>
          String(row[columnIndex] ?? '').length
        );

      return Math.max(
        header.length,
        ...rowWidths
      );
    });
  }

  private formatTextTableRow(
    values: string[],
    columnWidths: number[]
  ): string {

    return values
      .map((value, index) =>
        String(value ?? '').padEnd(columnWidths[index], ' ')
      )
      .join('  ');
  }

  private formatTextTableSeparator(
    columnWidths: number[]
  ): string {

    return columnWidths
      .map(width => '-'.repeat(width))
      .join('  ');
  }

  private async getS3ObjectBuffer(
    bucket: string,
    key: string
  ): Promise<ArrayBuffer> {

    const s3Client =
      await this.getS3Client();

    const response =
      await s3Client.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: key
        })
      );

    return await this.readS3BodyAsArrayBuffer(
      response.Body
    );
  }

  private getUniqueSelectedBinKeys(): string[] {
    return [...new Set(this.selectedBinKeys)]
      .filter(key => key.toLowerCase().endsWith('.bin'));
  }

  private getSelectedFolderBinKeys(): string[] {
    const selectedFolderKeys =
      this.getSelectedBinParentFolderKeys();

    if (selectedFolderKeys.length === 0) {
      throw new Error('No BIN folders selected for export.');
    }

    return this.getAllBinKeysFromSelectedFolders(
      selectedFolderKeys
    );
  }

  private getSelectedBinParentFolderKeys(): string[] {
    const folders =
      new Set<string>();

    for (const key of this.selectedBinKeys) {
      if (!key.toLowerCase().endsWith('.bin')) {
        continue;
      }

      const folderKey =
        this.getParentFolderKeyFromS3Key(key);

      if (folderKey) {
        folders.add(folderKey);
      }
    }

    return [...folders];
  }

  private getAllBinKeysFromSelectedFolders(
    folderKeys: string[]
  ): string[] {

    const folderKeySet =
      new Set(folderKeys);

    const result =
      new Set<string>();

    const walk = (
      nodes: S3TreeNode[]
    ): void => {

      for (const node of nodes) {
        if (this.isBinFile(node)) {
          const parentFolderKey =
            this.getParentFolderKeyFromS3Key(node.key);

          if (
            parentFolderKey &&
            folderKeySet.has(parentFolderKey)
          ) {
            result.add(node.key);
          }

          continue;
        }

        walk(node.children ?? []);
      }
    };

    walk(this.s3TreeDataSource.data);

    return [...result].sort((a, b) =>
      a.localeCompare(b)
    );
  }

  private getParentFolderKeyFromS3Key(
    key: string
  ): string | null {

    const parts =
      key
        .split('/')
        .filter(Boolean);

    if (parts.length < 2) {
      return null;
    }

    return parts.slice(0, -1).join('/');
  }

  private buildExportBlob(
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
      const arrayBuffer =
        new ArrayBuffer(content.byteLength);

      const view =
        new Uint8Array(arrayBuffer);

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

  private getExportMimeType(
    format: ExportFileFormat
  ): string {

    switch (format) {
      case 'bin':
        return 'application/octet-stream';

      case 'txt':
        return 'text/plain;charset=utf-8';

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

  private normalizeExportFileName(
    baseName: string,
    format: ExportFileFormat
  ): string {

    const cleanBaseName = baseName
      .trim()
      .replace(/[\\/:*?"<>|]+/g, '_');

    const safeBaseName =
      cleanBaseName || 'trackster-export';

    const extension =
      this.getExportExtension(format);

    if (
      safeBaseName
        .toLowerCase()
        .endsWith(`.${extension}`)
    ) {
      return safeBaseName;
    }

    return `${safeBaseName}.${extension}`;
  }

  private getExportExtension(
    format: ExportFileFormat
  ): string {

    switch (format) {
      case 'bin':
        return 'bin';

      case 'txt':
        return 'txt';

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

  private removeFileExtension(
    fileName: string
  ): string {

    return fileName.replace(
      /\.[^/.]+$/,
      ''
    );
  }

  private getDecodedSignalsFileNameFromBinName(
    fileName: string
  ): string {

    const baseName =
      this.removeFileExtension(fileName);

    return this.normalizeExportFileName(
      `${baseName}.decoded-signals`,
      'txt'
    );
  }

  private getJsonFileNameFromBinName(
    fileName: string
  ): string {

    const baseName =
      this.removeFileExtension(fileName);

    return this.normalizeExportFileName(
      baseName,
      'json'
    );
  }

  private getCsvFileNameFromBinName(
    fileName: string
  ): string {

    const baseName =
      this.removeFileExtension(fileName);

    return this.normalizeExportFileName(
      baseName,
      'csv'
    );
  }

  private getHexDumpFileNameFromBinName(
    fileName: string
  ): string {

    const baseName =
      this.removeFileExtension(fileName);

    return `${baseName}.hex`;
  }

  private getVectorAscFileNameFromBinName(
    fileName: string
  ): string {

    const baseName =
      this.removeFileExtension(fileName);

    return this.normalizeExportFileName(
      baseName,
      'vectorasc'
    );
  }

  private getDecodedSignalsZipEntryNameFromBinKey(
    key: string
  ): string {

    const entryName =
      this.getZipEntryNameFromBinKey(key);

    const parts =
      entryName
        .split('/')
        .filter(Boolean);

    const lastIndex =
      parts.length - 1;

    if (lastIndex < 0) {
      return 'trackster-export.decoded-signals.txt';
    }

    const baseName =
      this.removeFileExtension(parts[lastIndex]);

    parts[lastIndex] =
      `${baseName}.decoded-signals.txt`;

    return parts.join('/');
  }

  private getJsonZipEntryNameFromBinKey(
    key: string
  ): string {

    return this.replaceZipEntryExtension(
      key,
      'json'
    );
  }

  private getCsvZipEntryNameFromBinKey(
    key: string
  ): string {

    return this.replaceZipEntryExtension(
      key,
      'csv'
    );
  }

  private getHexDumpZipEntryNameFromBinKey(
    key: string
  ): string {

    const entryName =
      this.getZipEntryNameFromBinKey(key);

    const parts =
      entryName
        .split('/')
        .filter(Boolean);

    const lastIndex =
      parts.length - 1;

    if (lastIndex < 0) {
      return 'trackster-export.hex';
    }

    const baseName =
      this.removeFileExtension(parts[lastIndex]);

    parts[lastIndex] =
      `${baseName}.hex`;

    return parts.join('/');
  }

  private getVectorAscZipEntryNameFromBinKey(
    key: string
  ): string {

    return this.replaceZipEntryExtension(
      key,
      'asc'
    );
  }

  private replaceZipEntryExtension(
    key: string,
    extension: string
  ): string {

    const entryName =
      this.getZipEntryNameFromBinKey(key);

    const parts =
      entryName
        .split('/')
        .filter(Boolean);

    const lastIndex =
      parts.length - 1;

    if (lastIndex < 0) {
      return `trackster-export.${extension}`;
    }

    const baseName =
      this.removeFileExtension(parts[lastIndex]);

    parts[lastIndex] =
      `${baseName}.${extension}`;

    return parts.join('/');
  }

  private async readS3BodyAsArrayBuffer(
    body: unknown
  ): Promise<ArrayBuffer> {

    if (!body) {
      throw new Error(
        'S3 object body is empty.'
      );
    }

    const transformedBody = body as {
      transformToByteArray?: () => Promise<Uint8Array>;
    };

    if (
      typeof transformedBody.transformToByteArray === 'function'
    ) {
      const bytes =
        await transformedBody.transformToByteArray();

      return this.copyUint8ArrayToArrayBuffer(bytes);
    }

    if (body instanceof Blob) {
      return await body.arrayBuffer();
    }

    if (body instanceof ArrayBuffer) {
      return body;
    }

    if (body instanceof Uint8Array) {
      return this.copyUint8ArrayToArrayBuffer(body);
    }

    if (body instanceof ReadableStream) {
      return await new Response(body).arrayBuffer();
    }

    if (typeof body === 'string') {
      return new TextEncoder().encode(body).buffer;
    }

    throw new Error(
      'Unsupported S3 object body type.'
    );
  }

  private copyUint8ArrayToArrayBuffer(
    bytes: Uint8Array
  ): ArrayBuffer {

    const arrayBuffer =
      new ArrayBuffer(bytes.byteLength);

    const view =
      new Uint8Array(arrayBuffer);

    view.set(bytes);

    return arrayBuffer;
  }

  private getZipEntryNameFromBinKey(
    key: string
  ): string {

    const parts =
      key
        .split('/')
        .filter(Boolean);

    if (parts.length >= 2) {
      return parts.slice(1).join('/');
    }

    return parts[0] || 'trackster-export.bin';
  }

  private getFileNameFromS3Key(
    key: string
  ): string {

    const parts =
      key
        .split('/')
        .filter(Boolean);

    return parts[parts.length - 1] || 'trackster-export.bin';
  }

  private getRunIdFromKey(
    key: string
  ): string {

    const parts =
      key
        .split('/')
        .filter(Boolean);

    if (parts.length < 2) {
      return '';
    }

    return parts[1];
  }

  private getBinKeysFromFolder(
    node: S3TreeNode
  ): string[] {

    const result: string[] = [];

    const walk = (
      currentNode: S3TreeNode
    ): void => {

      if (this.isBinFile(currentNode)) {
        result.push(currentNode.key);
        return;
      }

      for (
        const child of currentNode.children ?? []
      ) {
        walk(child);
      }
    };

    walk(node);

    return result;
  }

  private getErrorMessage(
    error: unknown
  ): string {

    if (
      error instanceof Error &&
      error.message.trim()
    ) {
      return error.message;
    }

    return 'Unable to export file.';
  }

  private async getIdToken():
    Promise<string> {

    const session =
      await fetchAuthSession();

    const token =
      session.tokens?.idToken?.toString();

    if (!token) {
      throw new Error(
        'Cognito ID token unavailable.'
      );
    }

    return token;
  }

  private async loadS3GeneratedFilesTree(): Promise<void> {
    this.isLoadingBinCatalog = true;

    try {

      const config =
        await this.loadRuntimeConfig();

      const clientId =
        this.resolveClientId(config);

      if (this.shouldUseLocalMock()) {

        this.setTreeData(
          this.buildLocalMockTree()
        );

        return;
      }

      const bucket =
        config.s3Default?.trim();

      if (!bucket) {
        throw new Error(
          'Missing s3Default in assets/config.json'
        );
      }

      const prefix = `${clientId}/`;

      const keys =
        await this.listS3KeysFromBucket(
          bucket,
          prefix
        );

      const tree =
        this.buildTreeFromS3Keys(
          keys,
          clientId
        );

      this.setTreeData(tree);

    } finally {
      this.isLoadingBinCatalog = false;
    }
  }

  private setTreeData(data: S3TreeNode[]): void {
    this.s3TreeControl.expansionModel.clear();

    this.s3TreeDataSource.data = data;

    this.s3TreeControl.dataNodes = data;

    this.selectedBinKeys = this.selectedBinKeys.filter(key =>
      this.treeContainsKey(data, key)
    );

    const firstFolderNode = data.find(node =>
      node.children && node.children.length > 0
    );

    if (firstFolderNode) {
      this.s3TreeControl.expand(firstFolderNode);
    }

    const firstBinNode = this.findFirstBinNode(data);

    if (firstBinNode && !this.selectedBinNode) {
      this.selectedBinNode = firstBinNode;
      this.selectedS3Key = firstBinNode.key;
    }
  }

  private treeContainsKey(
    nodes: S3TreeNode[],
    key: string
  ): boolean {

    for (const node of nodes) {

      if (node.key === key) {
        return true;
      }

      if (
        node.children &&
        this.treeContainsKey(
          node.children,
          key
        )
      ) {
        return true;
      }
    }

    return false;
  }

  private buildTreeFromS3Keys(
    keys: string[],
    clientId: string
  ): S3TreeNode[] {

    const runs =
      new Map<string, S3TreeNode>();

    const prefix = `${clientId}/`;

    for (const rawKey of keys) {

      const key =
        rawKey.replace(
          /^generated-files\//,
          ''
        );

      if (!key.startsWith(prefix)) {
        continue;
      }

      const relativeKey =
        key.slice(prefix.length);

      const parts = relativeKey
        .split('/')
        .filter(Boolean);

      if (parts.length < 2) {
        continue;
      }

      const runId = parts[0];

      const fileName =
        parts[parts.length - 1];

      if (
        !fileName.toLowerCase().endsWith('.bin')
      ) {
        continue;
      }

      let runNode = runs.get(runId);

      if (!runNode) {

        runNode = {
          name: runId,
          key: `${clientId}/${runId}`,
          children: []
        };

        runs.set(runId, runNode);
      }

      runNode.children?.push({
        name: fileName,
        key: `${clientId}/${relativeKey}`
      });
    }

    const runNodes = [...runs.values()]
      .sort((a, b) =>
        b.name.localeCompare(a.name)
      );

    for (const runNode of runNodes) {

      runNode.children =
        [...(runNode.children ?? [])]
          .sort((a, b) =>
            a.name.localeCompare(b.name)
          );
    }

    return runNodes;
  }

  private async loadRuntimeConfig():
    Promise<RuntimeConfig> {

    const response = await fetch(
      `assets/config.json?t=${Date.now()}`
    );

    if (!response.ok) {
      throw new Error(
        `Failed to load assets/config.json. HTTP ${response.status}`
      );
    }

    return await response.json();
  }

  private resolveClientId(
    config: RuntimeConfig
  ): string {

    const clientId =
      config.clientId ||
      config.customerId ||
      localStorage.getItem('clientId') ||
      localStorage.getItem('customerId') ||
      '00000000';

    if (
      !/^[a-zA-Z0-9]{8}$/.test(clientId)
    ) {
      throw new Error(
        `Invalid clientId: ${clientId}`
      );
    }

    return clientId;
  }

  private shouldUseLocalMock(): boolean {

    const hostname =
      window.location.hostname;

    return (
      environment.disableAuth === true &&
      (
        hostname === 'localhost' ||
        hostname === '127.0.0.1'
      )
    );
  }

  private buildLocalMockTree():
    S3TreeNode[] {

    return [
      {
        name: '20260508183000',
        key: '00000000/20260508183000',

        children: [
          {
            name: 'VINKDT000001KADUT.bin',
            key: '00000000/20260508183000/VIN000001KADUT.bin'
          }
        ]
      }
    ];
  }

  private async getS3Client():
    Promise<S3Client> {

    const config =
      await this.loadRuntimeConfig();

    const region =
      config.s3Region?.trim() ||
      'us-east-1';

    const session =
      await fetchAuthSession();

    if (!session.credentials) {
      throw new Error(
        'Cognito credentials unavailable.'
      );
    }

    return new S3Client({
      region,
      credentials: session.credentials
    });
  }

  private async listS3KeysFromBucket(
    bucket: string,
    prefix: string
  ): Promise<string[]> {

    const s3Client =
      await this.getS3Client();

    const keys: string[] = [];

    let continuationToken:
      string | undefined;

    do {

      const response =
        await s3Client.send(
          new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix,
            ContinuationToken:
              continuationToken
          })
        );

      for (
        const item of response.Contents ?? []
      ) {

        if (item.Key) {
          keys.push(item.Key);
        }
      }

      continuationToken =
        response.NextContinuationToken;

    } while (continuationToken);

    return keys;
  }

  private findFirstBinNode(nodes: S3TreeNode[]): S3TreeNode | null {
    for (const node of nodes) {
      if (this.isBinFile(node)) {
        return node;
      }

      const childMatch = this.findFirstBinNode(node.children ?? []);

      if (childMatch) {
        return childMatch;
      }
    }

    return null;
  }
}