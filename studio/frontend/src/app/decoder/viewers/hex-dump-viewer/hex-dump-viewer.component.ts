import {
  Component,
  Input,
  OnChanges,
  SimpleChanges
} from '@angular/core';

import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';

import { environment } from '../../../../environments/environment';

import {
  GetObjectCommand,
  S3Client
} from '@aws-sdk/client-s3';

import { fetchAuthSession } from 'aws-amplify/auth';

import { S3TreeNode } from '../../decoder.component';

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  customerId?: string;
  clientId?: string;
}

interface HexDumpRow {
  offset: string;
  hex: string;
  ascii: string;
}

@Component({
  selector: 'app-hex-dump-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule
  ],
  templateUrl: './hex-dump-viewer.component.html',
  styleUrl: './hex-dump-viewer.component.css'
})
export class HexDumpViewerComponent
implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoadingHexDump = false;

  hexDumpErrorMessage = '';

  hexSearchText = '';

  offsetInputText = '';

  readonly bytesPerRow = 16;

  readonly rowsPerPage = 2048;

  private fileBytes =
    new Uint8Array();

  rows: HexDumpRow[] = [];

  filteredRows: HexDumpRow[] = [];

  currentPageIndex = 0;

  totalPages = 0;

  currentPageStartOffsetLabel = '0x00000000';

  currentPageEndOffsetLabel = '0x00000000';

  hexDumpViewer = {
    summary: [] as Array<{
      label: string;
      value: string;
    }>
  };

  async ngOnChanges(
    changes: SimpleChanges
  ): Promise<void> {

    if (
      changes['selectedNode'] &&
      this.selectedNode
    ) {
      await this.loadBinAsHexDump(
        this.selectedNode
      );
    }
  }

  nextPage(): void {

    if (
      this.currentPageIndex >=
      this.totalPages - 1
    ) {
      return;
    }

    this.currentPageIndex += 1;

    this.rebuildCurrentPage();
  }

  previousPage(): void {

    if (this.currentPageIndex <= 0) {
      return;
    }

    this.currentPageIndex -= 1;

    this.rebuildCurrentPage();
  }

  goToOffset(): void {

    if (!this.fileBytes.length) {
      return;
    }

    const offset =
      this.parseOffsetInput(
        this.offsetInputText
      );

    if (offset === null) {

      this.hexDumpErrorMessage =
        'Invalid offset. Use decimal or HEX format, for example 1024 or 0x400.';

      return;
    }

    this.hexDumpErrorMessage = '';

    const safeOffset =
      Math.min(
        Math.max(offset, 0),
        this.fileBytes.length - 1
      );

    const rowIndex =
      Math.floor(
        safeOffset / this.bytesPerRow
      );

    this.currentPageIndex =
      Math.floor(
        rowIndex / this.rowsPerPage
      );

    this.rebuildCurrentPage();
  }

  applyHexSearch(): void {

    const searchText =
      this.hexSearchText
        .trim()
        .toLowerCase();

    if (!searchText) {

      this.filteredRows = [
        ...this.rows
      ];

      this.updateSummary();

      return;
    }

    this.filteredRows =
      this.rows.filter((row) =>
        Object.values(row)
          .some((value) =>
            String(value)
              .toLowerCase()
              .includes(searchText)
          )
      );

    this.updateSummary();
  }

  clearHexSearch(): void {

    this.hexSearchText = '';

    this.filteredRows = [
      ...this.rows
    ];

    this.updateSummary();
  }

  async copyHexDumpPageToClipboard():
    Promise<void> {

    await navigator.clipboard.writeText(
      this.buildHexDumpText(
        this.filteredRows
      )
    );
  }

  private async loadBinAsHexDump(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingHexDump = true;

    this.hexDumpErrorMessage = '';

    this.hexSearchText = '';

    this.offsetInputText = '';

    this.fileBytes =
      new Uint8Array();

    this.rows = [];

    this.filteredRows = [];

    this.currentPageIndex = 0;

    this.totalPages = 0;

    this.hexDumpViewer = {
      summary: []
    };

    try {

      const buffer =
        await this.loadTracksterBinBuffer(
          node
        );

      this.fileBytes =
        new Uint8Array(buffer);

      this.totalPages =
        Math.max(
          1,
          Math.ceil(
            this.getTotalRows() /
            this.rowsPerPage
          )
        );

      this.rebuildCurrentPage();

    } catch (error) {

      console.error(
        'Failed to load BIN as HEX dump',
        error
      );

      this.hexDumpErrorMessage =
        error instanceof Error
          ? error.message
          : 'Failed to load BIN as HEX dump.';

    } finally {

      this.isLoadingHexDump = false;
    }
  }

  private rebuildCurrentPage(): void {

    this.rows =
      this.buildCurrentPageRows();

    this.filteredRows = [
      ...this.rows
    ];

    this.hexSearchText = '';

    this.updateCurrentPageOffsetLabels();

    this.updateSummary();
  }

  private buildCurrentPageRows():
    HexDumpRow[] {

    const rows: HexDumpRow[] = [];

    const startOffset =
      this.currentPageIndex *
      this.rowsPerPage *
      this.bytesPerRow;

    const endOffset =
      Math.min(
        startOffset +
        this.rowsPerPage *
        this.bytesPerRow,
        this.fileBytes.length
      );

    for (
      let offset = startOffset;
      offset < endOffset;
      offset += this.bytesPerRow
    ) {

      const chunk =
        this.fileBytes.slice(
          offset,
          Math.min(
            offset + this.bytesPerRow,
            this.fileBytes.length
          )
        );

      rows.push({
        offset:
          this.formatOffset(
            offset
          ),
        hex:
          this.formatHexBytes(
            chunk
          ),
        ascii:
          this.formatAscii(
            chunk
          )
      });
    }

    return rows;
  }

  private updateCurrentPageOffsetLabels():
    void {

    if (!this.fileBytes.length) {

      this.currentPageStartOffsetLabel =
        '0x00000000';

      this.currentPageEndOffsetLabel =
        '0x00000000';

      return;
    }

    const startOffset =
      this.currentPageIndex *
      this.rowsPerPage *
      this.bytesPerRow;

    const endOffset =
      Math.min(
        startOffset +
        this.rowsPerPage *
        this.bytesPerRow -
        1,
        this.fileBytes.length - 1
      );

    this.currentPageStartOffsetLabel =
      this.formatOffset(startOffset);

    this.currentPageEndOffsetLabel =
      this.formatOffset(endOffset);
  }

  private updateSummary(): void {

    const pageBytes =
      this.rows.length *
      this.bytesPerRow;

    this.hexDumpViewer = {
      summary: [
        {
          label: 'File size',
          value:
            this.formatBytes(
              this.fileBytes.length
            )
        },
        {
          label: 'Total rows',
          value:
            this.getTotalRows()
              .toLocaleString()
        },
        {
          label: 'Page',
          value:
            `${this.currentPageIndex + 1}/${this.totalPages}`
        },
        {
          label: 'Page rows',
          value:
            this.rows.length
              .toLocaleString()
        },
        {
          label: 'Visible',
          value:
            this.filteredRows.length
              .toLocaleString()
        },
        {
          label: 'Page size',
          value:
            this.formatBytes(
              Math.min(
                pageBytes,
                this.fileBytes.length
              )
            )
        }
      ]
    };
  }

  private getTotalRows(): number {

    if (!this.fileBytes.length) {
      return 0;
    }

    return Math.ceil(
      this.fileBytes.length /
      this.bytesPerRow
    );
  }

  private parseOffsetInput(
    value: string
  ): number | null {

    const normalized =
      value
        .trim()
        .toLowerCase();

    if (!normalized) {
      return null;
    }

    if (
      normalized.startsWith('0x')
    ) {

      const parsedHex =
        Number.parseInt(
          normalized.slice(2),
          16
        );

      return Number.isNaN(parsedHex)
        ? null
        : parsedHex;
    }

    const parsedDecimal =
      Number.parseInt(
        normalized,
        10
      );

    return Number.isNaN(parsedDecimal)
      ? null
      : parsedDecimal;
  }

  private formatOffset(
    offset: number
  ): string {

    return `0x${offset
      .toString(16)
      .toUpperCase()
      .padStart(8, '0')}`;
  }

  private formatHexBytes(
    bytes: Uint8Array
  ): string {

    return Array.from(bytes)
      .map((byte) =>
        byte
          .toString(16)
          .toUpperCase()
          .padStart(2, '0')
      )
      .join(' ');
  }

  private formatAscii(
    bytes: Uint8Array
  ): string {

    return Array.from(bytes)
      .map((byte) => {

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

  private buildHexDumpText(
    rows: HexDumpRow[]
  ): string {

    return rows
      .map((row) =>
        `${row.offset}  ${row.hex.padEnd(47, ' ')}  ${row.ascii}`
      )
      .join('\n');
  }

  private async loadTracksterBinBuffer(
    node: S3TreeNode
  ): Promise<ArrayBuffer> {

    if (this.shouldUseLocalMock()) {

      const response =
        await fetch(
          'assets/mock/sample.bin'
        );

      if (!response.ok) {

        throw new Error(
          `Failed to load local mock BIN. HTTP ${response.status}`
        );
      }

      return await response.arrayBuffer();
    }

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Default?.trim();

    if (!bucket) {

      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    return await this.getS3ObjectBuffer(
      bucket,
      node.key
    );
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
      credentials:
        session.credentials
    });
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

    if (!response.Body) {

      throw new Error(
        `S3 body empty. ${key}`
      );
    }

    return await this.s3BodyToArrayBuffer(
      response.Body
    );
  }

  private async s3BodyToArrayBuffer(
    body: any
  ): Promise<ArrayBuffer> {

    if (
      typeof body.transformToByteArray ===
      'function'
    ) {

      const bytes =
        await body.transformToByteArray();

      const output =
        new Uint8Array(
          bytes.byteLength
        );

      output.set(bytes);

      return output.buffer;
    }

    if (
      typeof body.arrayBuffer ===
      'function'
    ) {

      return await body.arrayBuffer();
    }

    throw new Error(
      'Unsupported S3 body.'
    );
  }

  private async loadRuntimeConfig():
    Promise<RuntimeConfig> {

    const response =
      await fetch(
        `assets/config.json?t=${Date.now()}`
      );

    if (!response.ok) {

      throw new Error(
        `Failed to load assets/config.json. HTTP ${response.status}`
      );
    }

    return await response.json();
  }

  private shouldUseLocalMock():
    boolean {

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

  private formatBytes(
    bytes: number
  ): string {

    if (bytes >= 1024 * 1024) {

      return `${(
        bytes /
        (1024 * 1024)
      ).toFixed(2)} MB`;
    }

    if (bytes >= 1024) {

      return `${(
        bytes / 1024
      ).toFixed(2)} KB`;
    }

    return `${bytes} B`;
  }
}