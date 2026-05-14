import {
  Component,
  Input,
  OnChanges,
  SimpleChanges
} from '@angular/core';

import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { environment } from '../../../../environments/environment';

import {
  GetObjectCommand,
  S3Client
} from '@aws-sdk/client-s3';

import { fetchAuthSession } from 'aws-amplify/auth';

import { parseTracksterBin } from '../../parser/decoder.bin.parser';

import { S3TreeNode } from '../../decoder.component';

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  customerId?: string;
  clientId?: string;
}

interface CsvRow {
  timestamp: string;
  canId: string;
  name: string;
  dlc: string;
  data: string;
  signal: string;
  value: string;
}

type CsvColumnKey =
  | 'timestamp'
  | 'canId'
  | 'name'
  | 'dlc'
  | 'data'
  | 'signal'
  | 'value';

interface CsvColumn {
  key: CsvColumnKey;
  label: string;
}

@Component({
  selector: 'app-csv-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule
  ],
  templateUrl: './csv-viewer.component.html',
  styleUrl: './csv-viewer.component.css'
})
export class CsvViewerComponent
implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoadingCsv = false;

  csvErrorMessage = '';

  csvSearchText = '';

  readonly maxRenderedBlocks = 50;

  readonly columns: CsvColumn[] = [
    {
      key: 'timestamp',
      label: 'Timestamp'
    },
    {
      key: 'canId',
      label: 'CAN ID'
    },
    {
      key: 'name',
      label: 'Message'
    },
    {
      key: 'dlc',
      label: 'DLC'
    },
    {
      key: 'data',
      label: 'Data'
    },
    {
      key: 'signal',
      label: 'Signal'
    },
    {
      key: 'value',
      label: 'Value'
    }
  ];

  rows: CsvRow[] = [];

  filteredRows: CsvRow[] = [];

  sortColumn: CsvColumnKey = 'timestamp';

  sortDirection: 'asc' | 'desc' = 'asc';

  csvViewer = {
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
      await this.loadBinAsCsv(
        this.selectedNode
      );
    }
  }

  applyCsvSearch(): void {

    const searchText =
      this.csvSearchText
        .trim()
        .toLowerCase();

    if (!searchText) {

      this.filteredRows = [
        ...this.rows
      ];

      this.applySort();

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

    this.applySort();
  }

  clearCsvSearch(): void {

    this.csvSearchText = '';

    this.filteredRows = [
      ...this.rows
    ];

    this.applySort();
  }

  sortBy(
    column: CsvColumnKey
  ): void {

    if (this.sortColumn === column) {

      this.sortDirection =
        this.sortDirection === 'asc'
          ? 'desc'
          : 'asc';

    } else {

      this.sortColumn = column;

      this.sortDirection = 'asc';
    }

    this.applySort();
  }

  getSortIndicator(
    column: CsvColumnKey
  ): string {

    if (this.sortColumn !== column) {
      return '';
    }

    return this.sortDirection === 'asc'
      ? '↑'
      : '↓';
  }

  async copyCsvToClipboard():
    Promise<void> {

    await navigator.clipboard.writeText(
      this.buildCsvText(
        this.filteredRows
      )
    );
  }

  private async loadBinAsCsv(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingCsv = true;

    this.csvErrorMessage = '';

    this.csvSearchText = '';

    this.rows = [];

    this.filteredRows = [];

    this.csvViewer = {
      summary: []
    };

    try {

      const buffer =
        await this.loadTracksterBinBuffer(
          node
        );

      const manifest =
        await this.loadRunManifest(
          node
        );

      const parsed =
        parseTracksterBin(
          buffer,
          manifest
        );

      this.rows =
        this.buildDecodedCsvRows(
          parsed
        );

      this.filteredRows = [
        ...this.rows
      ];

      this.applySort();

      this.csvViewer = {
        summary: [
          {
            label: 'Rows',
            value:
              this.rows.length
                .toLocaleString()
          },
          {
            label: 'Frames',
            value:
              parsed.totalFrameCount
                .toLocaleString()
          },
          {
            label: 'Blocks',
            value:
              parsed.blockCount
                .toLocaleString()
          },
          {
            label: 'Rendered',
            value:
              Math.min(
                parsed.blocks.length,
                this.maxRenderedBlocks
              ).toLocaleString()
          },
          {
            label: 'CSV size',
            value:
              this.formatBytes(
                new Blob([
                  this.buildCsvText(this.rows)
                ]).size
              )
          }
        ]
      };

    } catch (error) {

      console.error(
        'Failed to decode BIN as CSV',
        error
      );

      this.csvErrorMessage =
        error instanceof Error
          ? error.message
          : 'Failed to decode BIN as CSV.';

    } finally {

      this.isLoadingCsv = false;
    }
  }

  private buildDecodedCsvRows(
    parsed: any
  ): CsvRow[] {

    const rows: CsvRow[] = [];

    const blocks =
      Array.isArray(parsed.blocks)
        ? parsed.blocks.slice(
            0,
            this.maxRenderedBlocks
          )
        : [];

    const firstTimestampNs =
      blocks[0]?.timestampNs ?? '0';

    for (const block of blocks) {

      const blockTimestampNs =
        block.timestampNs ?? firstTimestampNs;

      for (
        const frame of block.frames ?? []
      ) {

        const baseRow = {
          timestamp:
            this.calculateFrameTimestampSeconds(
              firstTimestampNs,
              blockTimestampNs,
              frame.timestampDeltaNs
            ).toFixed(6),

          canId:
            frame.canIdHex || '',

          name:
            frame.messageName ||
            `CAN_${frame.canIdHex}`,

          dlc:
            String(
              frame.payloadLength ?? ''
            ),

          data:
            this.normalizePayloadHex(
              frame.payloadBytes
            )
        };

        const signals =
          frame.signals ?? [];

        if (!signals.length) {

          rows.push({
            ...baseRow,
            signal: '',
            value: ''
          });

          continue;
        }

        for (const signal of signals) {

          rows.push({
            ...baseRow,
            signal:
              signal.name || '',
            value:
              String(
                signal.value ?? ''
              )
          });
        }
      }
    }

    return rows;
  }

  private applySort(): void {

    const direction =
      this.sortDirection === 'asc'
        ? 1
        : -1;

    const column =
      this.sortColumn;

    this.filteredRows =
      [...this.filteredRows].sort(
        (left, right) => {

          const leftValue =
            left[column];

          const rightValue =
            right[column];

          const leftNumber =
            Number(leftValue);

          const rightNumber =
            Number(rightValue);

          if (
            !Number.isNaN(leftNumber) &&
            !Number.isNaN(rightNumber)
          ) {
            return (
              leftNumber - rightNumber
            ) * direction;
          }

          return String(leftValue)
            .localeCompare(
              String(rightValue),
              undefined,
              {
                numeric: true,
                sensitivity: 'base'
              }
            ) * direction;
        }
      );
  }

  private buildCsvText(
    rows: CsvRow[]
  ): string {

    const header =
      this.columns
        .map((column) =>
          this.escapeCsvValue(
            column.label
          )
        )
        .join(',');

    const body =
      rows.map((row) =>
        this.columns
          .map((column) =>
            this.escapeCsvValue(
              row[column.key]
            )
          )
          .join(',')
      );

    return [
      header,
      ...body
    ].join('\n');
  }

  private escapeCsvValue(
    value: string
  ): string {

    const normalized =
      String(value ?? '');

    if (
      normalized.includes(',') ||
      normalized.includes('"') ||
      normalized.includes('\n')
    ) {

      return `"${normalized.replace(/"/g, '""')}"`;
    }

    return normalized;
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

  private async loadRunManifest(
    node: S3TreeNode
  ): Promise<any> {

    if (this.shouldUseLocalMock()) {

      const response =
        await fetch(
          'assets/mock/run-manifest.json'
        );

      if (!response.ok) {
        return null;
      }

      return await response.json();
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

    const clientId =
      this.resolveClientId(config);

    const runId =
      this.getRunIdFromKey(
        node.key
      );

    const manifestKey =
      `${clientId}/${runId}/run-manifest.json`;

    try {

      const buffer =
        await this.getS3ObjectBuffer(
          bucket,
          manifestKey
        );

      const manifestText =
        new TextDecoder('utf-8')
          .decode(buffer);

      return JSON.parse(
        manifestText
      );

    } catch (error) {

      console.warn(
        'Run manifest not available for CSV viewer',
        error
      );

      return null;
    }
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

  private resolveClientId(
    config: RuntimeConfig
  ): string {

    return (
      config.clientId ||
      config.customerId ||
      localStorage.getItem('clientId') ||
      localStorage.getItem('customerId') ||
      '00000000'
    );
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