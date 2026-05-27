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

import { S3TreeNode } from '../../decoder.component';

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  s3ParquetBucket?: string;
  customerId?: string;
  clientId?: string;
  decoderApi?: {
    parquetExportUrl?: string;
  };
}

interface ParquetExportManifest {
  manifestVersion: string;
  format: string;
  inputKey: string;
  outputKey: string;
  manifestKey: string;
  inputFileSize: number;
  outputFileSize: number;
  summary: {
    frameCount: number;
    previewCount: number;
    previewLimit: number;
    uniqueCanIdCount: number;
    channelCount: number;
    durationSeconds: number;
  };
  columns: string[];
  rowsPreview: ParquetManifestRow[];
}

interface ParquetManifestRow {
  timestamp: string;
  channel: string;
  canId: string;
  direction: string;
  frameType: string;
  dlc: string;
  payload: string;
  signal?: string;
  value?: string;
}

interface ParquetRow {
  timestamp: string;
  channel: string;
  canId: string;
  direction: string;
  frameType: string;
  dlc: string;
  payload: string;

  name: string;
  data: string;
  signal: string;
  value: string;
}

type ParquetColumnKey =
  | 'timestamp'
  | 'channel'
  | 'canId'
  | 'direction'
  | 'frameType'
  | 'dlc'
  | 'payload'
  | 'name'
  | 'data'
  | 'signal'
  | 'value';

interface ParquetColumn {
  key: ParquetColumnKey;
  label: string;
}

@Component({
  selector: 'app-parquet-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule
  ],
  templateUrl: './parquet-viewer.component.html',
  styleUrl: './parquet-viewer.component.css'
})
export class ParquetViewerComponent
implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoadingParquet = false;

  parquetErrorMessage = '';

  parquetSearchText = '';

  readonly columns: ParquetColumn[] = [
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

  rows: ParquetRow[] = [];

  filteredRows: ParquetRow[] = [];

  sortColumn: ParquetColumnKey = 'timestamp';

  sortDirection: 'asc' | 'desc' = 'asc';

  private manifest: ParquetExportManifest | null = null;

  parquetViewer = {
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
      await this.loadParquetPreview(
        this.selectedNode
      );
    }
  }

  applyParquetSearch(): void {

    const searchText =
      this.parquetSearchText
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

  clearParquetSearch(): void {

    this.parquetSearchText = '';

    this.filteredRows = [
      ...this.rows
    ];

    this.applySort();
  }

  sortBy(
    column: ParquetColumnKey
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
    column: ParquetColumnKey
  ): string {

    if (this.sortColumn !== column) {
      return '';
    }

    return this.sortDirection === 'asc'
      ? '↑'
      : '↓';
  }

  private async loadParquetPreview(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingParquet = true;

    this.parquetErrorMessage = '';

    this.parquetSearchText = '';

    this.rows = [];

    this.filteredRows = [];

    this.manifest = null;

    this.parquetViewer = {
      summary: []
    };

    try {

      if (this.shouldUseLocalMock()) {

        await this.loadLocalMockParquetManifest();

      } else {

        await this.exportParquetWithLambda(
          node
        );

        await this.loadParquetManifestFromS3(
          node
        );
      }

      this.rows =
        this.buildRowsFromManifest();

      this.filteredRows = [
        ...this.rows
      ];

      this.applySort();

      this.updateSummary();

    } catch (error) {

      console.error(
        'Failed to load Parquet preview',
        error
      );

      this.parquetErrorMessage =
        error instanceof Error
          ? error.message
          : 'Failed to load Parquet preview.';

    } finally {

      this.isLoadingParquet = false;
    }
  }

  private async exportParquetWithLambda(
    node: S3TreeNode
  ): Promise<void> {

    const config =
      await this.loadRuntimeConfig();

    const exportUrl =
      config.decoderApi?.parquetExportUrl?.trim();

    if (!exportUrl) {

      throw new Error(
        'Missing decoderApi.parquetExportUrl in assets/config.json'
      );
    }

    const inputBucketName =
      config.s3Default?.trim() ||
      's3-trackster-can-bucket';

    const outputBucketName =
      config.s3ParquetBucket?.trim();

    if (!outputBucketName) {

      throw new Error(
        'Missing s3ParquetBucket in assets/config.json'
      );
    }

    const clientId =
      this.resolveClientId(
        config
      );

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
              node.key
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
        `Parquet export failed. HTTP ${response.status}`
      );
    }
  }

  private async loadParquetManifestFromS3(
    node: S3TreeNode
  ): Promise<void> {

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3ParquetBucket?.trim();

    if (!bucket) {

      throw new Error(
        'Missing s3ParquetBucket in assets/config.json'
      );
    }

    const manifestKey =
      this.buildParquetManifestKey(
        node.key
      );

    const buffer =
      await this.getS3ObjectBuffer(
        bucket,
        manifestKey
      );

    const manifestText =
      new TextDecoder('utf-8')
        .decode(buffer);

    this.manifest =
      JSON.parse(
        manifestText
      );
  }

  private async loadLocalMockParquetManifest():
    Promise<void> {

    const response =
      await fetch(
        'assets/mock/parquet-export-manifest.json'
      );

    if (!response.ok) {

      throw new Error(
        `Failed to load local Parquet mock manifest. HTTP ${response.status}`
      );
    }

    this.manifest =
      await response.json();
  }

  private buildRowsFromManifest():
    ParquetRow[] {

    const rowsPreview =
      this.manifest?.rowsPreview ?? [];

    return rowsPreview.map((row) => {

      const canId =
        String(row.canId ?? '');

      const payload =
        String(row.payload ?? '');

      return {
        timestamp:
          String(row.timestamp ?? ''),

        channel:
          String(row.channel ?? ''),

        canId,

        direction:
          String(row.direction ?? ''),

        frameType:
          String(row.frameType ?? ''),

        dlc:
          String(row.dlc ?? ''),

        payload,

        name:
          canId
            ? `CAN_${canId}`
            : '',

        data:
          payload,

        signal:
          String(
            (row as any).signal ?? ''
          ),

        value:
          String(
            (row as any).value ?? ''
          )
      };
    });
  }

  private updateSummary(): void {

    const summary =
      this.manifest?.summary;

    this.parquetViewer = {
      summary: [
        {
          label: 'Rows',
          value:
            Number(summary?.previewCount ?? this.rows.length)
              .toLocaleString()
        },
        {
          label: 'Frames',
          value:
            Number(summary?.frameCount ?? 0)
              .toLocaleString()
        },
        {
          label: 'CAN IDs',
          value:
            Number(summary?.uniqueCanIdCount ?? 0)
              .toLocaleString()
        },
        {
          label: 'Channels',
          value:
            Number(summary?.channelCount ?? 0)
              .toLocaleString()
        },
        {
          label: 'Preview limit',
          value:
            Number(summary?.previewLimit ?? this.rows.length)
              .toLocaleString()
        },
        {
          label: 'Parquet size',
          value:
            this.formatBytes(
              Number(this.manifest?.outputFileSize ?? 0)
            )
        }
      ]
    };
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

  private buildParquetManifestKey(
    inputKey: string
  ): string {

    return inputKey
      .replace(/\.[^.]+$/, '.parquet.json');
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