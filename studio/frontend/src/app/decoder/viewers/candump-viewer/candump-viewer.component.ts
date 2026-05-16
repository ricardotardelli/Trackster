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

import { parseTracksterBin } from '../../parser/decoder.bin.parser';

import { S3TreeNode } from '../../decoder.component';

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  customerId?: string;
  clientId?: string;
}

interface CandumpRow {
  timestamp: string;
  canId: string;
  dlc: string;
  data: string;
  line: string;
}

type CandumpColumnKey =
  | 'timestamp'
  | 'canId'
  | 'dlc'
  | 'data'
  | 'line';

interface CandumpColumn {
  key: CandumpColumnKey;
  label: string;
}

@Component({
  selector: 'app-candump-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule
  ],
  templateUrl: './candump-viewer.component.html',
  styleUrl: './candump-viewer.component.css'
})
export class CandumpViewerComponent
implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoadingCandump = false;

  candumpErrorMessage = '';

  candumpSearchText = '';

  readonly maxRenderedBlocks = 50;

  readonly defaultInterfaceName = 'can0';

  readonly columns: CandumpColumn[] = [
    {
      key: 'timestamp',
      label: 'Timestamp'
    },
    {
      key: 'canId',
      label: 'CAN ID'
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
      key: 'line',
      label: 'CANdump Line'
    }
  ];

  rows: CandumpRow[] = [];

  filteredRows: CandumpRow[] = [];

  sortColumn: CandumpColumnKey = 'timestamp';

  sortDirection: 'asc' | 'desc' = 'asc';

  candumpViewer = {
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
      await this.loadBinAsCandump(
        this.selectedNode
      );
    }
  }

  applyCandumpSearch(): void {

    const searchText =
      this.candumpSearchText
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

  clearCandumpSearch(): void {

    this.candumpSearchText = '';

    this.filteredRows = [
      ...this.rows
    ];

    this.applySort();
  }

  sortBy(
    column: CandumpColumnKey
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
    column: CandumpColumnKey
  ): string {

    if (this.sortColumn !== column) {
      return '';
    }

    return this.sortDirection === 'asc'
      ? '↑'
      : '↓';
  }

  async copyCandumpToClipboard():
    Promise<void> {

    await navigator.clipboard.writeText(
      this.buildCandumpText(
        this.filteredRows
      )
    );
  }

  private async loadBinAsCandump(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingCandump = true;

    this.candumpErrorMessage = '';

    this.candumpSearchText = '';

    this.rows = [];

    this.filteredRows = [];

    this.candumpViewer = {
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
        this.buildCandumpRows(
          parsed
        );

      this.filteredRows = [
        ...this.rows
      ];

      this.applySort();

      const candumpText =
        this.buildCandumpText(
          this.rows
        );

      this.candumpViewer = {
        summary: [
          {
            label: 'Lines',
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
            label: 'CANdump size',
            value:
              this.formatBytes(
                new Blob([
                  candumpText
                ]).size
              )
          }
        ]
      };

    } catch (error) {

      console.error(
        'Failed to decode BIN as CANdump',
        error
      );

      this.candumpErrorMessage =
        error instanceof Error
          ? error.message
          : 'Failed to decode BIN as CANdump.';

    } finally {

      this.isLoadingCandump = false;
    }
  }

  private buildCandumpRows(
    parsed: any
  ): CandumpRow[] {

    const rows: CandumpRow[] = [];

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

        const timestamp =
          this.calculateFrameTimestampSeconds(
            firstTimestampNs,
            blockTimestampNs,
            frame.timestampDeltaNs
          ).toFixed(6);

        const canId =
          this.normalizeCanId(
            frame.canIdHex
          );

        const data =
          this.normalizePayloadHex(
            frame.payloadBytes
          );

        const dlc =
          String(
            frame.payloadLength ??
            this.calculateDlcFromPayloadHex(data)
          );

        const line =
          this.buildCandumpLine(
            timestamp,
            this.defaultInterfaceName,
            canId,
            data
          );

        rows.push({
          timestamp,
          canId,
          dlc,
          data,
          line
        });
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

  private buildCandumpText(
    rows: CandumpRow[]
  ): string {

    return rows
      .map((row) => row.line)
      .join('\n');
  }

  private buildCandumpLine(
    timestamp: string,
    interfaceName: string,
    canId: string,
    data: string
  ): string {

    return `(${timestamp}) ${interfaceName} ${canId}#${data}`;
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

  private normalizeCanId(
    canId: string
  ): string {

    if (!canId) {
      return '';
    }

    return canId
      .replace(/^0x/i, '')
      .replace(/\s+/g, '')
      .toUpperCase();
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

  private calculateDlcFromPayloadHex(
    payloadHex: string
  ): number {

    if (!payloadHex) {
      return 0;
    }

    return Math.floor(
      payloadHex.length / 2
    );
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
        'Run manifest not available for CANdump viewer',
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