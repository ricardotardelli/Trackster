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

type VectorAscViewerMode =
  | 'table'
  | 'raw';

interface VectorAscRow {
  timestamp: string;
  channel: string;
  canId: string;
  direction: string;
  frameType: string;
  dlc: string;
  dataLength: string;
  data: string;
  name: string;
  rawLine: string;
}

@Component({
  selector: 'app-vector-asc-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule
  ],
  templateUrl: './vector-asc-viewer.component.html',
  styleUrl: './vector-asc-viewer.component.css'
})
export class VectorAscViewerComponent
implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoadingAsc = false;

  ascErrorMessage = '';

  searchText = '';

  pageInputText = '';

  viewerMode: VectorAscViewerMode = 'table';

  readonly rowsPerPage = 2048;

  allRows: VectorAscRow[] = [];

  rows: VectorAscRow[] = [];

  filteredRows: VectorAscRow[] = [];

  currentPageIndex = 0;

  totalPages = 0;

  currentPageStartRow = 0;

  currentPageEndRow = 0;

  private ascHeaderLines: string[] = [];

  ascViewer = {
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
      await this.loadBinAsVectorAsc(
        this.selectedNode
      );
    }
  }

  setViewerMode(
    mode: VectorAscViewerMode
  ): void {

    this.viewerMode = mode;
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

  goToPage(): void {

    const parsedPage =
      Number.parseInt(
        String(this.pageInputText).trim(),
        10
      );

    if (
      Number.isNaN(parsedPage) ||
      parsedPage < 1 ||
      parsedPage > this.totalPages
    ) {

      this.ascErrorMessage =
        `Invalid page. Use a value between 1 and ${this.totalPages}.`;

      return;
    }

    this.ascErrorMessage = '';

    this.currentPageIndex =
      parsedPage - 1;

    this.rebuildCurrentPage();
  }

  applySearch(): void {

    const normalizedSearch =
      this.searchText
        .trim()
        .toLowerCase();

    if (!normalizedSearch) {

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
              .includes(normalizedSearch)
          )
      );

    this.updateSummary();
  }

  clearSearch(): void {

    this.searchText = '';

    this.filteredRows = [
      ...this.rows
    ];

    this.updateSummary();
  }

  async copyCurrentPageToClipboard():
    Promise<void> {

    await navigator.clipboard.writeText(
      this.viewerMode === 'raw'
        ? this.buildRawPageText(
            this.filteredRows
          )
        : this.buildTablePageText(
            this.filteredRows
          )
    );
  }

  exportAscFile(): void {

    const fileName =
      this.buildAscFileName();

    const blob =
      new Blob(
        [
          this.buildFullAscText()
        ],
        {
          type: 'text/plain;charset=utf-8'
        }
      );

    const url =
      URL.createObjectURL(blob);

    const link =
      document.createElement('a');

    link.href = url;

    link.download = fileName;

    link.click();

    URL.revokeObjectURL(url);
  }

  buildRawPageText(
    rows: VectorAscRow[]
  ): string {

    const lines =
      rows.map((row) =>
        row.rawLine
      );

    if (this.currentPageIndex === 0) {

      return [
        ...this.ascHeaderLines,
        ...lines
      ].join('\n');
    }

    return lines.join('\n');
  }

  private async loadBinAsVectorAsc(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingAsc = true;

    this.ascErrorMessage = '';

    this.searchText = '';

    this.pageInputText = '';

    this.viewerMode = 'table';

    this.ascHeaderLines = [];

    this.allRows = [];

    this.rows = [];

    this.filteredRows = [];

    this.currentPageIndex = 0;

    this.totalPages = 0;

    this.currentPageStartRow = 0;

    this.currentPageEndRow = 0;

    this.ascViewer = {
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

      this.ascHeaderLines =
        this.buildAscHeaderLines();

      this.allRows =
        this.buildVectorAscRows(
          parsed
        );

      this.totalPages =
        Math.max(
          1,
          Math.ceil(
            this.allRows.length /
            this.rowsPerPage
          )
        );

      this.rebuildCurrentPage();

    } catch (error) {

      console.error(
        'Failed to generate Vector ASC',
        error
      );

      this.ascErrorMessage =
        error instanceof Error
          ? error.message
          : 'Failed to generate Vector ASC.';

    } finally {

      this.isLoadingAsc = false;
    }
  }

  private rebuildCurrentPage(): void {

    const startIndex =
      this.currentPageIndex *
      this.rowsPerPage;

    const endIndex =
      Math.min(
        startIndex + this.rowsPerPage,
        this.allRows.length
      );

    this.rows =
      this.allRows.slice(
        startIndex,
        endIndex
      );

    this.filteredRows = [
      ...this.rows
    ];

    this.searchText = '';

    this.pageInputText =
      String(
        this.currentPageIndex + 1
      );

    this.currentPageStartRow =
      this.allRows.length
        ? startIndex + 1
        : 0;

    this.currentPageEndRow =
      endIndex;

    this.updateSummary();
  }

  private buildVectorAscRows(
    parsed: any
  ): VectorAscRow[] {

    const rows: VectorAscRow[] = [];

    const blocks =
      Array.isArray(parsed.blocks)
        ? parsed.blocks
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

        const data =
          this.normalizePayloadHex(
            frame.payloadBytes
          );

        const payloadBytes =
          this.getPayloadByteCount(
            data,
            frame.payloadLength
          );

        const frameType =
          this.resolveFrameType(
            frame,
            payloadBytes
          );

        const dlc =
          this.resolveDlc(
            frame,
            payloadBytes,
            frameType
          );

        const channel =
          this.resolveChannel(
            frame
          );

        const direction =
          this.resolveDirection(
            frame
          );

        const canId =
          this.normalizeCanId(
            frame.canIdHex
          );

        const name =
          frame.messageName ||
          `CAN_${canId}`;

        const rawLine =
          this.buildAscFrameLine({
            timestamp,
            channel,
            canId,
            direction,
            frameType,
            dlc,
            payloadBytes,
            data
          });

        rows.push({
          timestamp,
          channel,
          canId,
          direction,
          frameType,
          dlc,
          dataLength:
            String(payloadBytes),
          data,
          name,
          rawLine
        });
      }
    }

    return rows;
  }

  private buildAscHeaderLines():
    string[] {

    return [
      `date ${this.buildAscDateLine()}`,
      'base hex  timestamps absolute',
      'internal events logged',
      'Begin Triggerblock'
    ];
  }

  private buildAscDateLine():
    string {

    const now =
      new Date();

    const weekday =
      now.toLocaleString(
        'en-US',
        {
          weekday: 'short'
        }
      );

    const month =
      now.toLocaleString(
        'en-US',
        {
          month: 'short'
        }
      );

    const day =
      String(now.getDate())
        .padStart(2, '0');

    const hours =
      String(now.getHours())
        .padStart(2, '0');

    const minutes =
      String(now.getMinutes())
        .padStart(2, '0');

    const seconds =
      String(now.getSeconds())
        .padStart(2, '0');

    const year =
      now.getFullYear();

    return `${weekday} ${month} ${day} ${hours}:${minutes}:${seconds} ${year}`;
  }

  private buildAscFrameLine(
    input: {
      timestamp: string;
      channel: string;
      canId: string;
      direction: string;
      frameType: string;
      dlc: string;
      payloadBytes: number;
      data: string;
    }
  ): string {

    const dataBytes =
      input.data
        ? ` ${input.data}`
        : '';

    if (input.frameType === 'CAN FD') {

      return [
        input.timestamp.padStart(12, ' '),
        input.channel,
        'CANFD',
        input.canId,
        input.direction,
        'd',
        input.dlc,
        input.payloadBytes,
        dataBytes.trim()
      ]
        .filter((part) =>
          part !== ''
        )
        .join(' ');
    }

    return `${input.timestamp.padStart(12, ' ')} ${input.channel} ${input.canId} ${input.direction} d ${input.dlc}${dataBytes}`;
  }

  private buildFullAscText():
    string {

    return [
      ...this.ascHeaderLines,
      ...this.allRows.map((row) =>
        row.rawLine
      ),
      'End TriggerBlock'
    ].join('\n');
  }

  private buildTablePageText(
    rows: VectorAscRow[]
  ): string {

    const header =
      [
        'Timestamp',
        'Channel',
        'CAN ID',
        'Direction',
        'Type',
        'DLC',
        'Length',
        'Data',
        'Message'
      ].join(',');

    const body =
      rows.map((row) =>
        [
          row.timestamp,
          row.channel,
          row.canId,
          row.direction,
          row.frameType,
          row.dlc,
          row.dataLength,
          row.data,
          row.name
        ]
          .map((value) =>
            this.escapeCsvValue(value)
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

  private updateSummary(): void {

    const canFdCount =
      this.allRows.filter((row) =>
        row.frameType === 'CAN FD'
      ).length;

    const classicCount =
      this.allRows.length -
      canFdCount;

    this.ascViewer = {
      summary: [
        {
          label: 'Frames',
          value:
            this.allRows.length
              .toLocaleString()
        },
        {
          label: 'Classic CAN',
          value:
            classicCount
              .toLocaleString()
        },
        {
          label: 'CAN FD',
          value:
            canFdCount
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
        }
      ]
    };
  }

  private resolveFrameType(
    frame: any,
    payloadBytes: number
  ): string {

    if (
      frame.isCanFd === true ||
      frame.canFd === true ||
      frame.frameType === 'CAN FD' ||
      frame.type === 'CAN FD' ||
      payloadBytes > 8
    ) {
      return 'CAN FD';
    }

    return 'CAN';
  }

  private resolveChannel(
    frame: any
  ): string {

    return String(
      frame.channel ??
      frame.bus ??
      frame.networkChannel ??
      1
    );
  }

  private resolveDirection(
    frame: any
  ): string {

    const direction =
      String(
        frame.direction ??
        frame.dir ??
        'Rx'
      ).toLowerCase();

    return direction === 'tx'
      ? 'Tx'
      : 'Rx';
  }

  private resolveDlc(
    frame: any,
    payloadBytes: number,
    frameType: string
  ): string {

    if (
      frame.dlc !== undefined &&
      frame.dlc !== null
    ) {
      return String(frame.dlc);
    }

    if (frameType === 'CAN FD') {
      return String(
        this.payloadLengthToCanFdDlc(
          payloadBytes
        )
      );
    }

    return String(
      Math.min(
        payloadBytes,
        8
      )
    );
  }

  private payloadLengthToCanFdDlc(
    payloadBytes: number
  ): number {

    if (payloadBytes <= 8) {
      return payloadBytes;
    }

    if (payloadBytes <= 12) {
      return 9;
    }

    if (payloadBytes <= 16) {
      return 10;
    }

    if (payloadBytes <= 20) {
      return 11;
    }

    if (payloadBytes <= 24) {
      return 12;
    }

    if (payloadBytes <= 32) {
      return 13;
    }

    if (payloadBytes <= 48) {
      return 14;
    }

    return 15;
  }

  private normalizeCanId(
    canIdHex: string
  ): string {

    const normalized =
      String(canIdHex || '')
        .replace(/^0x/i, '')
        .toUpperCase();

    if (!normalized) {
      return '0';
    }

    if (normalized.length > 3) {
      return `${normalized}x`;
    }

    return normalized;
  }

  private normalizePayloadHex(
    payload: string
  ): string {

    if (!payload) {
      return '';
    }

    return payload
      .replace(/\s+/g, '')
      .toUpperCase()
      .match(/.{1,2}/g)
      ?.join(' ') ?? '';
  }

  private getPayloadByteCount(
    data: string,
    fallbackLength: number
  ): number {

    if (data) {

      return data
        .split(' ')
        .filter(Boolean)
        .length;
    }

    return Number(
      fallbackLength ?? 0
    );
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

  private buildAscFileName():
    string {

    const originalName =
      this.selectedNode?.name ||
      'trackster';

    return originalName
      .replace(/\.[^.]+$/, '')
      .concat('.asc');
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
        'Run manifest not available for Vector ASC viewer',
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
}