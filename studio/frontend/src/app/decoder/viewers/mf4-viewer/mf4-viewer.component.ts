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
  s3Mf4Bucket?: string;
  s3Region?: string;
  clientId?: string;
  customerId?: string;

  decoderApi?: {
    mf4ExportUrl?: string;
  };
}

interface Mf4SummaryCard {
  label: string;
  value: string;
}

interface Mf4Header {
  mf4Version: string;
  manifestVersion: string;
  inputFileSize: string;
  outputFileSize: string;
}

interface Mf4Summary {
  totalFrames: string;
  canMessages: string;
  canFdMessages: string;
  channelCount: string;
  busCount: string;
  uniqueCanIds: string;
  duration: string;
}

interface Mf4HeaderField {
  label: string;
  value: string;
}

interface Mf4ViewerState {
  fileName: string;
  summaryCards: Mf4SummaryCard[];
  header: Mf4Header;
  summary: Mf4Summary;
  headerFields: Mf4HeaderField[];
  channels: any[];
  messages: any[];
}

@Component({
  selector: 'app-mf4-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule
  ],
  templateUrl: './mf4-viewer.component.html',
  styleUrl: './mf4-viewer.component.css'
})
export class Mf4ViewerComponent implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoading = false;

  loadError = '';

  filterText = '';

  mf4Viewer: Mf4ViewerState =
    this.createEmptyViewer();

  async ngOnChanges(
    changes: SimpleChanges
  ): Promise<void> {

    if (
      changes['selectedNode'] &&
      this.selectedNode
    ) {
      await this.loadMf4ForViewer();
    }
  }

  matchesFilter(
    value: string
  ): boolean {

    const filter =
      this.filterText
        .trim()
        .toLowerCase();

    if (!filter) {
      return true;
    }

    return value
      .toLowerCase()
      .includes(filter);
  }

  getTotalMessageCount(): number {
    return this.mf4Viewer.messages.length;
  }

  getMessagePageStart(): number {
    return this.mf4Viewer.messages.length
      ? 1
      : 0;
  }

  getMessagePageEnd(): number {
    return this.mf4Viewer.messages.length;
  }

  async copyVisibleMessagesToClipboard():
    Promise<void> {

    const rows = [
      [
        'Time',
        'Type',
        'Bus',
        'CAN ID',
        'DLC',
        'Payload',
        'Flags'
      ],

      ...this.mf4Viewer.messages.map(
        message => [
          message.time,
          message.type,
          message.bus,
          message.canId,
          message.dlc,
          message.payload,
          message.flags
        ]
      )
    ];

    await this.copyRowsToClipboard(
      rows
    );
  }

  async copyHeaderToClipboard():
    Promise<void> {

    const rows = [
      ['Field', 'Value'],

      ...this.mf4Viewer.headerFields.map(
        field => [
          field.label,
          field.value
        ]
      )
    ];

    await this.copyRowsToClipboard(
      rows
    );
  }

  private async loadMf4ForViewer():
    Promise<void> {

    this.isLoading = true;

    this.loadError = '';

    this.resetViewer();

    try {

      let manifest: any | null = null;

      try {

        manifest =
          await this.loadMf4Manifest();

      } catch (error) {

        if (
          this.shouldUseLocalMock()
        ) {
          throw error;
        }

        await this.generateMf4File(
          this.selectedNode
        );

        manifest =
          await this.loadMf4Manifest();
      }

      this.populateViewerFromManifest(
        manifest
      );

    } catch (error: any) {

      console.error(
        'Failed to load MF4 viewer',
        error
      );

      this.loadError =
        error?.message ||
        'Failed to load MF4 manifest.';

      this.resetViewer();

    } finally {

      this.isLoading = false;
    }
  }

  private async generateMf4File(
    node: S3TreeNode
  ): Promise<void> {

    const config =
      await this.loadRuntimeConfig();

    const apiUrl =
      config.decoderApi?.mf4ExportUrl?.trim();

    if (!apiUrl) {
      throw new Error(
        'Missing decoderApi.mf4ExportUrl in assets/config.json'
      );
    }

    const inputBucketName =
      config.s3Default?.trim();

    const outputBucketName =
      config.s3Mf4Bucket?.trim();

    if (!inputBucketName) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    if (!outputBucketName) {
      throw new Error(
        'Missing s3Mf4Bucket in assets/config.json'
      );
    }

    const clientId =
      this.resolveClientId(config);

    const session =
      await fetchAuthSession();

    const token =
      session.tokens?.idToken?.toString() ||
      session.tokens?.accessToken?.toString();

    if (!token) {
      throw new Error(
        'Cognito token unavailable.'
      );
    }

    const response =
      await fetch(
        apiUrl,
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

    let payload: any = null;

    if (responseText) {
      try {
        payload = JSON.parse(responseText);
      } catch {
        payload = responseText;
      }
    }

    if (!response.ok) {
      throw new Error(
        payload?.error ||
        payload?.message ||
        `MF4 generation failed. HTTP ${response.status}`
      );
    }
  }

  private async loadMf4Manifest():
    Promise<any> {

    if (
      this.shouldUseLocalMock()
    ) {

      const response =
        await fetch(
          '/assets/mock/sample.mf4.json'
        );

      if (!response.ok) {
        throw new Error(
          `Local MF4 manifest mock not found. HTTP ${response.status}`
        );
      }

      return await response.json();
    }

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Mf4Bucket?.trim();

    if (!bucket) {
      throw new Error(
        'Missing s3Mf4Bucket in assets/config.json'
      );
    }

    const buffer =
      await this.getS3ObjectBuffer(
        bucket,
        this.buildMf4ManifestKey(
          this.selectedNode.key
        )
      );

    const text =
      new TextDecoder()
        .decode(buffer);

    return JSON.parse(text);
  }

  private populateViewerFromManifest(
    manifest: any
  ): void {

    const summary =
      manifest.summary || {};

    const channels =
      Array.isArray(manifest.channels)
        ? manifest.channels
        : [];

    const messages =
      Array.isArray(manifest.messagesPreview)
        ? manifest.messagesPreview
        : [];

    const frameCount =
      this.formatNumber(
        summary.frameCount
      );

    const canMessageCount =
      this.formatNumber(
        summary.canMessageCount
      );

    const canFdMessageCount =
      this.formatNumber(
        summary.canFdMessageCount
      );

    const channelCount =
      this.formatNumber(
        channels.length
      );

    const busCount =
      this.formatNumber(
        summary.busCount
      );

    const uniqueCanIdCount =
      this.formatNumber(
        summary.uniqueCanIdCount
      );

    const duration =
      this.formatDuration(
        summary.durationSeconds
      );

    const outputFileSize =
      this.formatBytes(
        manifest.outputFileSize || 0
      );

    const inputFileSize =
      this.formatBytes(
        manifest.inputFileSize || 0
      );

    this.mf4Viewer = {
      fileName:
        this.buildMf4FileName(
          this.selectedNode.name
        ),

      summaryCards: [
        {
          label: 'Frames',
          value: frameCount
        },
        {
          label: 'CAN',
          value: canMessageCount
        },
        {
          label: 'CAN FD',
          value: canFdMessageCount
        },
        {
          label: 'Channels',
          value: channelCount
        },
        {
          label: 'Size',
          value: outputFileSize
        }
      ],

      header: {
        mf4Version:
          String(
            manifest.mf4Version || '-'
          ),
        manifestVersion:
          String(
            manifest.manifestVersion || '-'
          ),
        inputFileSize,
        outputFileSize
      },

      summary: {
        totalFrames: frameCount,
        canMessages: canMessageCount,
        canFdMessages: canFdMessageCount,
        channelCount,
        busCount,
        uniqueCanIds: uniqueCanIdCount,
        duration
      },

      headerFields: [
        {
          label: 'MF4 version',
          value:
            String(
              manifest.mf4Version || '-'
            )
        },
        {
          label: 'Manifest version',
          value:
            String(
              manifest.manifestVersion || '-'
            )
        },
        {
          label: 'Input file',
          value:
            String(
              manifest.inputKey || '-'
            )
        },
        {
          label: 'Output file',
          value:
            String(
              manifest.outputKey || '-'
            )
        },
        {
          label: 'Manifest file',
          value:
            String(
              manifest.manifestKey || '-'
            )
        },
        {
          label: 'Input size',
          value: inputFileSize
        },
        {
          label: 'Output size',
          value: outputFileSize
        },
        {
          label: 'Frames',
          value: frameCount
        },
        {
          label: 'Buses',
          value: busCount
        },
        {
          label: 'Unique CAN IDs',
          value: uniqueCanIdCount
        },
        {
          label: 'Duration',
          value: duration
        },
        {
          label: 'Preview messages',
          value:
            `${this.formatNumber(summary.previewCount)} / ${this.formatNumber(summary.previewLimit)}`
        }
      ],

      channels,

      messages:
        messages.map(
          (message: any) => ({
            ...message,
            searchText: [
              message.time,
              message.type,
              message.bus,
              message.canId,
              message.dlc,
              message.payload,
              message.flags
            ].join(' ')
          })
        )
    };
  }

  private buildMf4ManifestKey(
    key: string
  ): string {

    if (
      !key
        .toLowerCase()
        .endsWith('.bin')
    ) {
      return `${key}.json`;
    }

    return `${key.slice(0, -4)}.mf4.json`;
  }

  private buildMf4FileName(
    fileName: string
  ): string {

    if (
      !fileName
        .toLowerCase()
        .endsWith('.bin')
    ) {
      return fileName;
    }

    return `${fileName.slice(0, -4)}.mf4`;
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
        `Failed to load config.json. HTTP ${response.status}`
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

  private formatBytes(
    bytes: number
  ): string {

    if (
      !Number.isFinite(bytes) ||
      bytes <= 0
    ) {
      return '-';
    }

    if (
      bytes >=
      1024 * 1024
    ) {
      return `${(
        bytes /
        (
          1024 * 1024
        )
      ).toFixed(2)} MB`;
    }

    if (
      bytes >= 1024
    ) {
      return `${(
        bytes / 1024
      ).toFixed(2)} KB`;
    }

    return `${bytes} B`;
  }

  private formatDuration(
    value: number
  ): string {

    if (
      !Number.isFinite(value)
    ) {
      return '-';
    }

    return `${value.toFixed(3)} s`;
  }

  private formatNumber(
    value: number
  ): string {

    if (
      !Number.isFinite(value)
    ) {
      return '0';
    }

    return value
      .toLocaleString();
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

  private resetViewer():
    void {

    this.mf4Viewer =
      this.createEmptyViewer();
  }

  private createEmptyViewer():
    Mf4ViewerState {

    return {
      fileName: '',
      summaryCards: [],
      header: {
        mf4Version: '-',
        manifestVersion: '-',
        inputFileSize: '-',
        outputFileSize: '-'
      },
      summary: {
        totalFrames: '0',
        canMessages: '0',
        canFdMessages: '0',
        channelCount: '0',
        busCount: '0',
        uniqueCanIds: '0',
        duration: '-'
      },
      headerFields: [],
      channels: [],
      messages: []
    };
  }

  private async copyRowsToClipboard(
    rows: Array<Array<string | number>>
  ): Promise<void> {

    const text =
      rows
        .map(
          row => row.join('\t')
        )
        .join('\n');

    await navigator.clipboard
      .writeText(text);
  }
}