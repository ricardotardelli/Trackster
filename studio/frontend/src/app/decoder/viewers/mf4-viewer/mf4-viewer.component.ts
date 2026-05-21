import {
  Component,
  Input,
  OnChanges,
  SimpleChanges
} from '@angular/core';

import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';

import {
  GetObjectCommand,
  S3Client
} from '@aws-sdk/client-s3';

import { fetchAuthSession } from 'aws-amplify/auth';

import { S3TreeNode } from '../../decoder.component';

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

  mf4Viewer = {
    fileName: '',
    summary: [] as any[],
    headerFields: [] as any[],
    channels: [] as any[],
    messages: [] as any[]
  };

  async ngOnChanges(
    changes: SimpleChanges
  ): Promise<void> {

    if (
      changes['selectedNode'] &&
      this.selectedNode
    ) {
      await this.loadMf4Manifest();
    }
  }

  matchesFilter(value: string): boolean {

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
    return this.mf4Viewer.messages.length ? 1 : 0;
  }

  getMessagePageEnd(): number {
    return this.mf4Viewer.messages.length;
  }

  async copyVisibleMessagesToClipboard(): Promise<void> {

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

    await this.copyRowsToClipboard(rows);
  }

  async copyHeaderToClipboard(): Promise<void> {

    const rows = [
      ['Field', 'Value'],

      ...this.mf4Viewer.headerFields.map(
        field => [
          field.label,
          field.value
        ]
      )
    ];

    await this.copyRowsToClipboard(rows);
  }

  private async loadMf4Manifest(): Promise<void> {

    this.isLoading = true;

    this.loadError = '';

    try {

      const manifest =
        await this.loadManifest();

      this.mf4Viewer = {
        fileName: this.buildMf4FileName(this.selectedNode.name),

        summary: [
          {
            label: 'Frames',
            value: manifest.summary.frameCount.toLocaleString()
          },
          {
            label: 'CAN',
            value: manifest.summary.canMessageCount.toLocaleString()
          },
          {
            label: 'CAN FD',
            value: manifest.summary.canFdMessageCount.toLocaleString()
          },
          {
            label: 'Channels',
            value: manifest.channels.length.toLocaleString()
          },
          {
            label: 'Duration',
            value: `${manifest.summary.durationSeconds.toFixed(3)} s`
          }
        ],

        headerFields: [
          {
            label: 'MF4 Version',
            value: manifest.mf4Version
          },
          {
            label: 'Frames',
            value: manifest.summary.frameCount
          },
          {
            label: 'Buses',
            value: manifest.summary.busCount
          },
          {
            label: 'Unique CAN IDs',
            value: manifest.summary.uniqueCanIdCount
          },
          {
            label: 'Preview Messages',
            value: manifest.summary.previewCount
          }
        ],

        channels: manifest.channels,

        messages:
          manifest.messagesPreview.map(
            (message: any) => ({
              ...message,
              searchText: [
                message.time,
                message.type,
                message.canId,
                message.payload,
                message.flags
              ].join(' ')
            })
          )
      };

    } catch (error: any) {

      console.error(
        'Failed to load MF4 viewer',
        error
      );

      this.loadError =
        error?.message ||
        'Failed to load MF4 manifest.';

    } finally {

      this.isLoading = false;
    }
  }

  private async loadManifest(): Promise<any> {

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Mf4Bucket?.trim();

    const buffer =
      await this.getS3ObjectBuffer(
        bucket,
        this.buildManifestKey(this.selectedNode.key)
      );

    return JSON.parse(
      new TextDecoder().decode(buffer)
    );
  }

  private buildManifestKey(key: string): string {
    return `${key.slice(0, -4)}.mf4.json`;
  }

  private buildMf4FileName(fileName: string): string {
    return `${fileName.slice(0, -4)}.mf4`;
  }

  private async getS3Client(): Promise<S3Client> {

    const config =
      await this.loadRuntimeConfig();

    const session =
      await fetchAuthSession();

    return new S3Client({
      region: config.s3Region || 'us-east-1',
      credentials: session.credentials
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

    const bytes =
      await response.Body?.transformToByteArray();

    const output =
      new Uint8Array(bytes.byteLength);

    output.set(bytes);

    return output.buffer;
  }

  private async loadRuntimeConfig(): Promise<any> {

    const response =
      await fetch(
        `assets/config.json?t=${Date.now()}`
      );

    return await response.json();
  }

  private async copyRowsToClipboard(
    rows: Array<Array<string | number>>
  ): Promise<void> {

    const text =
      rows
        .map(row => row.join('\t'))
        .join('\n');

    await navigator.clipboard.writeText(text);
  }
}