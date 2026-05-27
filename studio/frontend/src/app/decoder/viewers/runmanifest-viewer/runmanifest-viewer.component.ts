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
  customerId?: string;
  clientId?: string;
}

@Component({
  selector: 'app-runmanifest-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule
  ],
  templateUrl: './runmanifest-viewer.component.html',
  styleUrl: './runmanifest-viewer.component.css'
})
export class RunmanifestViewerComponent
implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoadingRunManifest = false;

  runManifestErrorMessage = '';

  runManifestSearchText = '';

  manifestText = '';

  filteredManifestText = '';

  private manifest: any = null;

  runManifestViewer = {
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
      await this.loadRunManifest(
        this.selectedNode
      );
    }
  }

  applyRunManifestSearch(): void {

    const searchText =
      this.runManifestSearchText
        .trim()
        .toLowerCase();

    if (!searchText) {

      this.filteredManifestText =
        this.manifestText;

      return;
    }

    const lines =
      this.manifestText
        .split('\n');

    this.filteredManifestText =
      lines
        .filter((line) =>
          line
            .toLowerCase()
            .includes(searchText)
        )
        .join('\n');
  }

  clearRunManifestSearch(): void {

    this.runManifestSearchText = '';

    this.filteredManifestText =
      this.manifestText;
  }

  private async loadRunManifest(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingRunManifest = true;

    this.runManifestErrorMessage = '';

    this.runManifestSearchText = '';

    this.manifest = null;

    this.manifestText = '';

    this.filteredManifestText = '';

    this.runManifestViewer = {
      summary: []
    };

    try {

      if (this.shouldUseLocalMock()) {

        await this.loadLocalMockRunManifest();

      } else {

        await this.loadRunManifestFromS3(
          node
        );
      }

      this.manifestText =
        JSON.stringify(
          this.manifest,
          null,
          2
        );

      this.filteredManifestText =
        this.manifestText;

      this.updateSummary();

    } catch (error) {

      console.error(
        'Failed to load run manifest',
        error
      );

      this.runManifestErrorMessage =
        error instanceof Error
          ? error.message
          : 'Failed to load run manifest.';

    } finally {

      this.isLoadingRunManifest = false;
    }
  }

  private async loadRunManifestFromS3(
    node: S3TreeNode
  ): Promise<void> {

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Default?.trim() ||
      's3-trackster-can-bucket';

    const manifestKey =
      this.buildRunManifestKey(
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

  private async loadLocalMockRunManifest():
    Promise<void> {

    const response =
      await fetch(
        'assets/mock/run-manifest.json'
      );

    if (!response.ok) {

      throw new Error(
        `Failed to load local run manifest mock. HTTP ${response.status}`
      );
    }

    this.manifest =
      await response.json();
  }

  private updateSummary(): void {

    const simulation =
      this.manifest?.simulation ?? {};

    const dbc =
      this.manifest?.dbc ?? {};

    const output =
      this.manifest?.output ?? {};

    this.runManifestViewer = {
      summary: [
        {
          label: 'Run ID',
          value:
            String(
              this.manifest?.runId ??
              this.manifest?.timestamp ??
              '-'
            )
        },
        {
          label: 'Client',
          value:
            String(
              this.manifest?.clientId ??
              this.manifest?.customerId ??
              '-'
            )
        },
        {
          label: 'Format',
          value:
            String(
              output.outputFormat ??
              '-'
            )
        },
        {
          label: 'Vehicles',
          value:
            Number(
              simulation.amountOfVehicles ?? 0
            ).toLocaleString()
        },
        {
          label: 'Blocks',
          value:
            Number(
              simulation.numberOfBlocks ?? 0
            ).toLocaleString()
        },
        {
          label: 'CAN IDs',
          value:
            Number(
              dbc.resolvedCanIdCount ??
              dbc.selectedCanFrames ??
              0
            ).toLocaleString()
        },
        {
          label: 'DBC Files',
          value:
            Number(
              Array.isArray(dbc.dbcFiles)
                ? dbc.dbcFiles.length
                : 0
            ).toLocaleString()
        },
        {
          label: 'Duration',
          value:
            `${Number(simulation.durationSec ?? 0).toLocaleString()} s`
        }
      ]
    };
  }

  private buildRunManifestKey(
    inputKey: string
  ): string {

    const lastSlashIndex =
      inputKey.lastIndexOf('/');

    if (lastSlashIndex < 0) {
      return 'run-manifest.json';
    }

    return `${inputKey.slice(0, lastSlashIndex)}/run-manifest.json`;
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
}