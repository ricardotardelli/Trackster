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

interface ViewerCard {
  label: string;
  value: string;
  detail?: string;
}

interface CanFrameRow {
  canId: string;
  messageName: string;
  dbcFile: string;
  idf: string;
  signalCount: string;
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

  showRawManifest = false;

  manifestText = '';

  filteredManifestText = '';

  overviewCards: ViewerCard[] = [];

  simulationCards: ViewerCard[] = [];

  outputCards: ViewerCard[] = [];

  dbcCards: ViewerCard[] = [];

  canFrameRows: CanFrameRow[] = [];

  filteredCanFrameRows: CanFrameRow[] = [];

  private manifest: any = null;

  runManifestViewer = {
    summary: [] as ViewerCard[]
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

      this.filteredCanFrameRows = [
        ...this.canFrameRows
      ];

      this.filteredManifestText =
        this.manifestText;

      return;
    }

    this.filteredCanFrameRows =
      this.canFrameRows.filter((row) =>
        Object.values(row)
          .some((value) =>
            String(value)
              .toLowerCase()
              .includes(searchText)
          )
      );

    this.filteredManifestText =
      this.manifestText
        .split('\n')
        .filter((line) =>
          line
            .toLowerCase()
            .includes(searchText)
        )
        .join('\n');
  }

  clearRunManifestSearch(): void {

    this.runManifestSearchText = '';

    this.filteredCanFrameRows = [
      ...this.canFrameRows
    ];

    this.filteredManifestText =
      this.manifestText;
  }

  toggleRawManifest(): void {

    this.showRawManifest =
      !this.showRawManifest;
  }

  private async loadRunManifest(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingRunManifest = true;

    this.runManifestErrorMessage = '';

    this.runManifestSearchText = '';

    this.showRawManifest = false;

    this.manifest = null;

    this.manifestText = '';

    this.filteredManifestText = '';

    this.overviewCards = [];

    this.simulationCards = [];

    this.outputCards = [];

    this.dbcCards = [];

    this.canFrameRows = [];

    this.filteredCanFrameRows = [];

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

      this.buildViewModel();

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

  private buildViewModel(): void {

    const simulation =
      this.manifest?.simulation ?? {};

    const output =
      this.manifest?.output ?? {};

    const dbc =
      this.manifest?.dbc ?? {};

    const gps =
      this.manifest?.gps ?? {};

    const dbcFiles =
      Array.isArray(dbc.dbcFiles)
        ? dbc.dbcFiles
        : [];

    this.overviewCards = [
      {
        label: 'Run ID',
        value: String(
          this.manifest?.runId ??
          this.manifest?.timestamp ??
          '-'
        ),
        detail: 'Generated simulation run'
      },
      {
        label: 'Client',
        value: String(
          this.manifest?.clientId ??
          this.manifest?.customerId ??
          '-'
        ),
        detail: 'Tenant isolation key'
      },
      {
        label: 'Format',
        value: String(
          output.outputFormat ??
          '-'
        ),
        detail: 'Generated output format'
      },
      {
        label: 'Created at',
        value: this.formatDateTime(
          this.manifest?.createdAt
        ),
        detail: 'Manifest creation time'
      }
    ];

    this.simulationCards = [
      {
        label: 'Vehicles',
        value: this.formatNumber(
          simulation.amountOfVehicles
        ),
        detail: 'Synthetic vehicles'
      },
      {
        label: 'Duration',
        value: `${this.formatNumber(simulation.durationSec)} s`,
        detail: `${this.formatNumber(simulation.amountOfTime)} hour(s)`
      },
      {
        label: 'Blocks',
        value: this.formatNumber(
          simulation.numberOfBlocks
        ),
        detail: `${this.formatNumber(simulation.blocksSize)} bytes each`
      },
      {
        label: 'Interval',
        value: `${this.formatNumber(simulation.intervalSec)} s`,
        detail: 'Block interval'
      },
      {
        label: 'Speed',
        value: `${this.formatNumber(simulation.speed)} ${simulation.unity ?? ''}`.trim(),
        detail: 'Requested simulation speed'
      },
      {
        label: 'Driver profile',
        value: String(
          simulation.driverProfile ??
          '-'
        ),
        detail: 'Behavior model'
      }
    ];

    this.outputCards = [
      {
        label: 'Bucket',
        value: String(
          output.bucket ??
          '-'
        ),
        detail: 'Storage target'
      },
      {
        label: 'Run folder',
        value: String(
          output.runFolder ??
          '-'
        ),
        detail: 'S3 folder'
      },
      {
        label: 'Manifest key',
        value: String(
          output.manifestKey ??
          '-'
        ),
        detail: 'Current JSON source'
      },
      {
        label: 'GPS blocks',
        value: this.formatNumber(
          gps.gpsBlockCount
        ),
        detail: `${this.formatNumber(gps.gpsCoordinateRuns)} coordinate runs`
      }
    ];

    this.dbcCards = [
      {
        label: 'DBC files',
        value: this.formatNumber(
          dbcFiles.length
        ),
        detail: dbcFiles.join(', ') || '-'
      },
      {
        label: 'Selected CAN frames',
        value: this.formatNumber(
          dbc.selectedCanFrames
        ),
        detail: 'Requested from simulation'
      },
      {
        label: 'Resolved CAN IDs',
        value: this.formatNumber(
          dbc.resolvedCanIdCount
        ),
        detail: 'Ready for decode'
      },
      {
        label: 'Missing CAN IDs',
        value: this.formatNumber(
          Array.isArray(dbc.missingCanIds)
            ? dbc.missingCanIds.length
            : 0
        ),
        detail: 'Unresolved messages'
      }
    ];

    this.canFrameRows =
      this.buildCanFrameRows();

    this.filteredCanFrameRows = [
      ...this.canFrameRows
    ];

    this.runManifestViewer = {
      summary: [
        {
          label: 'Run ID',
          value: String(
            this.manifest?.runId ??
            '-'
          )
        },
        {
          label: 'Vehicles',
          value: this.formatNumber(
            simulation.amountOfVehicles
          )
        },
        {
          label: 'Blocks',
          value: this.formatNumber(
            simulation.numberOfBlocks
          )
        },
        {
          label: 'CAN IDs',
          value: this.formatNumber(
            dbc.resolvedCanIdCount ??
            dbc.selectedCanFrames
          )
        },
        {
          label: 'DBC Files',
          value: this.formatNumber(
            dbcFiles.length
          )
        },
        {
          label: 'Duration',
          value: `${this.formatNumber(simulation.durationSec)} s`
        }
      ]
    };
  }

  private buildCanFrameRows():
    CanFrameRow[] {

    const resolvedCanFrames =
      this.manifest?.dbc?.resolvedCanFrames;

    const canFrames =
      Array.isArray(resolvedCanFrames)
        ? resolvedCanFrames
        : this.manifest?.dbc?.canFrames;

    if (!Array.isArray(canFrames)) {
      return [];
    }

    return canFrames.map((frame: any) => {

      const signals =
        frame?.frame?.s;

      return {
        canId:
          String(frame?.canId ?? '-'),

        messageName:
          String(
            frame?.messageName ??
            frame?.frame?.n ??
            '-'
          ),

        dbcFile:
          String(frame?.dbcFile ?? '-'),

        idf:
          String(
            frame?.idf ??
            frame?.frame?.idf ??
            '-'
          ),

        signalCount:
          this.formatNumber(
            Array.isArray(signals)
              ? signals.length
              : 0
          )
      };
    });
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

  private formatNumber(
    value: unknown
  ): string {

    const numericValue =
      Number(value ?? 0);

    if (Number.isNaN(numericValue)) {
      return '0';
    }

    return numericValue.toLocaleString();
  }

  private formatDateTime(
    value: unknown
  ): string {

    if (!value) {
      return '-';
    }

    const date =
      new Date(String(value));

    if (Number.isNaN(date.getTime())) {
      return String(value);
    }

    return date.toLocaleString();
  }
}