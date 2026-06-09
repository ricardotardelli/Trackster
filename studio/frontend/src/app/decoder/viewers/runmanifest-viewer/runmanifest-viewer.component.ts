import {
  Component,
  Input,
  OnChanges,
  SimpleChanges
} from '@angular/core';

import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { MatIconModule } from '@angular/material/icon';

import { DefaultMonacoLoader, NGX_MONACO_LOADER_PROVIDER, NgxMonacoEditorComponent, type EditorInitializedEvent } from '@jean-merelis/ngx-monaco-editor';

import * as monaco from 'monaco-editor';

import { registerDbcLanguage } from '../../../dbceditor/dbc-monaco-language';

import { environment } from '../../../../environments/environment';

import {
  GetObjectCommand,
  S3Client
} from '@aws-sdk/client-s3';

import { fetchAuthSession } from 'aws-amplify/auth';

import { S3TreeNode } from '../../decoder.component';

const tracksterMonacoLoader =
  (globalThis as any).__tracksterMonacoLoader ??
  ((globalThis as any).__tracksterMonacoLoader = new DefaultMonacoLoader({
    paths: {
      vs: '/vs'
    }
  }));

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
  signalCountValue: number;
}

interface DbcFileRow {
  dbcFile: string;
  messageCount: string;
  signalCount: string;
}

type RunManifestView = 'summary' | 'dbc' | 'json';

@Component({
  selector: 'app-runmanifest-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule,
    NgxMonacoEditorComponent
  ],
  providers: [
    {
      provide: NGX_MONACO_LOADER_PROVIDER,
      useValue: tracksterMonacoLoader
    }
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

  activeView: RunManifestView = 'summary';

  manifestText = '';

  filteredManifestText = '';

  dbcPreviewText = '';

  filteredDbcPreviewText = '';

  overviewCards: ViewerCard[] = [];

  simulationCards: ViewerCard[] = [];

  gpsCards: ViewerCard[] = [];

  coverageCards: ViewerCard[] = [];

  dbcFileRows: DbcFileRow[] = [];

  canFrameRows: CanFrameRow[] = [];

  filteredCanFrameRows: CanFrameRow[] = [];

  private manifest: any = null;

  private dbcEditorInstance: monaco.editor.IStandaloneCodeEditor | null = null;

  dbcEditorOptions: monaco.editor.IStandaloneEditorConstructionOptions = {
    automaticLayout: true,
    fixedOverflowWidgets: true,
    minimap: {
      enabled: false
    },
    fontSize: 13,
    lineHeight: 20,
    lineNumbers: 'on',
    lineNumbersMinChars: 3,
    glyphMargin: false,
    folding: false,
    roundedSelection: false,
    scrollBeyondLastLine: false,
    wordWrap: 'off',
    tabSize: 2,
    insertSpaces: true,
    readOnly: true,
    language: 'dbc',
    theme: 'dbcVsCodeLight',
    overviewRulerBorder: false,
    hideCursorInOverviewRuler: true,
    padding: {
      top: 10,
      bottom: 10
    },
    scrollbar: {
      vertical: 'auto',
      horizontal: 'auto',
      verticalScrollbarSize: 8,
      horizontalScrollbarSize: 8,
      alwaysConsumeMouseWheel: false
    }
  };

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

  onDbcEditorInit(
    event: EditorInitializedEvent
  ): void {

    this.dbcEditorInstance =
      event.editor;

    registerDbcLanguage(
      event.monaco
    );

    const model =
      this.dbcEditorInstance.getModel();

    if (model) {
      event.monaco.editor.setModelLanguage(
        model,
        'dbc'
      );
    }

    event.monaco.editor.setTheme(
      'dbcVsCodeLight'
    );

    setTimeout(() => {
      this.dbcEditorInstance?.layout();
    }, 0);
  }

  setView(
    view: RunManifestView
  ): void {

    this.activeView = view;

    this.applyRunManifestSearch();

    setTimeout(() => {
      this.dbcEditorInstance?.layout();
    }, 0);
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

      this.filteredDbcPreviewText =
        this.dbcPreviewText;

      return;
    }

    this.filteredCanFrameRows =
      this.canFrameRows.filter((row) =>
        [
          row.canId,
          row.messageName,
          row.dbcFile,
          row.idf,
          row.signalCount
        ].some((value) =>
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

    this.filteredDbcPreviewText =
      this.dbcPreviewText
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

    this.filteredDbcPreviewText =
      this.dbcPreviewText;
  }

  toggleWrap(): void {

    const nextWrap: monaco.editor.IEditorOptions['wordWrap'] =
      this.dbcEditorOptions.wordWrap === 'off'
        ? 'on'
        : 'off';

    this.dbcEditorOptions.wordWrap =
      nextWrap;

    this.dbcEditorInstance?.updateOptions({
      wordWrap: nextWrap
    });

    this.dbcEditorInstance?.layout();
  }

  downloadDbcPreview(): void {

    const blob =
      new Blob(
        [this.dbcPreviewText],
        {
          type: 'text/plain;charset=utf-8'
        }
      );

    const url =
      window.URL.createObjectURL(blob);

    const anchor =
      document.createElement('a');

    anchor.href = url;
    anchor.download = 'run-manifest-preview.dbc';
    anchor.click();

    window.URL.revokeObjectURL(url);
  }

  private async loadRunManifest(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingRunManifest = true;

    this.runManifestErrorMessage = '';

    this.runManifestSearchText = '';

    this.activeView = 'summary';

    this.manifest = null;

    this.manifestText = '';

    this.filteredManifestText = '';

    this.dbcPreviewText = '';

    this.filteredDbcPreviewText = '';

    this.overviewCards = [];

    this.simulationCards = [];

    this.gpsCards = [];

    this.coverageCards = [];

    this.dbcFileRows = [];

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

      this.buildDbcPreview();

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

    const dbc =
      this.manifest?.dbc ?? {};

    const gps =
      this.manifest?.gps ?? {};

    this.canFrameRows =
      this.buildCanFrameRows();

    this.filteredCanFrameRows = [
      ...this.canFrameRows
    ];

    this.dbcFileRows =
      this.buildDbcFileRows();

    const totalSignalCount =
      this.canFrameRows.reduce(
        (total, row) =>
          total + row.signalCountValue,
        0
      );

    this.overviewCards = [
      {
        label: 'Run ID',
        value: String(
          this.manifest?.runId ??
          this.manifest?.timestamp ??
          '-'
        ),
        detail: 'Simulation execution reference'
      },
      {
        label: 'Created at',
        value: this.formatDateTime(
          this.manifest?.createdAt
        ),
        detail: 'Manifest creation time'
      },
      {
        label: 'Initial time',
        value: String(
          simulation.initialDateTime ??
          '-'
        ),
        detail: 'Simulation start reference'
      },
      {
        label: 'Generation',
        value: this.formatGenerationType(
          simulation.generationType
        ),
        detail: 'Generation strategy'
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

    this.gpsCards = [
      {
        label: 'GPS blocks',
        value: this.formatNumber(
          gps.gpsBlockCount
        ),
        detail: 'Blocks containing route data'
      },
      {
        label: 'Coordinate runs',
        value: this.formatNumber(
          gps.gpsCoordinateRuns
        ),
        detail: 'Compressed coordinate groups'
      },
      {
        label: 'Route points',
        value: this.formatNumber(
          Array.isArray(gps.gpsCoordinates)
            ? gps.gpsCoordinates.length
            : 0
        ),
        detail: 'Manifest route entries'
      }
    ];

    this.coverageCards = [
      {
        label: 'Resolved CAN IDs',
        value: this.formatNumber(
          dbc.resolvedCanIdCount ??
          dbc.selectedCanFrames ??
          this.canFrameRows.length
        ),
        detail: 'Messages ready for decode'
      },
      {
        label: 'Signals',
        value: this.formatNumber(
          totalSignalCount
        ),
        detail: 'Decoded signal definitions'
      },
      {
        label: 'DBC files',
        value: this.formatNumber(
          this.dbcFileRows.length
        ),
        detail: 'Sources used for CAN mapping'
      },
      {
        label: 'Missing CAN IDs',
        value: this.formatNumber(
          Array.isArray(dbc.missingCanIds)
            ? dbc.missingCanIds.length
            : 0
        ),
        detail: 'Unresolved requested messages'
      }
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
            dbc.selectedCanFrames ??
            this.canFrameRows.length
          )
        },
        {
          label: 'Signals',
          value: this.formatNumber(
            totalSignalCount
          )
        },
        {
          label: 'Duration',
          value: `${this.formatNumber(simulation.durationSec)} s`
        }
      ]
    };
  }

  private buildDbcPreview(): void {

    const lines: string[] = [];

    lines.push('VERSION ""');
    lines.push('');
    lines.push('NS_ :');
    lines.push('\tNS_DESC_');
    lines.push('\tCM_');
    lines.push('\tBA_DEF_');
    lines.push('\tBA_');
    lines.push('\tVAL_');
    lines.push('\tCAT_DEF_');
    lines.push('\tCAT_');
    lines.push('\tFILTER');
    lines.push('\tBA_DEF_DEF_');
    lines.push('\tEV_DATA_');
    lines.push('\tENVVAR_DATA_');
    lines.push('\tSGTYPE_');
    lines.push('\tSGTYPE_VAL_');
    lines.push('\tBA_DEF_SGTYPE_');
    lines.push('\tBA_SGTYPE_');
    lines.push('\tSIG_TYPE_REF_');
    lines.push('\tVAL_TABLE_');
    lines.push('\tSIG_GROUP_');
    lines.push('\tSIG_VALTYPE_');
    lines.push('\tSIGTYPE_VALTYPE_');
    lines.push('\tBO_TX_BU_');
    lines.push('\tBA_DEF_REL_');
    lines.push('\tBA_REL_');
    lines.push('\tBA_DEF_DEF_REL_');
    lines.push('\tBU_SG_REL_');
    lines.push('\tBU_EV_REL_');
    lines.push('\tBU_BO_REL_');
    lines.push('\tSG_MUL_VAL_');
    lines.push('');
    lines.push('BS_:');
    lines.push('');
    lines.push('BU_: Vector__XXX');
    lines.push('');

    const resolvedCanFrames =
      this.manifest?.dbc?.resolvedCanFrames;

    const canFrames =
      Array.isArray(resolvedCanFrames)
        ? resolvedCanFrames
        : this.manifest?.dbc?.canFrames;

    if (!Array.isArray(canFrames)) {

      this.dbcPreviewText =
        lines.join('\n');

      this.filteredDbcPreviewText =
        this.dbcPreviewText;

      return;
    }

    for (const frame of canFrames) {

      const rawCanId =
        frame?.canId ??
        frame?.frame?.id ??
        frame?.frame?.i ??
        0;

      const canId =
        this.parseCanIdForDbc(rawCanId);

      const messageName =
        this.sanitizeDbcIdentifier(
          String(
            frame?.messageName ??
            frame?.frame?.n ??
            `Message_${canId}`
          )
        );

      const dlc =
        Number(
          frame?.frame?.l ??
          frame?.frame?.dlc ??
          8
        );

      lines.push(
        `BO_ ${canId} ${messageName}: ${dlc} Vector__XXX`
      );

      const signals =
        frame?.frame?.s;

      if (Array.isArray(signals)) {

        for (const signal of signals) {

          const signalName =
            this.sanitizeDbcIdentifier(
              String(
                signal?.n ??
                signal?.name ??
                'Signal'
              )
            );

          const startBit =
            Number(
              signal?.sb ??
              signal?.startBit ??
              0
            );

          const bitLength =
            Number(
              signal?.bl ??
              signal?.bitLength ??
              1
            );

          const byteOrder =
            Number(
              signal?.bo ??
              signal?.byteOrder ??
              0
            );

          const signed =
            Number(
              signal?.sg ??
              signal?.signed ??
              0
            );

          const factor =
            Number(
              signal?.f ??
              signal?.factor ??
              1
            );

          const offset =
            Number(
              signal?.o ??
              signal?.offset ??
              0
            );

          const min =
            Number(
              signal?.min ??
              signal?.minimum ??
              0
            );

          const max =
            Number(
              signal?.max ??
              signal?.maximum ??
              0
            );

          const unit =
            String(
              signal?.u ??
              signal?.unit ??
              ''
            ).replace(/"/g, '');

          lines.push(
            ` SG_ ${signalName} : ${startBit}|${bitLength}@${byteOrder}${signed ? '-' : '+'} (${factor},${offset}) [${min}|${max}] "${unit}" Vector__XXX`
          );
        }
      }

      lines.push('');
    }

    this.dbcPreviewText =
      lines.join('\n');

    this.filteredDbcPreviewText =
      this.dbcPreviewText;
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

      const signalCountValue =
        Array.isArray(signals)
          ? signals.length
          : 0;

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
            signalCountValue
          ),

        signalCountValue
      };
    });
  }

  private buildDbcFileRows():
    DbcFileRow[] {

    const byDbc =
      new Map<string, {
        messageCount: number;
        signalCount: number;
      }>();

    for (const row of this.canFrameRows) {

      const current =
        byDbc.get(row.dbcFile) ??
        {
          messageCount: 0,
          signalCount: 0
        };

      current.messageCount += 1;

      current.signalCount +=
        row.signalCountValue;

      byDbc.set(
        row.dbcFile,
        current
      );
    }

    return Array.from(
      byDbc.entries()
    ).map(([dbcFile, value]) => ({
      dbcFile,
      messageCount:
        this.formatNumber(value.messageCount),
      signalCount:
        this.formatNumber(value.signalCount)
    }));
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

  private parseCanIdForDbc(
    value: unknown
  ): number {

    if (typeof value === 'number') {
      return value;
    }

    const text =
      String(value ?? '0').trim();

    if (
      text.toLowerCase().startsWith('0x')
    ) {
      return parseInt(text, 16);
    }

    const parsed =
      Number(text);

    return Number.isFinite(parsed)
      ? parsed
      : 0;
  }

  private sanitizeDbcIdentifier(
    value: string
  ): string {

    const sanitized =
      value
        .trim()
        .replace(/[^A-Za-z0-9_]/g, '_')
        .replace(/_{2,}/g, '_')
        .replace(/^_+|_+$/g, '');

    if (!sanitized) {
      return 'Unnamed';
    }

    if (/^[0-9]/.test(sanitized)) {
      return `_${sanitized}`;
    }

    return sanitized;
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

  private formatGenerationType(
    value: unknown
  ): string {

    return String(value ?? '-')
      .replace(/_/g, ' ');
  }
}