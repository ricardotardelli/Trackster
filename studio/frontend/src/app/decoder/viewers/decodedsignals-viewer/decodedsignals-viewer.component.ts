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

import {
  parseTracksterBin,
  ParsedTracksterBin,
  ParsedTracksterBinFrame,
  ParsedTracksterBinSignal
} from '../../parser/decoder.bin.parser';

import { S3TreeNode } from '../../decoder.component';

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  customerId?: string;
  clientId?: string;
}

interface SignalCatalogRow {
  key: string;
  blockIndex: number;
  canId: string;
  messageName: string;
  signalName: string;
  value: string;
  firstValue: string;
  lastValue: string;
  numericValue: number | null;
  min: string;
  max: string;
  avg: string;
  numericMin: number | null;
  numericMax: number | null;
  numericAvg: number | null;
  numericSum: number;
  numericCount: number;
  samples: number;
  changes: number;
  firstBlockIndex: number;
  lastBlockIndex: number;
  raw: string;
  searchText: string;
}

interface SortRule {
  column: keyof SignalCatalogRow;
  direction: 'asc' | 'desc';
}

@Component({
  selector: 'app-decodedsignals-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule
  ],
  templateUrl: './decodedsignals-viewer.component.html',
  styleUrl: './decodedsignals-viewer.component.css'
})
export class DecodedSignalsViewerComponent implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  filterText = '';

  isLoading = false;

  expandedDecodedSignalKey: string | null = null;

  summary = [
    { label: 'Signals', value: '0' },
    { label: 'Messages', value: '0' },
    { label: 'Samples', value: '0' },
    { label: 'Blocks', value: '0' },
    { label: 'Coverage', value: '0%' }
  ];

  signalCatalog: SignalCatalogRow[] = [];

  sortRules: SortRule[] = [
    {
      column: 'messageName',
      direction: 'asc'
    },
    {
      column: 'signalName',
      direction: 'asc'
    }
  ];

  async ngOnChanges(
    changes: SimpleChanges
  ): Promise<void> {

    if (
      changes['selectedNode'] &&
      this.selectedNode
    ) {
      await this.loadDecodedSignals(
        this.selectedNode
      );
    }
  }

  getVisibleRows(): SignalCatalogRow[] {
    const filter =
      this.filterText
        .trim()
        .toLowerCase();

    const filteredRows =
      !filter
        ? [...this.signalCatalog]
        : this.signalCatalog.filter(row =>
            row.searchText.includes(filter)
          );

    return this.sortRows(filteredRows);
  }

  toggleDecodedSignalRow(
    row: SignalCatalogRow
  ): void {

    this.expandedDecodedSignalKey =
      this.expandedDecodedSignalKey === row.key
        ? null
        : row.key;
  }

  isDecodedSignalExpanded(
    row: SignalCatalogRow
  ): boolean {

    return this.expandedDecodedSignalKey === row.key;
  }

  getSignalChangedPercent(
    row: SignalCatalogRow
  ): string {

    if (row.samples <= 0) {
      return '0%';
    }

    const percent =
      (row.changes / row.samples) * 100;

    return `${percent.toFixed(1)}%`;
  }

  onSortColumn(
    event: MouseEvent,
    column: keyof SignalCatalogRow
  ): void {

    const existingIndex =
      this.sortRules.findIndex(rule =>
        rule.column === column
      );

    if (!event.shiftKey) {

      if (existingIndex === 0) {

        const current =
          this.sortRules[0];

        this.sortRules = [
          {
            column,
            direction:
              current.direction === 'asc'
                ? 'desc'
                : 'asc'
          }
        ];

        return;
      }

      this.sortRules = [
        {
          column,
          direction: 'asc'
        }
      ];

      return;
    }

    if (existingIndex >= 0) {

      const updated =
        [...this.sortRules];

      updated[existingIndex] = {
        column,
        direction:
          updated[existingIndex].direction === 'asc'
            ? 'desc'
            : 'asc'
      };

      this.sortRules = updated;

      return;
    }

    this.sortRules = [
      ...this.sortRules,
      {
        column,
        direction: 'asc'
      }
    ];
  }

  getSortIndicator(
    column: keyof SignalCatalogRow
  ): string {

    const index =
      this.sortRules.findIndex(rule =>
        rule.column === column
      );

    if (index < 0) {
      return '';
    }

    const rule =
      this.sortRules[index];

    const arrow =
      rule.direction === 'asc'
        ? '↑'
        : '↓';

    return `${arrow}${index + 1}`;
  }

  async copySignalCatalogToClipboard():
    Promise<void> {

    const rows = [
      [
        'CAN ID',
        'Message',
        'Signal',
        'Value',
        'Min',
        'Max',
        'Avg',
        'First Value',
        'Last Value',
        'Changes',
        'Changed %',
        'First Block',
        'Last Block'
      ],
      ...this.getVisibleRows().map(row => [
        row.canId,
        row.messageName,
        row.signalName,
        row.value,
        row.min,
        row.max,
        row.avg,
        row.firstValue,
        row.lastValue,
        row.changes,
        this.getSignalChangedPercent(row),
        row.firstBlockIndex,
        row.lastBlockIndex
      ])
    ];

    await this.copyRowsToClipboard(rows);
  }

  private sortRows(
    rows: SignalCatalogRow[]
  ): SignalCatalogRow[] {

    return rows.sort((a, b) => {

      for (const rule of this.sortRules) {

        const comparison =
          this.compareRows(
            a,
            b,
            rule.column
          );

        if (comparison !== 0) {
          return rule.direction === 'asc'
            ? comparison
            : comparison * -1;
        }
      }

      return 0;
    });
  }

  private compareRows(
    left: SignalCatalogRow,
    right: SignalCatalogRow,
    column: keyof SignalCatalogRow
  ): number {

    if (column === 'value') {
      return this.compareValues(
        left.numericValue ?? left.value,
        right.numericValue ?? right.value
      );
    }

    if (column === 'min') {
      return this.compareValues(
        left.numericMin ?? left.min,
        right.numericMin ?? right.min
      );
    }

    if (column === 'max') {
      return this.compareValues(
        left.numericMax ?? left.max,
        right.numericMax ?? right.max
      );
    }

    if (column === 'avg') {
      return this.compareValues(
        left.numericAvg ?? left.avg,
        right.numericAvg ?? right.avg
      );
    }

    return this.compareValues(
      left[column],
      right[column]
    );
  }

  private compareValues(
    left: unknown,
    right: unknown
  ): number {

    if (
      typeof left === 'number' &&
      typeof right === 'number'
    ) {
      return left - right;
    }

    const leftNumber =
      Number(left);

    const rightNumber =
      Number(right);

    if (
      Number.isFinite(leftNumber) &&
      Number.isFinite(rightNumber)
    ) {
      return leftNumber - rightNumber;
    }

    return String(left ?? '')
      .localeCompare(
        String(right ?? ''),
        undefined,
        {
          numeric: true,
          sensitivity: 'base'
        }
      );
  }

  private async loadDecodedSignals(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoading = true;

    try {

      this.filterText = '';
      this.expandedDecodedSignalKey = null;

      const buffer =
        await this.loadTracksterBinBuffer(node);

      const manifest =
        await this.loadRunManifest(node);

      const parsed =
        parseTracksterBin(
          buffer,
          manifest
        );

      this.buildSignalCatalog(parsed);

    } catch (error) {

      console.error(
        'Failed to load decoded signals',
        error
      );

      this.signalCatalog = [];
      this.expandedDecodedSignalKey = null;

      this.summary = [
        { label: 'Signals', value: '0' },
        { label: 'Messages', value: '0' },
        { label: 'Samples', value: '0' },
        { label: 'Blocks', value: '0' },
        { label: 'Coverage', value: '0%' }
      ];

    } finally {
      this.isLoading = false;
    }
  }

  private buildSignalCatalog(
    parsed: ParsedTracksterBin
  ): void {

    const catalog =
      new Map<string, SignalCatalogRow>();

    const messageKeys =
      new Set<string>();

    const decodedFrameKeys =
      new Set<string>();

    for (const block of parsed.blocks) {

      for (const frame of block.frames) {

        messageKeys.add(frame.canIdHex);

        if (frame.signals.length > 0) {
          decodedFrameKeys.add(
            `${block.blockIndex}-${frame.offset}`
          );
        }

        for (const signal of frame.signals) {
          this.appendSignalToCatalog(
            catalog,
            block.blockIndex,
            frame,
            signal
          );
        }
      }
    }

    this.signalCatalog =
      [...catalog.values()];

    const totalSamples =
      this.signalCatalog.reduce(
        (sum, row) => sum + row.samples,
        0
      );

    const coverage =
      parsed.totalFrameCount > 0
        ? (
            decodedFrameKeys.size /
            parsed.totalFrameCount
          ) * 100
        : 0;

    this.summary = [
      {
        label: 'Signals',
        value:
          this.signalCatalog.length
            .toLocaleString()
      },
      {
        label: 'Messages',
        value:
          messageKeys.size
            .toLocaleString()
      },
      {
        label: 'Samples',
        value:
          totalSamples
            .toLocaleString()
      },
      {
        label: 'Blocks',
        value:
          parsed.blockCount
            .toLocaleString()
      },
      {
        label: 'Coverage',
        value:
          `${coverage.toFixed(1)}%`
      }
    ];
  }

  private appendSignalToCatalog(
    catalog: Map<string, SignalCatalogRow>,
    blockIndex: number,
    frame: ParsedTracksterBinFrame,
    signal: ParsedTracksterBinSignal
  ): void {

    const key = [
      frame.canIdHex,
      frame.messageName,
      signal.name
    ].join('|');

    const numericValue =
      this.parseNumericValue(
        signal.value
      );

    const existing =
      catalog.get(key);

    if (existing) {

      if (signal.value !== existing.lastValue) {
        existing.changes += 1;
      }

      existing.blockIndex = blockIndex;
      existing.value = signal.value;
      existing.lastValue = signal.value;
      existing.numericValue = numericValue;
      existing.raw = signal.raw;
      existing.samples += 1;
      existing.lastBlockIndex = blockIndex;

      if (numericValue !== null) {

        existing.numericMin =
          existing.numericMin === null
            ? numericValue
            : Math.min(
                existing.numericMin,
                numericValue
              );

        existing.numericMax =
          existing.numericMax === null
            ? numericValue
            : Math.max(
                existing.numericMax,
                numericValue
              );

        existing.numericSum += numericValue;
        existing.numericCount += 1;

        existing.numericAvg =
          existing.numericSum /
          existing.numericCount;

        existing.min =
          this.formatStatValue(
            existing.numericMin
          );

        existing.max =
          this.formatStatValue(
            existing.numericMax
          );

        existing.avg =
          this.formatStatValue(
            existing.numericAvg
          );
      }

      existing.searchText =
        this.buildSearchText(existing);

      return;
    }

    const numericMin =
      numericValue;

    const numericMax =
      numericValue;

    const numericAvg =
      numericValue;

    const row: SignalCatalogRow = {
      key,
      blockIndex,
      canId: frame.canIdHex,
      messageName: frame.messageName,
      signalName: signal.name,
      value: signal.value,
      firstValue: signal.value,
      lastValue: signal.value,
      numericValue,
      min: this.formatStatValue(numericMin),
      max: this.formatStatValue(numericMax),
      avg: this.formatStatValue(numericAvg),
      numericMin,
      numericMax,
      numericAvg,
      numericSum:
        numericValue === null
          ? 0
          : numericValue,
      numericCount:
        numericValue === null
          ? 0
          : 1,
      samples: 1,
      changes: 0,
      firstBlockIndex: blockIndex,
      lastBlockIndex: blockIndex,
      raw: signal.raw,
      searchText: ''
    };

    row.searchText =
      this.buildSearchText(row);

    catalog.set(key, row);
  }

  private buildSearchText(
    row: SignalCatalogRow
  ): string {

    return [
      row.blockIndex,
      row.canId,
      row.messageName,
      row.signalName,
      row.value,
      row.firstValue,
      row.lastValue,
      row.min,
      row.max,
      row.avg,
      row.samples,
      row.changes,
      row.firstBlockIndex,
      row.lastBlockIndex,
      row.raw
    ].join(' ').toLowerCase();
  }

  private parseNumericValue(
    value: string
  ): number | null {

    const numeric =
      Number(value);

    if (!Number.isFinite(numeric)) {
      return null;
    }

    return numeric;
  }

  private formatStatValue(
    value: number | null
  ): string {

    if (value === null) {
      return '-';
    }

    if (Number.isInteger(value)) {
      return value.toString();
    }

    return Number(value.toFixed(3)).toString();
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

    const clientId =
      this.resolveClientId(config);

    const runId =
      this.getRunIdFromKey(node.key);

    const manifestKey =
      `${clientId}/${runId}/run-manifest.json`;

    const buffer =
      await this.getS3ObjectBuffer(
        bucket!,
        manifestKey
      );

    const manifestText =
      new TextDecoder('utf-8')
        .decode(buffer);

    return JSON.parse(manifestText);
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
        'Failed config.json'
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