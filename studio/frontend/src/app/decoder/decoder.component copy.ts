import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { NestedTreeControl } from '@angular/cdk/tree';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule, MatTreeNestedDataSource } from '@angular/material/tree';
import { FormsModule } from '@angular/forms';

import { environment } from '../../environments/environment';
import { MatMenuModule } from '@angular/material/menu';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatCheckboxModule } from '@angular/material/checkbox';

import { fetchAuthSession } from 'aws-amplify/auth';
import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client
} from '@aws-sdk/client-s3';

import { parseTracksterBin } from './parser/decoder.bin.parser';

interface S3TreeNode {
  name: string;
  key: string;
  children?: S3TreeNode[];
}

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  customerId?: string;
  clientId?: string;
  decoderApi?: {
    binFilesCatalogUrl?: string;
    binGetFiles?: string;
    binGetManifest?: string;
  };
}

@Component({
  selector: 'app-decoder',
  standalone: true,
  imports: [
    CommonModule,
    MatTreeModule,
    MatIconModule,
    FormsModule,
    MatMenuModule,
    MatFormFieldModule,
    MatSelectModule,
    MatCheckboxModule
  ],
  templateUrl: './decoder.component.html',
  styleUrl: './decoder.component.css',
})
export class DecoderComponent implements OnInit {
  selectedViewerMode = 'trackster-bin';

  isDecoderFilterOpen = false;
  decoderFilterText = '';

  isLoadingBinCatalog = false;
  isLoadingBlockPage = false;

  selectedS3Key: string | null = null;
  selectedBinKeys: string[] = [];

  currentBlockPage = 1;
  blockPageInput = 1;

  readonly blocksPerPage = 50;

  totalBlockCount = 0;

  private fullParsedBlocks: any[] = [];

  payloadViewer = {
    fileName: '',
    summary: [] as any[],
    headerFields: [] as any[],
    blocks: [] as any[]
  };

  readonly s3TreeControl = new NestedTreeControl<S3TreeNode>(
    node => node.children ?? []
  );

  readonly s3TreeDataSource = new MatTreeNestedDataSource<S3TreeNode>();

  async ngOnInit(): Promise<void> {
    await this.loadS3GeneratedFilesTree();
  }

  hasChild = (_: number, node: S3TreeNode): boolean => {
    return !!node.children && node.children.length > 0;
  };

  trackByS3Node = (_: number, node: S3TreeNode): string => {
    return node.key;
  };

  async selectS3Node(node: S3TreeNode): Promise<void> {
    this.selectedS3Key = node.key;

    if (!this.isBinFile(node)) {
      return;
    }

    if (this.selectedViewerMode !== 'trackster-bin') {
      return;
    }

    await this.loadTracksterBinForViewer(node);
  }

  isBinFile(node: S3TreeNode): boolean {
    return node.name.toLowerCase().endsWith('.bin');
  }

  isJsonFile(node: S3TreeNode): boolean {
    return node.name.toLowerCase().endsWith('.json');
  }

  isBinSelected(node: S3TreeNode): boolean {
    return this.selectedBinKeys.includes(node.key);
  }

  toggleBinSelection(node: S3TreeNode, checked: boolean): void {
    if (!this.isBinFile(node)) {
      return;
    }

    if (checked) {
      if (!this.selectedBinKeys.includes(node.key)) {
        this.selectedBinKeys = [...this.selectedBinKeys, node.key];
      }

      return;
    }

    this.selectedBinKeys = this.selectedBinKeys
      .filter(key => key !== node.key);
  }

  isFolderFullySelected(node: S3TreeNode): boolean {
    const binKeys = this.getBinKeysFromFolder(node);

    if (binKeys.length === 0) {
      return false;
    }

    return binKeys.every(key => this.selectedBinKeys.includes(key));
  }

  isFolderPartiallySelected(node: S3TreeNode): boolean {
    const binKeys = this.getBinKeysFromFolder(node);

    if (binKeys.length === 0) {
      return false;
    }

    const selectedCount = binKeys
      .filter(key => this.selectedBinKeys.includes(key))
      .length;

    return selectedCount > 0 && selectedCount < binKeys.length;
  }

  toggleFolderSelection(node: S3TreeNode, checked: boolean): void {
    const folderBinKeys = this.getBinKeysFromFolder(node);

    if (folderBinKeys.length === 0) {
      return;
    }

    if (checked) {
      const mergedKeys = new Set<string>([
        ...this.selectedBinKeys,
        ...folderBinKeys
      ]);

      this.selectedBinKeys = [...mergedKeys];
      return;
    }

    const folderKeys = new Set<string>(folderBinKeys);

    this.selectedBinKeys = this.selectedBinKeys
      .filter(key => !folderKeys.has(key));
  }

  toggleDecoderFilter(): void {
    this.isDecoderFilterOpen = !this.isDecoderFilterOpen;

    if (!this.isDecoderFilterOpen) {
      this.decoderFilterText = '';
    }
  }

  clearDecoderFilter(): void {
    this.decoderFilterText = '';
  }

  matchesDecoderFilter(value: string): boolean {
    const filter = this.decoderFilterText.trim().toLowerCase();

    if (!filter) {
      return true;
    }

    return value.toLowerCase().includes(filter);
  }

  toggleFrame(frame: any): void {
    frame.expanded = !frame.expanded;
  }

  toggleBlock(block: any): void {
    block.expanded = !block.expanded;
  }

  getTotalBlockCount(): number {
    return this.totalBlockCount;
  }

  getTotalBlockPages(): number {
    return Math.max(
      1,
      Math.ceil(this.totalBlockCount / this.blocksPerPage)
    );
  }

  getBlockPageStart(): number {
    if (this.totalBlockCount === 0) {
      return 0;
    }

    return ((this.currentBlockPage - 1) * this.blocksPerPage) + 1;
  }

  getBlockPageEnd(): number {
    return Math.min(
      this.currentBlockPage * this.blocksPerPage,
      this.totalBlockCount
    );
  }

  isFirstBlockPage(): boolean {
    return this.currentBlockPage <= 1;
  }

  isLastBlockPage(): boolean {
    return this.currentBlockPage >= this.getTotalBlockPages();
  }

  async loadPreviousBlockPage(): Promise<void> {
    if (this.isFirstBlockPage()) {
      return;
    }

    await this.goToBlockPage(this.currentBlockPage - 1);
  }

  async loadNextBlockPage(): Promise<void> {
    if (this.isLastBlockPage()) {
      return;
    }

    await this.goToBlockPage(this.currentBlockPage + 1);
  }

  async goToBlockPageFromInput(): Promise<void> {
    await this.goToBlockPage(this.blockPageInput);
  }

  private async goToBlockPage(page: number): Promise<void> {
    const safePage = this.normalizeBlockPage(page);

    this.currentBlockPage = safePage;
    this.blockPageInput = safePage;

    await this.refreshCurrentBlockPage();
  }

  private normalizeBlockPage(page: number): number {
    const totalPages = this.getTotalBlockPages();

    if (!Number.isFinite(page)) {
      return this.currentBlockPage;
    }

    const integerPage = Math.trunc(page);

    return Math.min(
      Math.max(integerPage, 1),
      totalPages
    );
  }

  private async refreshCurrentBlockPage(): Promise<void> {
    this.isLoadingBlockPage = true;

    try {
      const start = (this.currentBlockPage - 1) * this.blocksPerPage;
      const end = start + this.blocksPerPage;

      this.payloadViewer.blocks = this.fullParsedBlocks.slice(start, end);
    } finally {
      this.isLoadingBlockPage = false;
    }
  }

  private async loadTracksterBinForViewer(node: S3TreeNode): Promise<void> {
    try {
      this.isLoadingBlockPage = true;

      this.currentBlockPage = 1;
      this.blockPageInput = 1;

      const buffer = await this.loadTracksterBinBuffer(node);
      const manifest = await this.loadRunManifest(node);

      const parsed = parseTracksterBin(buffer, manifest);

      this.totalBlockCount = parsed.blockCount;

      const firstTimestampNs = parsed.blocks[0]?.timestampNs ?? '0';
      const secondTimestampNs = parsed.blocks[1]?.timestampNs ?? firstTimestampNs;
      const lastBlock = parsed.blocks[parsed.blocks.length - 1];
      const lastTimestampNs = lastBlock?.timestampNs ?? firstTimestampNs;

      this.payloadViewer = {
        fileName: node.name,

        summary: [
          {
            label: 'Blocks',
            value: parsed.blockCount.toLocaleString()
          },
          {
            label: 'Frames',
            value: parsed.totalFrameCount.toLocaleString()
          },
          {
            label: 'Interval',
            value: this.formatBlockDuration(firstTimestampNs, secondTimestampNs)
          },
          {
            label: 'Duration',
            value: this.formatBlockDuration(firstTimestampNs, lastTimestampNs)
          },
          {
            label: 'Size',
            value: this.formatBytes(parsed.totalBytes)
          }
        ],

        headerFields: [
          {
            label: 'Magic',
            value: parsed.magic
          },
          {
            label: 'Version',
            value: `${parsed.versionMajor}.${parsed.versionMinor}`
          },
          {
            label: 'Header bytes',
            value: parsed.headerBytes
          },
          {
            label: 'Block header',
            value: parsed.blockHeaderBytes
          },
          {
            label: 'Frame header',
            value: parsed.frameFixedHeaderBytes
          },
          {
            label: 'Blocks',
            value: parsed.blockCount
          },
          {
            label: 'Frames',
            value: parsed.totalFrameCount
          },
          {
            label: 'Payload bytes',
            value: parsed.totalPayloadBytes.toLocaleString()
          },
          {
            label: 'File bytes',
            value: parsed.totalBytes.toLocaleString()
          }
        ],

        blocks: []
      };

      this.fullParsedBlocks = parsed.blocks.map((block: any) => {
        const startNs = BigInt(block.timestampNs);
        const nextBlock = parsed.blocks[block.blockIndex + 1];

        const endNs = nextBlock
          ? BigInt(nextBlock.timestampNs)
          : startNs;

        return {
          index: block.blockIndex,

          expanded: block.blockIndex === 0,

          startNs: this.formatRelativeTimeNs(
            firstTimestampNs,
            startNs.toString()
          ),

          endNs: this.formatRelativeTimeNs(
            firstTimestampNs,
            endNs.toString()
          ),

          duration: this.formatBlockDuration(
            startNs.toString(),
            endNs.toString()
          ),

          frameCount: block.frameCount,

          frames: block.frames.map((frame: any) => {
            const signals = Array.isArray(frame.signals)
              ? frame.signals.map((signal: any) => ({
                  name: signal.name,
                  value: signal.value,
                  raw: signal.raw,
                  unit: signal.unit,
                  searchText: [
                    frame.canIdHex,
                    frame.messageName,
                    signal.name
                  ].join(' ')
                }))
              : [];

            return {
              expanded: false,

              searchText: [
                frame.canIdHex,
                frame.messageName,
                frame.payloadBytes,
                ...signals.map((signal: any) => signal.name)
              ].join(' '),

              canId: frame.canIdHex,

              messageName:
                frame.messageName ||
                `CAN_${frame.canIdHex}`,

              time: `${frame.timestampDeltaNs} ns`,

              dlc: frame.payloadLength,

              decodedSignals: Number(
                frame.decodedSignals ?? signals.length
              ),

              payloadHex: frame.payloadBytes,

              signals
            };
          })
        };
      });

      await this.refreshCurrentBlockPage();

    } catch (error) {
      console.error(
        'Failed to parse Trackster BIN',
        error
      );
    } finally {
      this.isLoadingBlockPage = false;
    }
  }

  private formatRelativeTimeNs(baseNs: string, valueNs: string): string {
    const diffNs = BigInt(valueNs) - BigInt(baseNs);
    const seconds = Number(diffNs) / 1_000_000_000;

    return `${seconds.toFixed(3)} s`;
  }

  private formatBlockDuration(startNs: string, endNs: string): string {
    const diffNs = Number(BigInt(endNs) - BigInt(startNs));
    const seconds = diffNs / 1_000_000_000;

    return `${seconds.toFixed(2)} s`;
  }

  private formatBytes(bytes: number): string {
    if (bytes >= 1024 * 1024) {
      return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
    }

    if (bytes >= 1024) {
      return `${(bytes / 1024).toFixed(2)} KB`;
    }

    return `${bytes} B`;
  }

  private async loadTracksterBinBuffer(node: S3TreeNode): Promise<ArrayBuffer> {
    if (this.shouldUseLocalMock()) {
      const response = await fetch('assets/mock/sample.bin');

      if (!response.ok) {
        throw new Error(
          `Failed to load local mock BIN. HTTP ${response.status}`
        );
      }

      return await response.arrayBuffer();
    }

    const config = await this.loadRuntimeConfig();

    const bucket = config.s3Default?.trim();
    const key = node.key;

    if (!bucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    if (!key) {
      throw new Error(
        'Selected BIN node does not contain a valid S3 key.'
      );
    }

    return await this.getS3ObjectBuffer(
      bucket,
      key
    );
  }

  private async loadRunManifest(node: S3TreeNode): Promise<any> {
    if (this.shouldUseLocalMock()) {
      const response = await fetch('assets/mock/run-manifest.json');

      if (!response.ok) {
        console.warn(
          `Local run-manifest.json not found. Decoder will show raw frames only. HTTP ${response.status}`
        );

        return null;
      }

      return await response.json();
    }

    const config = await this.loadRuntimeConfig();

    const bucket = config.s3Default?.trim();
    const clientId = this.resolveClientId(config);
    const runId = this.getRunIdFromKey(node.key);

    if (!bucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    if (!runId) {
      throw new Error(
        `Unable to resolve runId from selected key: ${node.key}`
      );
    }

    const manifestKey = `${clientId}/${runId}/run-manifest.json`;

    const buffer = await this.getS3ObjectBuffer(
      bucket,
      manifestKey
    );

    const manifestText = new TextDecoder('utf-8').decode(buffer);

    try {
      return JSON.parse(manifestText);
    } catch {
      throw new Error(
        `Invalid run-manifest.json at S3 key: ${manifestKey}`
      );
    }
  }

  private async getS3Client(): Promise<S3Client> {
    const config = await this.loadRuntimeConfig();

    const region =
      config.s3Region?.trim() ||
      'us-east-1';

    const session =
      await fetchAuthSession();

    if (!session.credentials) {
      throw new Error(
        'Cognito Identity Pool credentials are not available. Check Amplify configuration for identityPoolId.'
      );
    }

    return new S3Client({
      region,
      credentials: session.credentials
    });
  }

  private async getS3ObjectBuffer(
    bucket: string,
    key: string
  ): Promise<ArrayBuffer> {
    const s3Client = await this.getS3Client();

    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key
      })
    );

    if (!response.Body) {
      throw new Error(
        `S3 object body is empty. Bucket: ${bucket}. Key: ${key}`
      );
    }

    return await this.s3BodyToArrayBuffer(response.Body);
  }

  private async listS3KeysFromBucket(
    bucket: string,
    prefix: string
  ): Promise<string[]> {
    const s3Client = await this.getS3Client();

    const keys: string[] = [];
    let continuationToken: string | undefined;

    do {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken
        })
      );

      for (const item of response.Contents ?? []) {
        if (item.Key) {
          keys.push(item.Key);
        }
      }

      continuationToken = response.NextContinuationToken;

    } while (continuationToken);

    return keys;
  }

  private async s3BodyToArrayBuffer(body: any): Promise<ArrayBuffer> {
    if (typeof body.transformToByteArray === 'function') {
      const bytes = await body.transformToByteArray();

      const output = new Uint8Array(bytes.byteLength);
      output.set(bytes);

      return output.buffer;
    }

    if (typeof body.arrayBuffer === 'function') {
      return await body.arrayBuffer();
    }

    if (body instanceof ReadableStream) {
      const reader = body.getReader();
      const chunks: Uint8Array[] = [];

      let totalLength = 0;

      while (true) {
        const result = await reader.read();

        if (result.done) {
          break;
        }

        if (result.value) {
          chunks.push(result.value);
          totalLength += result.value.length;
        }
      }

      const merged = new Uint8Array(totalLength);
      let offset = 0;

      for (const chunk of chunks) {
        merged.set(chunk, offset);
        offset += chunk.length;
      }

      return merged.buffer;
    }

    throw new Error(
      'Unsupported S3 response body type in browser.'
    );
  }

  private getRunIdFromKey(key: string): string {
    const parts = key
      .split('/')
      .filter(Boolean);

    if (parts.length < 2) {
      return '';
    }

    return parts[1];
  }

  private getBinKeysFromFolder(node: S3TreeNode): string[] {
    const result: string[] = [];

    const walk = (currentNode: S3TreeNode): void => {
      if (this.isBinFile(currentNode)) {
        result.push(currentNode.key);
        return;
      }

      for (const child of currentNode.children ?? []) {
        walk(child);
      }
    };

    walk(node);

    return result;
  }

  private async loadS3GeneratedFilesTree(): Promise<void> {
    this.isLoadingBinCatalog = true;

    try {
      const config = await this.loadRuntimeConfig();
      const clientId = this.resolveClientId(config);

      if (this.shouldUseLocalMock()) {
        this.setTreeData(
          this.buildLocalMockTree()
        );

        return;
      }

      const bucket = config.s3Default?.trim();

      if (!bucket) {
        throw new Error(
          'Missing s3Default in assets/config.json'
        );
      }

      const prefix = `${clientId}/`;

      const keys = await this.listS3KeysFromBucket(
        bucket,
        prefix
      );

      const tree = this.buildTreeFromS3Keys(
        keys,
        clientId
      );

      this.setTreeData(tree);

    } finally {
      this.isLoadingBinCatalog = false;
    }
  }

  private setTreeData(data: S3TreeNode[]): void {
    this.s3TreeControl.expansionModel.clear();

    this.s3TreeControl.expansionModel.select(...data);

    this.s3TreeDataSource.data = data;

    this.s3TreeControl.dataNodes = data;

    this.selectedBinKeys = this.selectedBinKeys.filter(key =>
      this.treeContainsKey(data, key)
    );
  }

  private treeContainsKey(nodes: S3TreeNode[], key: string): boolean {
    for (const node of nodes) {
      if (node.key === key) {
        return true;
      }

      if (
        node.children &&
        this.treeContainsKey(node.children, key)
      ) {
        return true;
      }
    }

    return false;
  }

  private buildTreeFromS3Keys(keys: string[], clientId: string): S3TreeNode[] {
    const runs = new Map<string, S3TreeNode>();
    const prefix = `${clientId}/`;

    for (const rawKey of keys) {
      const key = rawKey.replace(/^generated-files\//, '');

      if (!key.startsWith(prefix)) {
        continue;
      }

      const relativeKey = key.slice(prefix.length);

      const parts = relativeKey
        .split('/')
        .filter(Boolean);

      if (parts.length < 2) {
        continue;
      }

      const runId = parts[0];
      const fileName = parts[parts.length - 1];

      if (!fileName.toLowerCase().endsWith('.bin')) {
        continue;
      }

      let runNode = runs.get(runId);

      if (!runNode) {
        runNode = {
          name: runId,
          key: `${clientId}/${runId}`,
          children: []
        };

        runs.set(runId, runNode);
      }

      runNode.children?.push({
        name: fileName,
        key: `${clientId}/${relativeKey}`
      });
    }

    const runNodes = [...runs.values()]
      .sort((a, b) => b.name.localeCompare(a.name));

    for (const runNode of runNodes) {
      runNode.children = [...(runNode.children ?? [])]
        .sort((a, b) => a.name.localeCompare(b.name));
    }

    return runNodes;
  }

  private async loadRuntimeConfig(): Promise<RuntimeConfig> {
    const response = await fetch(
      `assets/config.json?t=${Date.now()}`
    );

    if (!response.ok) {
      throw new Error(
        `Failed to load assets/config.json. HTTP ${response.status}`
      );
    }

    const config = await response.json();

    return config as RuntimeConfig;
  }

  private resolveClientId(config: RuntimeConfig): string {
    const clientId =
      config.clientId ||
      config.customerId ||
      localStorage.getItem('clientId') ||
      localStorage.getItem('customerId') ||
      localStorage.getItem('tracksterClientId') ||
      localStorage.getItem('tracksterCustomerId') ||
      '00000000';

    if (!/^[a-zA-Z0-9]{8}$/.test(clientId)) {
      throw new Error(
        `Invalid clientId: ${clientId}`
      );
    }

    return clientId;
  }

  private shouldUseLocalMock(): boolean {
    const hostname = window.location.hostname;

    return (
      environment.disableAuth === true &&
      (
        hostname === 'localhost' ||
        hostname === '127.0.0.1'
      )
    );
  }

  private buildLocalMockTree(): S3TreeNode[] {
    return [
      {
        name: '20260508183000',
        key: '00000000/20260508183000',
        children: [
          {
            name: 'VINKDT000001KADUT.bin',
            key: '00000000/20260508183000/VIN000001KADUT.bin'
          }
        ]
      }
    ];
  }

  async copyHeaderToClipboard(): Promise<void> {
    const rows = [
      ['Field', 'Value'],
      ...this.payloadViewer.headerFields.map((field: any) => [
        field.label,
        field.value
      ])
    ];

    await this.copyRowsToClipboard(rows);
  }

  async copyBlocksToClipboard(): Promise<void> {
    const rows = [
      ['Block', 'Start', 'End', 'Duration', 'Frames'],
      ...this.payloadViewer.blocks.map((block: any) => [
        block.index,
        block.startNs,
        block.endNs,
        block.duration,
        block.frameCount
      ])
    ];

    await this.copyRowsToClipboard(rows);
  }

  async copyPayloadViewerToClipboard(): Promise<void> {
    const rows = [
      ['Block', 'CAN ID', 'Message', 'Time', 'DLC', 'Payload'],
      ...this.payloadViewer.blocks.flatMap((block: any) =>
        block.frames.map((frame: any) => [
          block.index,
          frame.canId,
          frame.messageName,
          frame.time,
          frame.dlc,
          frame.payloadHex
        ])
      )
    ];

    await this.copyRowsToClipboard(rows);
  }

  private async copyRowsToClipboard(
    rows: Array<Array<string | number>>
  ): Promise<void> {
    const text = rows
      .map(row => row.join('\t'))
      .join('\n');

    await navigator.clipboard.writeText(text);
  }
}