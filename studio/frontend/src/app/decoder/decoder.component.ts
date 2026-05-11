import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { NestedTreeControl } from '@angular/cdk/tree';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule, MatTreeNestedDataSource } from '@angular/material/tree';
import { FormsModule } from '@angular/forms';

import { environment } from '../../environments/environment';
import { AuthService } from '../auth/auth.service';
import { MatMenuModule } from '@angular/material/menu';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatCheckboxModule } from '@angular/material/checkbox';

import { parseTracksterBin } from './parser/decoder.bin.parser';

interface S3TreeNode {
  name: string;
  key: string;
  children?: S3TreeNode[];
}

interface RuntimeConfig {
  s3Default?: string;
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

  selectedS3Key: string | null = null;

  selectedBinKeys: string[] = [];

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

  constructor(
    private readonly authService: AuthService
  ) {}

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

  private async loadTracksterBinForViewer(
    node: S3TreeNode
  ): Promise<void> {
    try {
      const buffer = await this.loadTracksterBinBuffer(node);
      const manifest = await this.loadRunManifest(node);

      console.log('RUN MANIFEST LOADED');
      console.log(manifest);

      const parsed = parseTracksterBin(
        buffer,
        manifest
      );

      console.log('TRACKSTER BIN PARSED');
      console.log(parsed);

      const firstTimestampNs =
        parsed.blocks[0]?.timestampNs ?? '0';

      const secondTimestampNs =
        parsed.blocks[1]?.timestampNs ?? firstTimestampNs;

      const lastBlock =
        parsed.blocks[parsed.blocks.length - 1];

      const lastTimestampNs =
        lastBlock?.timestampNs ?? firstTimestampNs;

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
            value: this.formatBlockDuration(
              firstTimestampNs,
              secondTimestampNs
            )
          },
          {
            label: 'Duration',
            value: this.formatBlockDuration(
              firstTimestampNs,
              lastTimestampNs
            )
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

        blocks: parsed.blocks.slice(0, 50).map((block: any) => {
          const startNs = BigInt(block.timestampNs);

          const nextBlock =
            parsed.blocks[block.blockIndex + 1];

          const endNs =
            nextBlock
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

                decodedSignals:
                  Number(frame.decodedSignals ?? signals.length),

                payloadHex:
                  frame.payloadBytes,

                signals
              };
            })
          };
        })
      };

      console.log('TRACKSTER PAYLOAD VIEWER MODEL');
      console.log(this.payloadViewer);

    } catch (error) {
      console.error(
        'Failed to parse Trackster BIN',
        error
      );
    }
  }

  private formatRelativeTimeNs(
    baseNs: string,
    valueNs: string
  ): string {
    const diffNs =
      BigInt(valueNs) - BigInt(baseNs);

    const seconds =
      Number(diffNs) / 1_000_000_000;

    return `${seconds.toFixed(3)} s`;
  }

  private formatBlockDuration(
    startNs: string,
    endNs: string
  ): string {
    const diffNs =
      Number(BigInt(endNs) - BigInt(startNs));

    const seconds =
      diffNs / 1_000_000_000;

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

  private async loadTracksterBinBuffer(
    node: S3TreeNode
  ): Promise<ArrayBuffer> {
    if (this.shouldUseLocalMock()) {
      const response =
        await fetch('assets/mock/sample.bin');

      if (!response.ok) {
        throw new Error(
          `Failed to load local mock BIN. HTTP ${response.status}`
        );
      }

      return await response.arrayBuffer();
    }

    const config =
      await this.loadRuntimeConfig();

    const binGetFilesUrl =
      config.decoderApi?.binGetFiles?.trim();

    const bucket =
      config.s3Default?.trim();

    const clientId =
      this.resolveClientId(config);

    const runId =
      this.getRunIdFromKey(node.key);

    if (!binGetFilesUrl) {
      throw new Error(
        'Missing decoderApi.binGetFiles in assets/config.json'
      );
    }

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

    const token =
      await this.authService.getIdToken();

    const url =
      `${binGetFilesUrl}` +
      `?action=get-bin-files` +
      `&bucket=${encodeURIComponent(bucket)}` +
      `&clientId=${encodeURIComponent(clientId)}` +
      `&runId=${encodeURIComponent(runId)}` +
      `&binFiles=${encodeURIComponent(node.name)}`;

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`
      }
    });

    if (!response.ok) {
      const text = await response.text();

      throw new Error(
        `Failed to load BIN file from decoder API. HTTP ${response.status}. ${text}`
      );
    }

    const payload = await response.json();

    const contentBase64 =
      payload?.files?.[0]?.contentBase64;

    if (!contentBase64 || typeof contentBase64 !== 'string') {
      throw new Error(
        'Decoder API response does not contain files[0].contentBase64.'
      );
    }

    return this.base64ToArrayBuffer(contentBase64);
  }

  private async loadRunManifest(
    node: S3TreeNode
  ): Promise<any> {
    if (this.shouldUseLocalMock()) {
      const response =
        await fetch('assets/mock/run-manifest.json');

      if (!response.ok) {
        console.warn(
          `Local run-manifest.json not found. Decoder will show raw frames only. HTTP ${response.status}`
        );

        return null;
      }

      return await response.json();
    }

    const config =
      await this.loadRuntimeConfig();

    const binGetManifestUrl =
      config.decoderApi?.binGetManifest?.trim();

    const bucket =
      config.s3Default?.trim();

    const clientId =
      this.resolveClientId(config);

    const runId =
      this.getRunIdFromKey(node.key);

    if (!binGetManifestUrl) {
      throw new Error(
        'Missing decoderApi.binGetManifest in assets/config.json'
      );
    }

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

    const token =
      await this.authService.getIdToken();

    const url =
      `${binGetManifestUrl}` +
      `?action=get-run-manifest` +
      `&bucket=${encodeURIComponent(bucket)}` +
      `&clientId=${encodeURIComponent(clientId)}` +
      `&runId=${encodeURIComponent(runId)}`;

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`
      }
    });

    if (!response.ok) {
      const text = await response.text();

      throw new Error(
        `Failed to load run manifest from decoder API. HTTP ${response.status}. ${text}`
      );
    }

    const payload = await response.json();

    if (!payload?.manifest) {
      throw new Error(
        'Decoder API response does not contain manifest.'
      );
    }

    return payload.manifest;
  }

  private base64ToArrayBuffer(base64: string): ArrayBuffer {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);

    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }

    return bytes.buffer;
  }

  private getRunIdFromKey(key: string): string {
    const parts =
      key
        .split('/')
        .filter(Boolean);

    if (parts.length < 2) {
      return '';
    }

    return parts[1];
  }

  private getBinKeysFromFolder(
    node: S3TreeNode
  ): string[] {
    const result: string[] = [];

    const walk = (
      currentNode: S3TreeNode
    ): void => {
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
      const config =
        await this.loadRuntimeConfig();

      const clientId =
        this.resolveClientId(config);

      if (this.shouldUseLocalMock()) {
        this.setTreeData(
          this.buildLocalMockTree()
        );

        return;
      }

      const catalogUrl =
        config.decoderApi?.binFilesCatalogUrl;

      const bucket =
        config.s3Default;

      if (!catalogUrl) {
        throw new Error(
          'Missing decoderApi.binFilesCatalogUrl in assets/config.json'
        );
      }

      if (!bucket) {
        throw new Error(
          'Missing s3Default in assets/config.json'
        );
      }

      const token =
        await this.authService.getIdToken();

      const url =
        `${catalogUrl}` +
        `?clientId=${encodeURIComponent(clientId)}` +
        `&bucket=${encodeURIComponent(bucket)}`;

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      if (!response.ok) {
        const text =
          await response.text();

        throw new Error(
          `Failed to load BIN files catalog. HTTP ${response.status}. ${text}`
        );
      }

      const payload =
        await response.json();

      const keys =
        this.extractS3Keys(payload);

      const tree =
        this.buildTreeFromS3Keys(keys, clientId);

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

    this.selectedBinKeys =
      this.selectedBinKeys.filter(key =>
        this.treeContainsKey(data, key)
      );
  }

  private treeContainsKey(
    nodes: S3TreeNode[],
    key: string
  ): boolean {
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

  private buildTreeFromS3Keys(
    keys: string[],
    clientId: string
  ): S3TreeNode[] {
    const runs =
      new Map<string, S3TreeNode>();

    const prefix =
      `${clientId}/`;

    for (const rawKey of keys) {
      const key =
        rawKey.replace(/^generated-files\//, '');

      if (!key.startsWith(prefix)) {
        continue;
      }

      const relativeKey =
        key.slice(prefix.length);

      const parts =
        relativeKey
          .split('/')
          .filter(Boolean);

      if (parts.length < 2) {
        continue;
      }

      const runId = parts[0];

      const fileName =
        parts[parts.length - 1];

      if (
        !fileName.toLowerCase().endsWith('.bin')
      ) {
        continue;
      }

      let runNode =
        runs.get(runId);

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

    const runNodes =
      [...runs.values()]
        .sort((a, b) =>
          b.name.localeCompare(a.name)
        );

    for (const runNode of runNodes) {
      runNode.children =
        [...(runNode.children ?? [])]
          .sort((a, b) =>
            a.name.localeCompare(b.name)
          );
    }

    return runNodes;
  }

  private extractS3Keys(payload: unknown): string[] {
    if (Array.isArray(payload)) {
      return payload.filter(
        (item): item is string =>
          typeof item === 'string'
      );
    }

    if (
      !payload ||
      typeof payload !== 'object'
    ) {
      return [];
    }

    const data = payload as {
      keys?: unknown;
      files?: unknown;
      objects?: unknown;
    };

    const source =
      data.keys ??
      data.files ??
      data.objects;

    if (!Array.isArray(source)) {
      return [];
    }

    return source
      .map(item => {
        if (typeof item === 'string') {
          return item;
        }

        if (
          item &&
          typeof item === 'object' &&
          'key' in item
        ) {
          const key =
            (item as { key?: unknown }).key;

          return typeof key === 'string'
            ? key
            : '';
        }

        return '';
      })
      .filter(key => key.length > 0);
  }

  private async loadRuntimeConfig(): Promise<RuntimeConfig> {
    const response =
      await fetch(
        `assets/config.json?t=${Date.now()}`
      );

    if (!response.ok) {
      throw new Error(
        `Failed to load assets/config.json. HTTP ${response.status}`
      );
    }

    return await response.json() as RuntimeConfig;
  }

  private resolveClientId(
    config: RuntimeConfig
  ): string {
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