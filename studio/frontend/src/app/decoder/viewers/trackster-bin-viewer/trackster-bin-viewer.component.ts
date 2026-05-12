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

@Component({
  selector: 'app-trackster-bin-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule
  ],
  templateUrl: './trackster-bin-viewer.component.html',
  styleUrl: './trackster-bin-viewer.component.css'
})
export class TracksterBinViewerComponent
implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  decoderFilterText = '';

  isLoadingBlockPage = false;

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

  async ngOnChanges(
    changes: SimpleChanges
  ): Promise<void> {

    if (
      changes['selectedNode'] &&
      this.selectedNode
    ) {
      await this.loadTracksterBinForViewer(
        this.selectedNode
      );
    }
  }

  matchesDecoderFilter(value: string): boolean {

    const filter =
      this.decoderFilterText
        .trim()
        .toLowerCase();

    if (!filter) {
      return true;
    }

    return value
      .toLowerCase()
      .includes(filter);
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
      Math.ceil(
        this.totalBlockCount /
        this.blocksPerPage
      )
    );
  }

  getBlockPageStart(): number {

    if (this.totalBlockCount === 0) {
      return 0;
    }

    return (
      (
        (this.currentBlockPage - 1) *
        this.blocksPerPage
      ) + 1
    );
  }

  getBlockPageEnd(): number {

    return Math.min(
      this.currentBlockPage *
      this.blocksPerPage,
      this.totalBlockCount
    );
  }

  isFirstBlockPage(): boolean {
    return this.currentBlockPage <= 1;
  }

  isLastBlockPage(): boolean {

    return (
      this.currentBlockPage >=
      this.getTotalBlockPages()
    );
  }

  async loadPreviousBlockPage():
    Promise<void> {

    if (this.isFirstBlockPage()) {
      return;
    }

    await this.goToBlockPage(
      this.currentBlockPage - 1
    );
  }

  async loadNextBlockPage():
    Promise<void> {

    if (this.isLastBlockPage()) {
      return;
    }

    await this.goToBlockPage(
      this.currentBlockPage + 1
    );
  }

  async goToBlockPageFromInput():
    Promise<void> {

    await this.goToBlockPage(
      this.blockPageInput
    );
  }

  private async goToBlockPage(
    page: number
  ): Promise<void> {

    const safePage =
      this.normalizeBlockPage(page);

    this.currentBlockPage = safePage;

    this.blockPageInput = safePage;

    await this.refreshCurrentBlockPage();
  }

  private normalizeBlockPage(
    page: number
  ): number {

    const totalPages =
      this.getTotalBlockPages();

    if (!Number.isFinite(page)) {
      return this.currentBlockPage;
    }

    const integerPage =
      Math.trunc(page);

    return Math.min(
      Math.max(integerPage, 1),
      totalPages
    );
  }

  private async refreshCurrentBlockPage():
    Promise<void> {

    this.isLoadingBlockPage = true;

    try {

      const start =
        (
          this.currentBlockPage - 1
        ) * this.blocksPerPage;

      const end =
        start + this.blocksPerPage;

      this.payloadViewer.blocks =
        this.fullParsedBlocks.slice(
          start,
          end
        );

    } finally {
      this.isLoadingBlockPage = false;
    }
  }

  private async loadTracksterBinForViewer(
    node: S3TreeNode
  ): Promise<void> {

    try {

      this.isLoadingBlockPage = true;

      this.currentBlockPage = 1;

      this.blockPageInput = 1;

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

      this.totalBlockCount =
        parsed.blockCount;

      const firstTimestampNs =
        parsed.blocks[0]?.timestampNs ?? '0';

      const secondTimestampNs =
        parsed.blocks[1]?.timestampNs ??
        firstTimestampNs;

      const lastBlock =
        parsed.blocks[
          parsed.blocks.length - 1
        ];

      const lastTimestampNs =
        lastBlock?.timestampNs ??
        firstTimestampNs;

      this.payloadViewer = {

        fileName: node.name,

        summary: [
          {
            label: 'Blocks',
            value:
              parsed.blockCount
                .toLocaleString()
          },
          {
            label: 'Frames',
            value:
              parsed.totalFrameCount
                .toLocaleString()
          },
          {
            label: 'Interval',
            value:
              this.formatBlockDuration(
                firstTimestampNs,
                secondTimestampNs
              )
          },
          {
            label: 'Duration',
            value:
              this.formatBlockDuration(
                firstTimestampNs,
                lastTimestampNs
              )
          },
          {
            label: 'Size',
            value:
              this.formatBytes(
                parsed.totalBytes
              )
          }
        ],

        headerFields: [
          {
            label: 'Magic',
            value: parsed.magic
          },
          {
            label: 'Version',
            value:
              `${parsed.versionMajor}.${parsed.versionMinor}`
          },
          {
            label: 'Header bytes',
            value: parsed.headerBytes
          },
          {
            label: 'Block header',
            value:
              parsed.blockHeaderBytes
          },
          {
            label: 'Frame header',
            value:
              parsed.frameFixedHeaderBytes
          },
          {
            label: 'Blocks',
            value: parsed.blockCount
          },
          {
            label: 'Frames',
            value:
              parsed.totalFrameCount
          },
          {
            label: 'Payload bytes',
            value:
              parsed.totalPayloadBytes
                .toLocaleString()
          },
          {
            label: 'File bytes',
            value:
              parsed.totalBytes
                .toLocaleString()
          }
        ],

        blocks: []
      };

      this.fullParsedBlocks =
        parsed.blocks.map(
          (
            block: any
          ) => {

            const startNs =
              BigInt(
                block.timestampNs
              );

            const nextBlock =
              parsed.blocks[
                block.blockIndex + 1
              ];

            const endNs =
              nextBlock
                ? BigInt(
                    nextBlock.timestampNs
                  )
                : startNs;

            return {

              index:
                block.blockIndex,

              expanded:
                block.blockIndex === 0,

              startNs:
                this.formatRelativeTimeNs(
                  firstTimestampNs,
                  startNs.toString()
                ),

              endNs:
                this.formatRelativeTimeNs(
                  firstTimestampNs,
                  endNs.toString()
                ),

              duration:
                this.formatBlockDuration(
                  startNs.toString(),
                  endNs.toString()
                ),

              frameCount:
                block.frameCount,

              frames:
                block.frames.map(
                  (
                    frame: any
                  ) => {

                    const signals =
                      Array.isArray(
                        frame.signals
                      )
                        ? frame.signals.map(
                            (
                              signal: any
                            ) => ({
                              name:
                                signal.name,

                              value:
                                signal.value,

                              raw:
                                signal.raw,

                              unit:
                                signal.unit,

                              searchText: [
                                frame.canIdHex,
                                frame.messageName,
                                signal.name
                              ].join(' ')
                            })
                          )
                        : [];

                    return {

                      expanded: false,

                      searchText: [
                        frame.canIdHex,
                        frame.messageName,
                        frame.payloadBytes,
                        ...signals.map(
                          (
                            signal: any
                          ) => signal.name
                        )
                      ].join(' '),

                      canId:
                        frame.canIdHex,

                      messageName:
                        frame.messageName ||
                        `CAN_${frame.canIdHex}`,

                      time:
                        `${frame.timestampDeltaNs} ns`,

                      dlc:
                        frame.payloadLength,

                      decodedSignals:
                        Number(
                          frame.decodedSignals ??
                          signals.length
                        ),

                      payloadHex:
                        frame.payloadBytes,

                      signals
                    };
                  }
                )
            };
          }
        );

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

  private formatRelativeTimeNs(
    baseNs: string,
    valueNs: string
  ): string {

    const diffNs =
      BigInt(valueNs) -
      BigInt(baseNs);

    const seconds =
      Number(diffNs) /
      1_000_000_000;

    return `${seconds.toFixed(3)} s`;
  }

  private formatBlockDuration(
    startNs: string,
    endNs: string
  ): string {

    const diffNs =
      Number(
        BigInt(endNs) -
        BigInt(startNs)
      );

    const seconds =
      diffNs / 1_000_000_000;

    return `${seconds.toFixed(2)} s`;
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

    const key = node.key;

    if (!bucket) {

      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    return await this.getS3ObjectBuffer(
      bucket,
      key
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
      this.getRunIdFromKey(
        node.key
      );

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
        `Failed config.json`
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

  async copyHeaderToClipboard():
    Promise<void> {

    const rows = [
      ['Field', 'Value'],

      ...this.payloadViewer.headerFields.map(
        (field: any) => [
          field.label,
          field.value
        ]
      )
    ];

    await this.copyRowsToClipboard(rows);
  }

  async copyBlocksToClipboard():
    Promise<void> {

    const rows = [
      [
        'Block',
        'Start',
        'End',
        'Duration',
        'Frames'
      ],

      ...this.payloadViewer.blocks.map(
        (block: any) => [
          block.index,
          block.startNs,
          block.endNs,
          block.duration,
          block.frameCount
        ]
      )
    ];

    await this.copyRowsToClipboard(rows);
  }

  async copyPayloadViewerToClipboard():
    Promise<void> {

    const rows = [
      [
        'Block',
        'CAN ID',
        'Message',
        'Time',
        'DLC',
        'Payload'
      ],

      ...this.payloadViewer.blocks.flatMap(
        (block: any) =>
          block.frames.map(
            (frame: any) => [
              block.index,
              frame.canId,
              frame.messageName,
              frame.time,
              frame.dlc,
              frame.payloadHex
            ]
          )
      )
    ];

    await this.copyRowsToClipboard(rows);
  }

  private async copyRowsToClipboard(
    rows: Array<
      Array<string | number>
    >
  ): Promise<void> {

    const text =
      rows
        .map(row =>
          row.join('\t')
        )
        .join('\n');

    await navigator.clipboard
      .writeText(text);
  }
}