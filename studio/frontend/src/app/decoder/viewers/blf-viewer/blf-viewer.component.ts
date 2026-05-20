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
  s3BlfBucket?: string;
  s3Region?: string;
  clientId?: string;
  customerId?: string;

  decoderApi?: {
    blfExportUrl?: string;
  };
}

interface BlfMessageRow {
  index: number;
  objectType: string;
  timestamp: string;
  channel: number;
  canId: string;
  dlc: number;
  payload: string;
  flags: string;
  searchText: string;
}

interface BlfObjectRow {
  index: number;
  offset: number;
  type: string;
  size: number;
  headerSize: number;
  objectVersion: number;
}

interface ParsedBlfStream {
  objects: BlfObjectRow[];
  messages: BlfMessageRow[];
  canMessageCount: number;
  canFdMessageCount: number;
  logContainerCount: number;
  unknownObjectCount: number;
}

interface BlfContainerChunk {
  object: BlfObjectRow;
  buffer: ArrayBuffer;
}

@Component({
  selector: 'app-blf-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule
  ],
  templateUrl: './blf-viewer.component.html',
  styleUrl: './blf-viewer.component.css'
})
export class BlfViewerComponent implements OnChanges {

  @Input()
  selectedNode!: S3TreeNode;

  isLoading = false;

  loadError = '';

  filterText = '';

  currentMessagePage = 1;

  messagePageInput = 1;

  readonly messagesPerPage = 100;

  blfViewer = {
    fileName: '',
    size: '',
    signature: '',
    headerSize: 0,
    appId: 0,
    appMajor: 0,
    appMinor: 0,
    objectCountDeclared: 0,
    objectCountParsed: 0,
    messageCount: 0,
    canMessageCount: 0,
    canFdMessageCount: 0,
    unknownObjectCount: 0,
    startTime: '',
    stopTime: '',
    summary: [] as any[],
    headerFields: [] as any[],
    messages: [] as BlfMessageRow[],
    objects: [] as BlfObjectRow[]
  };

  private fullMessages: BlfMessageRow[] = [];

  async ngOnChanges(
    changes: SimpleChanges
  ): Promise<void> {

    if (
      changes['selectedNode'] &&
      this.selectedNode
    ) {
      await this.loadBlfForViewer();
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
    return this.fullMessages.length;
  }

  getTotalMessagePages(): number {

    return Math.max(
      1,
      Math.ceil(
        this.getTotalMessageCount() /
        this.messagesPerPage
      )
    );
  }

  getMessagePageStart(): number {

    if (
      this.getTotalMessageCount() === 0
    ) {
      return 0;
    }

    return (
      (
        (this.currentMessagePage - 1) *
        this.messagesPerPage
      ) + 1
    );
  }

  getMessagePageEnd(): number {

    return Math.min(
      this.currentMessagePage *
      this.messagesPerPage,
      this.getTotalMessageCount()
    );
  }

  isFirstMessagePage(): boolean {
    return this.currentMessagePage <= 1;
  }

  isLastMessagePage(): boolean {

    return (
      this.currentMessagePage >=
      this.getTotalMessagePages()
    );
  }

  async loadPreviousMessagePage():
    Promise<void> {

    if (this.isFirstMessagePage()) {
      return;
    }

    await this.goToMessagePage(
      this.currentMessagePage - 1
    );
  }

  async loadNextMessagePage():
    Promise<void> {

    if (this.isLastMessagePage()) {
      return;
    }

    await this.goToMessagePage(
      this.currentMessagePage + 1
    );
  }

  async goToMessagePageFromInput():
    Promise<void> {

    await this.goToMessagePage(
      this.messagePageInput
    );
  }

  async copyVisibleMessagesToClipboard():
    Promise<void> {

    const rows = [
      [
        'Index',
        'Object Type',
        'Timestamp',
        'Channel',
        'CAN ID',
        'DLC',
        'Payload',
        'Flags'
      ],

      ...this.blfViewer.messages.map(
        message => [
          message.index,
          message.objectType,
          message.timestamp,
          message.channel,
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

      ...this.blfViewer.headerFields.map(
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

  private async loadBlfForViewer():
    Promise<void> {

    this.isLoading = true;

    this.loadError = '';

    this.currentMessagePage = 1;

    this.messagePageInput = 1;

    try {

      let buffer: ArrayBuffer;

      try {

        buffer =
          await this.loadBlfBuffer(
            this.selectedNode
          );

      } catch (error: any) {

        if (
          this.shouldUseLocalMock() ||
          !this.isMissingS3ObjectError(error)
        ) {
          throw error;
        }

        await this.generateBlfFile(
          this.selectedNode
        );

        buffer =
          await this.loadBlfBuffer(
            this.selectedNode
          );
      }

      await this.parseBlf(
        buffer,
        this.selectedNode.name
      );

      await this.refreshCurrentMessagePage();

    } catch (error: any) {

      console.error(
        'Failed to load BLF viewer',
        error
      );

      this.loadError =
        error?.message ||
        'Failed to load BLF file.';

      this.resetViewer();

    } finally {

      this.isLoading = false;
    }
  }

  private async generateBlfFile(
    node: S3TreeNode
  ): Promise<void> {

    const config =
      await this.loadRuntimeConfig();

    const apiUrl =
      config.decoderApi?.blfExportUrl?.trim();

    if (!apiUrl) {
      throw new Error(
        'Missing decoderApi.blfExportUrl in assets/config.json'
      );
    }

    const inputBucketName =
      config.s3Default?.trim();

    const outputBucketName =
      config.s3BlfBucket?.trim();

    if (!inputBucketName) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    if (!outputBucketName) {
      throw new Error(
        'Missing s3BlfBucket in assets/config.json'
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
            inputKey: node.key
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
        `BLF generation failed. HTTP ${response.status}`
      );
    }
  }

  private async parseBlf(
    buffer: ArrayBuffer,
    sourceFileName: string
  ): Promise<void> {

    const view =
      new DataView(buffer);

    if (buffer.byteLength < 144) {

      throw new Error(
        'Invalid BLF file. File is too small.'
      );
    }

    const signature =
      this.readAscii(
        view,
        0,
        4
      );

    if (signature !== 'LOGG') {

      throw new Error(
        `Invalid BLF signature: ${signature}`
      );
    }

    const headerSize =
      view.getUint32(
        4,
        true
      );

    const appId =
      view.getUint8(8);

    const appMajor =
      view.getUint8(10);

    const appMinor =
      view.getUint8(11);

    const fileSizeDeclared =
      this.readUint64AsNumber(
        view,
        16
      );

    const uncompressedSizeDeclared =
      this.readUint64AsNumber(
        view,
        24
      );

    const objectCountDeclared =
      view.getUint32(
        32,
        true
      );

    const startTime =
      this.readSystemTime(
        view,
        40
      );

    const stopTime =
      this.readSystemTime(
        view,
        72
      );

    const topLevelOffset =
      headerSize >= 144
        ? headerSize
        : 144;

    const containerChunks =
      await this.extractContainerChunks(
        buffer,
        topLevelOffset
      );

    const uncompressedStream =
      this.concatArrayBuffers(
        containerChunks.map(
          chunk => chunk.buffer
        )
      );

    const parsedStream =
      this.parseFlatObjectStream(
        uncompressedStream,
        0,
        0
      );

    const topLevelObjects =
      containerChunks.map(
        chunk => chunk.object
      );

    const allObjects = [
      ...topLevelObjects,
      ...parsedStream.objects
    ];

    this.fullMessages =
      parsedStream.messages;

    const outputFileName =
      this.buildBlfFileName(
        sourceFileName
      );

    this.blfViewer = {
      fileName:
        outputFileName,

      size:
        this.formatBytes(
          buffer.byteLength
        ),

      signature,

      headerSize,

      appId,

      appMajor,

      appMinor,

      objectCountDeclared,

      objectCountParsed:
        allObjects.length,

      messageCount:
        parsedStream.messages.length,

      canMessageCount:
        parsedStream.canMessageCount,

      canFdMessageCount:
        parsedStream.canFdMessageCount,

      unknownObjectCount:
        parsedStream.unknownObjectCount,

      startTime,

      stopTime,

      summary: [
        {
          label: 'Objects',
          value:
            allObjects.length
              .toLocaleString()
        },
        {
          label: 'Messages',
          value:
            parsedStream.messages.length
              .toLocaleString()
        },
        {
          label: 'CAN',
          value:
            parsedStream.canMessageCount
              .toLocaleString()
        },
        {
          label: 'Containers',
          value:
            containerChunks.length
              .toLocaleString()
        },
        {
          label: 'Size',
          value:
            this.formatBytes(
              buffer.byteLength
            )
        }
      ],

      headerFields: [
        {
          label: 'Signature',
          value: signature
        },
        {
          label: 'Header bytes',
          value: headerSize
        },
        {
          label: 'Application ID',
          value: appId
        },
        {
          label: 'Application version',
          value:
            `${appMajor}.${appMinor}`
        },
        {
          label: 'File size',
          value:
            this.formatBytes(
              fileSizeDeclared ||
              buffer.byteLength
            )
        },
        {
          label: 'Uncompressed size',
          value:
            this.formatBytes(
              uncompressedSizeDeclared
            )
        },
        {
          label: 'Declared objects',
          value:
            objectCountDeclared
              .toLocaleString()
        },
        {
          label: 'Parsed objects',
          value:
            allObjects.length
              .toLocaleString()
        },
        {
          label: 'Start time',
          value: startTime
        },
        {
          label: 'Stop time',
          value: stopTime
        }
      ],

      messages: [],

      objects:
        allObjects
    };
  }

  private async extractContainerChunks(
    buffer: ArrayBuffer,
    startOffset: number
  ): Promise<BlfContainerChunk[]> {

    const view =
      new DataView(buffer);

    const chunks:
      BlfContainerChunk[] = [];

    let offset =
      startOffset;

    let objectIndex = 0;

    while (
      offset + 16 <=
      buffer.byteLength
    ) {

      const objectSignature =
        this.readAscii(
          view,
          offset,
          4
        );

      if (
        objectSignature !== 'LOBJ'
      ) {
        const nextOffset =
          this.findNextObjectOffset(
            view,
            offset,
            Math.min(
              offset + 16,
              buffer.byteLength
            )
          );

        if (nextOffset < 0) {
          break;
        }

        offset =
          nextOffset;

        continue;
      }

      const headerSizeObject =
        view.getUint16(
          offset + 4,
          true
        );

      const headerVersion =
        view.getUint16(
          offset + 6,
          true
        );

      const objectSize =
        view.getUint32(
          offset + 8,
          true
        );

      const objectType =
        view.getUint32(
          offset + 12,
          true
        );

      if (
        objectSize <= 0 ||
        headerSizeObject < 16 ||
        offset + objectSize >
        buffer.byteLength
      ) {
        break;
      }

      const typeName =
        this.getObjectTypeName(
          objectType
        );

      const objectRow: BlfObjectRow = {
        index:
          objectIndex,

        offset,

        type:
          typeName,

        size:
          objectSize,

        headerSize:
          headerSizeObject,

        objectVersion:
          headerVersion
      };

      if (objectType === 10) {

        const containerBuffer =
          await this.inflateLogContainer(
            view,
            offset,
            headerSizeObject,
            objectSize
          );

        chunks.push({
          object:
            objectRow,

          buffer:
            containerBuffer
        });
      }

      objectIndex++;

      const nextOffset =
        this.findNextObjectOffset(
          view,
          offset + objectSize,
          Math.min(
            offset + objectSize + 16,
            buffer.byteLength
          )
        );

      if (nextOffset < 0) {
        break;
      }

      offset =
        nextOffset;
    }

    return chunks;
  }

  private parseFlatObjectStream(
    buffer: ArrayBuffer,
    startOffset: number,
    baseOffset: number
  ): ParsedBlfStream {

    const view =
      new DataView(buffer);

    const objects:
      BlfObjectRow[] = [];

    const messages:
      BlfMessageRow[] = [];

    let canMessageCount = 0;

    let canFdMessageCount = 0;

    let logContainerCount = 0;

    let unknownObjectCount = 0;

    let offset =
      startOffset;

    while (
      offset + 16 <=
      buffer.byteLength
    ) {

      const objectSignature =
        this.readAscii(
          view,
          offset,
          4
        );

      if (
        objectSignature !== 'LOBJ'
      ) {
        break;
      }

      const headerSizeObject =
        view.getUint16(
          offset + 4,
          true
        );

      const headerVersion =
        view.getUint16(
          offset + 6,
          true
        );

      const objectSize =
        view.getUint32(
          offset + 8,
          true
        );

      const objectType =
        view.getUint32(
          offset + 12,
          true
        );

      if (
        objectSize <= 0 ||
        offset + objectSize >
        buffer.byteLength ||
        headerSizeObject < 16
      ) {
        break;
      }

      const objectIndex =
        objects.length;

      const typeName =
        this.getObjectTypeName(
          objectType
        );

      objects.push({
        index:
          objectIndex,

        offset:
          baseOffset + offset,

        type:
          typeName,

        size:
          objectSize,

        headerSize:
          headerSizeObject,

        objectVersion:
          headerVersion
      });

      if (objectType === 1) {

        const message =
          this.parseCanMessage(
            view,
            offset,
            headerSizeObject,
            messages.length
          );

        if (message) {

          messages.push(message);

          canMessageCount++;
        }

      } else if (objectType === 100) {

        const message =
          this.parseCanFdMessage(
            view,
            offset,
            headerSizeObject,
            messages.length
          );

        if (message) {

          messages.push(message);

          canFdMessageCount++;
        }

      } else if (objectType === 10) {

        logContainerCount++;

      } else {

        unknownObjectCount++;
      }

      offset +=
        objectSize;
    }

    return {
      objects,
      messages,
      canMessageCount,
      canFdMessageCount,
      logContainerCount,
      unknownObjectCount
    };
  }

  private async inflateLogContainer(
    view: DataView,
    objectOffset: number,
    headerSize: number,
    objectSize: number
  ): Promise<ArrayBuffer> {

    const dataOffset =
      objectOffset +
      headerSize;

    const dataSize =
      objectSize -
      headerSize;

    if (
      dataSize <= 16 ||
      dataOffset + dataSize >
      view.byteLength
    ) {
      return new ArrayBuffer(0);
    }

    const compressionMethod =
      view.getUint16(
        dataOffset,
        true
      );

    const compressedOffset =
      dataOffset + 16;

    const compressedLength =
      dataSize - 16;

    const compressedBytes =
      new Uint8Array(
        view.buffer,
        view.byteOffset + compressedOffset,
        compressedLength
      );

    if (compressionMethod === 0) {

      return this.copyUint8ArrayToArrayBuffer(
        compressedBytes
      );
    }

    if (compressionMethod !== 2) {

      throw new Error(
        `Unsupported BLF container compression method: ${compressionMethod}`
      );
    }

    return await this.inflateDeflate(
      compressedBytes
    );
  }

  private async inflateDeflate(
    compressedBytes: Uint8Array
  ): Promise<ArrayBuffer> {

    if (
      typeof DecompressionStream ===
      'undefined'
    ) {

      throw new Error(
        'This browser does not support BLF zlib decompression.'
      );
    }

    const compressedBuffer =
      this.copyUint8ArrayToArrayBuffer(
        compressedBytes
      );

    const stream =
      new Blob([
        compressedBuffer
      ])
        .stream()
        .pipeThrough(
          new DecompressionStream(
            'deflate'
          )
        );

    const response =
      new Response(stream);

    return await response.arrayBuffer();
  }

  private findNextObjectOffset(
    view: DataView,
    startOffset: number,
    endOffset: number
  ): number {

    const safeEnd =
      Math.min(
        endOffset,
        view.byteLength - 4
      );

    for (
      let offset = startOffset;
      offset <= safeEnd;
      offset++
    ) {

      if (
        this.readAscii(
          view,
          offset,
          4
        ) === 'LOBJ'
      ) {
        return offset;
      }
    }

    return -1;
  }

  private concatArrayBuffers(
    buffers: ArrayBuffer[]
  ): ArrayBuffer {

    const totalLength =
      buffers.reduce(
        (
          total,
          buffer
        ) => total + buffer.byteLength,
        0
      );

    const output =
      new Uint8Array(
        totalLength
      );

    let offset = 0;

    for (
      const buffer of buffers
    ) {

      output.set(
        new Uint8Array(buffer),
        offset
      );

      offset +=
        buffer.byteLength;
    }

    return output.buffer;
  }

  private copyUint8ArrayToArrayBuffer(
    bytes: Uint8Array
  ): ArrayBuffer {

    const buffer =
      new ArrayBuffer(
        bytes.byteLength
      );

    const target =
      new Uint8Array(
        buffer
      );

    target.set(bytes);

    return buffer;
  }

  private parseCanMessage(
    view: DataView,
    objectOffset: number,
    headerSize: number,
    index: number
  ): BlfMessageRow | null {

    const dataOffset =
      objectOffset +
      headerSize;

    if (
      dataOffset + 16 >
      view.byteLength
    ) {
      return null;
    }

    const channel =
      view.getUint16(
        dataOffset,
        true
      );

    const flags =
      view.getUint8(
        dataOffset + 2
      );

    const dlc =
      view.getUint8(
        dataOffset + 3
      );

    const canIdRaw =
      view.getUint32(
        dataOffset + 4,
        true
      );

    const payload =
      this.readBytesAsHex(
        view,
        dataOffset + 8,
        Math.min(
          dlc,
          8
        )
      );

    const timestamp =
      this.readObjectTimestamp(
        view,
        objectOffset,
        headerSize
      );

    const isExtended =
      (
        canIdRaw &
        0x80000000
      ) !== 0;

    const canId =
      canIdRaw &
      0x1fffffff;

    return {
      index,

      objectType:
        'CAN',

      timestamp,

      channel,

      canId:
        this.formatCanId(
          canId,
          isExtended
        ),

      dlc,

      payload,

      flags:
        this.formatCanFlags(
          flags,
          isExtended,
          false
        ),

      searchText: [
        'CAN',
        timestamp,
        channel,
        this.formatCanId(
          canId,
          isExtended
        ),
        payload
      ].join(' ')
    };
  }

  private parseCanFdMessage(
    view: DataView,
    objectOffset: number,
    headerSize: number,
    index: number
  ): BlfMessageRow | null {

    const dataOffset =
      objectOffset +
      headerSize;

    if (
      dataOffset + 20 >
      view.byteLength
    ) {
      return null;
    }

    const channel =
      view.getUint16(
        dataOffset,
        true
      );

    const flags =
      view.getUint8(
        dataOffset + 2
      );

    const dlc =
      view.getUint8(
        dataOffset + 3
      );

    const canIdRaw =
      view.getUint32(
        dataOffset + 4,
        true
      );

    const payloadLength =
      this.canFdDlcToLength(
        dlc
      );

    const payloadOffset =
      dataOffset + 20;

    const payload =
      this.readBytesAsHex(
        view,
        payloadOffset,
        Math.min(
          payloadLength,
          64
        )
      );

    const timestamp =
      this.readObjectTimestamp(
        view,
        objectOffset,
        headerSize
      );

    const isExtended =
      (
        canIdRaw &
        0x80000000
      ) !== 0;

    const canId =
      canIdRaw &
      0x1fffffff;

    return {
      index,

      objectType:
        'CAN FD',

      timestamp,

      channel,

      canId:
        this.formatCanId(
          canId,
          isExtended
        ),

      dlc,

      payload,

      flags:
        this.formatCanFlags(
          flags,
          isExtended,
          true
        ),

      searchText: [
        'CAN FD',
        timestamp,
        channel,
        this.formatCanId(
          canId,
          isExtended
        ),
        payload
      ].join(' ')
    };
  }

  private readObjectTimestamp(
    view: DataView,
    objectOffset: number,
    headerSize: number
  ): string {

    if (
      headerSize >= 32 &&
      objectOffset + 32 <=
      view.byteLength
    ) {

      const timestamp =
        this.readUint64AsNumber(
          view,
          objectOffset + 24
        );

      return `${(
        timestamp /
        1_000_000_000
      ).toFixed(6)} s`;
    }

    return '-';
  }

  private readSystemTime(
    view: DataView,
    offset: number
  ): string {

    if (
      offset + 16 >
      view.byteLength
    ) {
      return '-';
    }

    const year =
      view.getUint16(
        offset,
        true
      );

    const month =
      view.getUint16(
        offset + 2,
        true
      );

    const dayOfWeek =
      view.getUint16(
        offset + 4,
        true
      );

    const day =
      view.getUint16(
        offset + 6,
        true
      );

    const hour =
      view.getUint16(
        offset + 8,
        true
      );

    const minute =
      view.getUint16(
        offset + 10,
        true
      );

    const second =
      view.getUint16(
        offset + 12,
        true
      );

    const milliseconds =
      view.getUint16(
        offset + 14,
        true
      );

    if (
      !year ||
      !month ||
      !day
    ) {
      return '-';
    }

    return [
      `${year}-${this.pad2(month)}-${this.pad2(day)}`,
      `${this.pad2(hour)}:${this.pad2(minute)}:${this.pad2(second)}.${milliseconds.toString().padStart(3, '0')}`,
      `(dow ${dayOfWeek})`
    ].join(' ');
  }

  private async refreshCurrentMessagePage():
    Promise<void> {

    const start =
      (
        this.currentMessagePage - 1
      ) *
      this.messagesPerPage;

    const end =
      start +
      this.messagesPerPage;

    this.blfViewer.messages =
      this.fullMessages.slice(
        start,
        end
      );
  }

  private async goToMessagePage(
    page: number
  ): Promise<void> {

    const safePage =
      this.normalizeMessagePage(
        page
      );

    this.currentMessagePage =
      safePage;

    this.messagePageInput =
      safePage;

    await this.refreshCurrentMessagePage();
  }

  private normalizeMessagePage(
    page: number
  ): number {

    if (
      !Number.isFinite(page)
    ) {
      return this.currentMessagePage;
    }

    return Math.min(
      Math.max(
        Math.trunc(page),
        1
      ),
      this.getTotalMessagePages()
    );
  }

  private getObjectTypeName(
    objectType: number
  ): string {

    if (objectType === 1) {
      return 'CAN_MESSAGE';
    }

    if (objectType === 100) {
      return 'CAN_FD_MESSAGE';
    }

    if (objectType === 10) {
      return 'LOG_CONTAINER';
    }

    return `OBJECT_${objectType}`;
  }

  private formatCanId(
    canId: number,
    isExtended: boolean
  ): string {

    const width =
      isExtended
        ? 8
        : 3;

    return `0x${canId.toString(16).toUpperCase().padStart(width, '0')}`;
  }

  private formatCanFlags(
    flags: number,
    isExtended: boolean,
    isFd: boolean
  ): string {

    const values: string[] = [];

    if (isExtended) {
      values.push('EXT');
    }

    if (isFd) {
      values.push('FD');
    }

    if (flags) {
      values.push(
        `0x${flags.toString(16).toUpperCase()}`
      );
    }

    return values.length
      ? values.join(' · ')
      : '-';
  }

  private canFdDlcToLength(
    dlc: number
  ): number {

    if (dlc <= 8) {
      return dlc;
    }

    const map:
      Record<number, number> = {
        9: 12,
        10: 16,
        11: 20,
        12: 24,
        13: 32,
        14: 48,
        15: 64
      };

    return map[dlc] ?? 0;
  }

  private readAscii(
    view: DataView,
    offset: number,
    length: number
  ): string {

    let value = '';

    for (
      let index = 0;
      index < length;
      index++
    ) {

      value +=
        String.fromCharCode(
          view.getUint8(
            offset + index
          )
        );
    }

    return value;
  }

  private readBytesAsHex(
    view: DataView,
    offset: number,
    length: number
  ): string {

    const bytes: string[] = [];

    for (
      let index = 0;
      index < length;
      index++
    ) {

      if (
        offset + index >=
        view.byteLength
      ) {
        break;
      }

      bytes.push(
        view
          .getUint8(
            offset + index
          )
          .toString(16)
          .toUpperCase()
          .padStart(2, '0')
      );
    }

    return bytes.join(' ');
  }

  private readUint64AsNumber(
    view: DataView,
    offset: number
  ): number {

    if (
      offset + 8 >
      view.byteLength
    ) {
      return 0;
    }

    const low =
      view.getUint32(
        offset,
        true
      );

    const high =
      view.getUint32(
        offset + 4,
        true
      );

    return (
      high *
      4294967296 +
      low
    );
  }

  private pad2(
    value: number
  ): string {

    return value
      .toString()
      .padStart(2, '0');
  }

  private resetViewer():
    void {

    this.fullMessages = [];

    this.blfViewer = {
      fileName: '',
      size: '',
      signature: '',
      headerSize: 0,
      appId: 0,
      appMajor: 0,
      appMinor: 0,
      objectCountDeclared: 0,
      objectCountParsed: 0,
      messageCount: 0,
      canMessageCount: 0,
      canFdMessageCount: 0,
      unknownObjectCount: 0,
      startTime: '',
      stopTime: '',
      summary: [],
      headerFields: [],
      messages: [],
      objects: []
    };
  }

  private async loadBlfBuffer(
    node: S3TreeNode
  ): Promise<ArrayBuffer> {

    if (
      this.shouldUseLocalMock()
    ) {

      const response =
        await fetch(
          '/assets/mock/sample.blf'
        );

      if (!response.ok) {

        throw new Error(
          `Local BLF mock not found. HTTP ${response.status}`
        );
      }

      return await response.arrayBuffer();
    }

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3BlfBucket?.trim();

    if (!bucket) {

      throw new Error(
        'Missing s3BlfBucket in assets/config.json'
      );
    }

    return await this.getS3ObjectBuffer(
      bucket,
      this.buildBlfKey(
        node.key
      )
    );
  }

  private buildBlfKey(
    key: string
  ): string {

    if (
      !key
        .toLowerCase()
        .endsWith('.bin')
    ) {
      return key;
    }

    return `${key.slice(0, -4)}.blf`;
  }

  private buildBlfFileName(
    fileName: string
  ): string {

    if (
      !fileName
        .toLowerCase()
        .endsWith('.bin')
    ) {
      return fileName;
    }

    return `${fileName.slice(0, -4)}.blf`;
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

  private isMissingS3ObjectError(
    error: any
  ): boolean {

    const name =
      String(
        error?.name ||
        ''
      );

    const code =
      String(
        error?.Code ||
        error?.code ||
        ''
      );

    const message =
      String(
        error?.message ||
        ''
      );

    const status =
      Number(
        error?.$metadata?.httpStatusCode ||
        error?.statusCode ||
        error?.status ||
        0
      );

    return (
      status === 404 ||
      name === 'NoSuchKey' ||
      code === 'NoSuchKey' ||
      message.includes('NoSuchKey') ||
      message.includes('The specified key does not exist')
    );
  }

  private formatBytes(
    bytes: number
  ): string {

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
    rows: Array<
      Array<string | number>
    >
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