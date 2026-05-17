import {
  Component,
  Input,
  OnChanges,
  SimpleChanges,
  OnDestroy
} from '@angular/core';

import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { NgxMonacoEditorComponent } from '@jean-merelis/ngx-monaco-editor';

import * as monaco from 'monaco-editor';

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

interface MonacoEditorInitializedEvent {
  editor?: monaco.editor.IStandaloneCodeEditor;
  monaco?: typeof monaco;
}

@Component({
  selector: 'app-json-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NgxMonacoEditorComponent
  ],
  templateUrl: './json-viewer.component.html',
  styleUrl: './json-viewer.component.css'
})
export class JsonViewerComponent
implements OnChanges, OnDestroy {

  @Input()
  selectedNode!: S3TreeNode;

  jsonViewMode: 'pretty' | 'raw' = 'pretty';

  jsonSearchText = '';

  isLoadingJson = false;

  jsonErrorMessage = '';

  rawJsonText = '';

  prettyJsonText = '';

  readonly maxRenderedBlocks = 50;

  searchMatchCount = 0;

  private currentSearchIndex = -1;

  private lastSearchText = '';

  private editor?:
    monaco.editor.IStandaloneCodeEditor;

  private searchDecorations?:
    monaco.editor.IEditorDecorationsCollection;

  private searchTimer:
    ReturnType<typeof setTimeout> | null = null;

  editorOptions:
    monaco.editor.IStandaloneEditorConstructionOptions = {

    find: {
      addExtraSpaceOnTop: false,
      autoFindInSelection: 'never',
      seedSearchStringFromSelection: 'never'
    },

    automaticLayout: true,
    readOnly: true,
    language: 'json',
    theme: 'hc-light',

    minimap: {
      enabled: false
    },

    fontSize: 12,
    lineHeight: 20,
    lineNumbers: 'on',
    lineNumbersMinChars: 6,
    glyphMargin: false,
    folding: true,
    lineDecorationsWidth: 0,
    roundedSelection: false,
    scrollBeyondLastLine: false,
    wordWrap: 'off',
    tabSize: 2,
    insertSpaces: true,
    overviewRulerBorder: false,
    overviewRulerLanes: 0,
    hideCursorInOverviewRuler: true,
    renderLineHighlight: 'none',
    renderValidationDecorations: 'off',
    occurrencesHighlight: 'off',
    selectionHighlight: false,
    matchBrackets: 'never',
    fixedOverflowWidgets: true,
    contextmenu: false,
    quickSuggestions: false,
    suggestOnTriggerCharacters: false,
    acceptSuggestionOnEnter: 'off',

    hover: {
      enabled: false
    },

    guides: {
      indentation: false,
      highlightActiveIndentation: false,
      bracketPairs: false
    },

    scrollbar: {
      vertical: 'auto',
      horizontal: 'auto',
      verticalScrollbarSize: 8,
      horizontalScrollbarSize: 8,
      alwaysConsumeMouseWheel: false,
      useShadows: false
    },

    padding: {
      top: 10,
      bottom: 10
    }
  };

  jsonViewer = {
    summary: [] as Array<{
      label: string;
      value: string;
    }>
  };

  get searchMatchLabel(): string {

    if (!this.jsonSearchText.trim()) {
      return '0 / 0';
    }

    if (this.searchMatchCount === 0) {
      return '0 / 0';
    }

    return `${this.currentSearchIndex + 1} / ${this.searchMatchCount}`;
  }

  ngOnDestroy(): void {

    if (this.searchTimer) {
      clearTimeout(this.searchTimer);
    }

    this.searchDecorations?.clear();
  }

  async ngOnChanges(
    changes: SimpleChanges
  ): Promise<void> {

    if (
      changes['selectedNode'] &&
      this.selectedNode
    ) {
      await this.loadBinAsJson(
        this.selectedNode
      );
    }
  }

  onEditorInitialized(
    event: unknown
  ): void {

    const payload =
      event as MonacoEditorInitializedEvent;

    const editor =
      payload.editor;

    if (!editor) {
      return;
    }

    this.editor = editor;

    this.searchDecorations =
      editor.createDecorationsCollection();

    this.syncEditorValue();

    setTimeout(() => {
      this.editor?.layout();
      this.applyEditorSearch(true);
    }, 0);
  }

  get displayJsonText(): string {

    if (this.jsonViewMode === 'raw') {
      return this.rawJsonText;
    }

    return (
      this.prettyJsonText ||
      this.rawJsonText
    );
  }

  setJsonViewMode(
    mode: 'pretty' | 'raw'
  ): void {

    this.jsonViewMode = mode;

    this.currentSearchIndex = -1;
    this.lastSearchText = '';

    setTimeout(() => {
      this.syncEditorValue();
      this.editor?.layout();
      this.applyEditorSearch(true);
    }, 0);
  }

  async copyFullJsonToClipboard():
    Promise<void> {

    await navigator.clipboard.writeText(
      this.displayJsonText
    );
  }

  applyEditorSearch(
    resetIndex = false
  ): void {

    if (this.searchTimer) {
      clearTimeout(this.searchTimer);
    }

    this.searchTimer =
      setTimeout(() => {
        this.executeEditorSearch(
          resetIndex
        );
      }, 40);
  }

  searchNextOccurrence(): void {

    this.executeEditorSearch(false);
  }

  searchPreviousOccurrence(): void {

    this.executeEditorSearch(false, true);
  }

  clearEditorSearch(): void {

    this.jsonSearchText = '';

    this.currentSearchIndex = -1;

    this.lastSearchText = '';

    this.searchMatchCount = 0;

    this.searchDecorations?.clear();
  }

  private syncEditorValue(): void {

    if (!this.editor) {
      return;
    }

    const model =
      this.editor.getModel();

    if (!model) {
      return;
    }

    const currentValue =
      model.getValue();

    const expectedValue =
      this.displayJsonText;

    if (currentValue !== expectedValue) {
      model.setValue(expectedValue);
    }
  }

  private executeEditorSearch(
    resetIndex = false,
    reverse = false
  ): void {

    const editor =
      this.editor;

    if (!editor) {
      return;
    }

    this.syncEditorValue();

    const model =
      editor.getModel();

    if (!model) {
      return;
    }

    const searchText =
      this.jsonSearchText.trim();

    if (!searchText) {

      this.currentSearchIndex = -1;

      this.lastSearchText = '';

      this.searchMatchCount = 0;

      this.searchDecorations?.clear();

      return;
    }

    const matches =
      model.findMatches(
        searchText,
        true,
        false,
        false,
        null,
        false,
        5000
      );

    this.searchMatchCount =
      matches.length;

    if (!matches.length) {

      this.currentSearchIndex = -1;

      this.searchDecorations?.clear();

      return;
    }

    const isSameSearch =
      searchText === this.lastSearchText;

    if (
      resetIndex ||
      !isSameSearch ||
      this.currentSearchIndex < 0
    ) {

      this.currentSearchIndex = 0;

    } else if (reverse) {

      this.currentSearchIndex =
        (
          this.currentSearchIndex - 1 +
          matches.length
        ) % matches.length;

    } else {

      this.currentSearchIndex =
        (
          this.currentSearchIndex + 1
        ) % matches.length;
    }

    this.lastSearchText =
      searchText;

    if (!this.searchDecorations) {

      this.searchDecorations =
        editor.createDecorationsCollection();
    }

    this.searchDecorations.set(
      matches.map((match, index) => ({
        range: match.range,
        options: {
          inlineClassName:
            index ===
            this.currentSearchIndex
              ? 'json-search-inline-current'
              : 'json-search-inline'
        }
      }))
    );

    const selectedMatch =
      matches[
        this.currentSearchIndex
      ].range;

    editor.setSelection(
      selectedMatch
    );

    editor.revealRangeInCenter(
      selectedMatch,
      monaco.editor.ScrollType.Immediate
    );
  }

  private async loadBinAsJson(
    node: S3TreeNode
  ): Promise<void> {

    this.isLoadingJson = true;

    this.jsonErrorMessage = '';

    this.rawJsonText = '';

    this.prettyJsonText = '';

    this.jsonViewer = {
      summary: []
    };

    this.currentSearchIndex = -1;

    this.lastSearchText = '';

    this.searchMatchCount = 0;

    this.searchDecorations?.clear();

    try {

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

      const messages =
        this.buildDecodedMessagesJson(
          parsed
        );

      this.rawJsonText =
        JSON.stringify(messages);

      this.prettyJsonText =
        JSON.stringify(
          messages,
          null,
          2
        );

      this.jsonViewer = {
        summary: [
          {
            label: 'Frames',
            value:
              parsed.totalFrameCount
                .toLocaleString()
          },
          {
            label: 'Blocks',
            value:
              parsed.blockCount
                .toLocaleString()
          },
          {
            label: 'Rendered',
            value:
              Math.min(
                parsed.blocks.length,
                this.maxRenderedBlocks
              ).toLocaleString()
          },
          {
            label: 'File bytes',
            value:
              parsed.totalBytes
                .toLocaleString()
          },
          {
            label: 'JSON size',
            value:
              this.formatBytes(
                new Blob([
                  this.prettyJsonText
                ]).size
              )
          }
        ]
      };

    } catch (error) {

      console.error(
        'Failed to decode BIN as JSON',
        error
      );

      this.jsonErrorMessage =
        error instanceof Error
          ? error.message
          : 'Failed to decode BIN as JSON.';

    } finally {

      this.isLoadingJson = false;

      setTimeout(() => {
        this.syncEditorValue();
        this.editor?.layout();
        this.applyEditorSearch(true);
      }, 0);
    }
  }

  private buildDecodedMessagesJson(
    parsed: any
  ): any[] {

    const messages: any[] = [];

    const blocks =
      Array.isArray(parsed.blocks)
        ? parsed.blocks.slice(
            0,
            this.maxRenderedBlocks
          )
        : [];

    const firstTimestampNs =
      blocks[0]?.timestampNs ?? '0';

    for (const block of blocks) {

      const blockTimestampNs =
        block.timestampNs ?? firstTimestampNs;

      for (
        const frame of block.frames ?? []
      ) {

        const signals:
          Record<string, any> = {};

        for (
          const signal of frame.signals ?? []
        ) {

          signals[
            signal.name
          ] = signal.value;
        }

        messages.push({
          timestamp:
            this.calculateFrameTimestampSeconds(
              firstTimestampNs,
              blockTimestampNs,
              frame.timestampDeltaNs
            ),

          canId:
            frame.canIdHex,

          name:
            frame.messageName ||
            `CAN_${frame.canIdHex}`,

          dlc:
            frame.payloadLength,

          data:
            this.normalizePayloadHex(
              frame.payloadBytes
            ),

          signals
        });
      }
    }

    return messages;
  }

  private calculateFrameTimestampSeconds(
    firstTimestampNs: string,
    blockTimestampNs: string,
    frameDeltaNs: string | number
  ): number {

    const baseNs =
      BigInt(
        firstTimestampNs || '0'
      );

    const blockNs =
      BigInt(
        blockTimestampNs || '0'
      );

    const deltaNs =
      BigInt(
        frameDeltaNs ?? 0
      );

    const absoluteNs =
      blockNs + deltaNs;

    const relativeNs =
      absoluteNs - baseNs;

    return Number(relativeNs) /
      1_000_000_000;
  }

  private normalizePayloadHex(
    payload: string
  ): string {

    if (!payload) {
      return '';
    }

    return payload
      .replace(/\s+/g, '')
      .toUpperCase();
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

    if (!bucket) {

      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    const clientId =
      this.resolveClientId(config);

    const runId =
      this.getRunIdFromKey(
        node.key
      );

    const manifestKey =
      `${clientId}/${runId}/run-manifest.json`;

    try {

      const buffer =
        await this.getS3ObjectBuffer(
          bucket,
          manifestKey
        );

      const manifestText =
        new TextDecoder('utf-8')
          .decode(buffer);

      return JSON.parse(
        manifestText
      );

    } catch (error) {

      console.warn(
        'Run manifest not available for JSON viewer',
        error
      );

      return null;
    }
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
        `Failed to load assets/config.json. HTTP ${response.status}`
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
      localStorage.getItem('clientId') ||
      localStorage.getItem('customerId') ||
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
}