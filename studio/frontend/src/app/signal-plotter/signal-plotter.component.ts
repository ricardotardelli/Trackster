import { CommonModule } from '@angular/common';
import { NestedTreeControl } from '@angular/cdk/tree';
import { HttpClient, HttpClientModule } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { fetchAuthSession } from 'aws-amplify/auth';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule, MatTreeNestedDataSource } from '@angular/material/tree';
import { EChartsOption, EChartsType } from 'echarts';
import { NgxEchartsDirective, provideEcharts } from 'ngx-echarts';
import { firstValueFrom } from 'rxjs';

type SignalChartType =
  | 'signals-over-time'
  | 'signal-vs-signal';

interface PlotSignalOption {
  id: string;
  canId: string;
  messageName: string;
  signalName: string;
  unit: string;
  selected: boolean;
  values: number[];
}

interface SignalPlotterData {
  selectedBinFile: string;
  selectedBinKey: string;
  selectedMessageNames: string[];
  timeAxisSeconds: number[];
  messageOptions: string[];
  signalOptions: PlotSignalOption[];
}

interface BinTreeFile {
  name: string;
  key: string;
}

interface BinTreeNode {
  name: string;
  key: string;
  children?: BinTreeNode[];
}

interface AxisBounds {
  min: number;
  max: number;
}

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  customerId?: string;
  clientId?: string;
  decoderApi?: {
    signalPlotterDataUrl?: string;
  };
}

interface RunManifestSignalFrame {
  l?: number;
  n?: string;
  s?: unknown[][];
}

interface RunManifestResolvedCanFrame {
  dbcFile?: string;
  canId?: string;
  messageName?: string;
  frame?: RunManifestSignalFrame;
}

interface RunManifest {
  output?: {
    bucket?: string;
    runFolder?: string;
    manifestKey?: string;
  };
  simulation?: {
    intervalSec?: number;
    durationSec?: number;
    numberOfBlocks?: number;
  };
  dbc?: {
    resolvedCanFrames?: RunManifestResolvedCanFrame[];
    canFrames?: RunManifestResolvedCanFrame[];
  };
}

interface SignalPlotDataRequestSignal {
  id: string;
  canId: string;
  messageName: string;
  signalName: string;
}

interface SignalPlotDataRequest {
  bucket: string;
  binKey: string;
  manifestKey: string;
  chartType: SignalChartType;
  signals: SignalPlotDataRequestSignal[];
}

interface SignalPlotDataResponseSignal {
  id?: string;
  canId?: string;
  messageName: string;
  signalName: string;
  unit?: string;
  values: number[];
}

interface SignalPlotDataResponse {
  timeAxisSeconds: number[];
  signals: SignalPlotDataResponseSignal[];
}

@Component({
  selector: 'app-signal-plotter',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    HttpClientModule,
    MatTreeModule,
    MatIconModule,
    NgxEchartsDirective
  ],
  providers: [
    provideEcharts()
  ],
  templateUrl: './signal-plotter.component.html',
  styleUrl: './signal-plotter.component.css'
})
export class SignalPlotterComponent {

  readonly maxSelectedSignals = 8;

  chartType: SignalChartType = 'signals-over-time';

  selectedBinFile = '';
  selectedBinKey = '';
  selectedMessageNames: string[] = [];

  selectedXAxisSignalId = '';
  selectedYAxisSignalId = '';

  signalSelectionWarning = '';
  messageSelectionWarning = '';
  isBinPickerOpen = false;
  isLoadingBinCatalog = false;
  isLoadingPlotData = false;
  isMessagePickerOpen = false;
  isSignalPickerOpen = false;

  chartOptions: EChartsOption = {};

  timeAxisSeconds: number[] = [];
  messageOptions: string[] = [];
  signalOptions: PlotSignalOption[] = [];

  private chartInstance?: EChartsType;
  private readonly hiddenSignalIds = new Set<string>();

  private fullTimeAxisSeconds: number[] = [];
  private fullSignalOptions: PlotSignalOption[] = [];

  private fullStartSeconds = 0;
  private fullEndSeconds = 0;

  private visibleStartSeconds = 0;
  private visibleEndSeconds = 0;

  private currentZoomStartPercent = 0;
  private currentZoomEndPercent = 100;

  private isUpdatingZoom = false;

  private readonly maxRenderedPointsPerSignal = 120;
  private readonly maxPointsWithSymbols = 80;
  private readonly developmentRunManifestUrl = 'assets/mock/run-manifest.json';
  private readonly developmentSignalPlotDataUrl = 'assets/mock/signal-plot-data.json';

  readonly tracksterSignalColors = [
    '#2563eb',
    '#eab308',
    '#dc2626',
    '#22c55e',
    '#d946ef',
    '#7c3aed',
    '#06b6d4',
    '#64748b'
  ];

  readonly binTreeControl = new NestedTreeControl<BinTreeNode>(
    node => node.children ?? []
  );

  readonly binTreeDataSource = new MatTreeNestedDataSource<BinTreeNode>();

  constructor(
    private readonly http: HttpClient
  ) {
    void this.loadPlotterData();
  }

  hasBinChild = (_: number, node: BinTreeNode): boolean => {
    return !!node.children && node.children.length > 0;
  };

  get selectedSignals(): PlotSignalOption[] {
    return this.availableSignalOptions.filter(signal => signal.selected);
  }

  get availableSignalOptions(): PlotSignalOption[] {
    return this.signalOptions.filter(signal =>
      this.selectedMessageNames.includes(signal.messageName)
    );
  }

  get availableSignalCount(): number {
    return this.availableSignalOptions.length;
  }

  get selectedMessageCount(): number {
    return this.selectedMessageNames.length;
  }

  get selectedMessageSummary(): string {
    if (this.selectedMessageNames.length === 0) {
      return 'Select Message';
    }

    if (this.selectedMessageNames.length === 1) {
      return this.selectedMessageNames[0];
    }

    return `${this.selectedMessageNames.length} messages selected`;
  }

  get selectedSignalSummary(): string {
    if (this.selectedSignalCount === 0) {
      return 'No Signal Selected';
    }

    if (this.selectedSignalCount === 1) {
      return this.selectedSignals[0].signalName;
    }

    return `${this.selectedSignalCount} signals selected`;
  }

  get visibleSelectedSignals(): PlotSignalOption[] {
    return this.selectedSignals.filter(signal => !this.hiddenSignalIds.has(signal.id));
  }

  get selectedSignalCount(): number {
    return this.selectedSignals.length;
  }

  get maxSelectableSignalsForCurrentMessages(): number {
    return Math.min(
      this.maxSelectedSignals,
      this.availableSignalCount
    );
  }

  get selectedXAxisSignalName(): string {
    return this.getSignalNameById(this.selectedXAxisSignalId);
  }

  get selectedYAxisSignalName(): string {
    return this.getSignalNameById(this.selectedYAxisSignalId);
  }

  get plotterHeaderSummary(): string {
    if (this.isLoadingPlotData) {
      return `${this.selectedBinFile} · loading signal data`;
    }

    if (this.chartType === 'signal-vs-signal') {
      return `${this.selectedBinFile} · ${this.selectedXAxisSignalName} x ${this.selectedYAxisSignalName}`;
    }

    return `${this.selectedBinFile} · ${this.selectedMessageCount} messages · ${this.selectedSignalCount} signals`;
  }

  toggleBinPicker(): void {
    this.isBinPickerOpen = !this.isBinPickerOpen;
    this.isMessagePickerOpen = false;
    this.isSignalPickerOpen = false;
  }

  toggleMessagePicker(): void {
    this.isMessagePickerOpen = !this.isMessagePickerOpen;
    this.isBinPickerOpen = false;
    this.isSignalPickerOpen = false;
  }

  toggleSignalPicker(): void {
    this.isSignalPickerOpen = !this.isSignalPickerOpen;
    this.isBinPickerOpen = false;
    this.isMessagePickerOpen = false;
  }

  selectBinFile(file: BinTreeFile): void {
    this.selectedBinFile = file.name;
    this.selectedBinKey = file.key;
    this.isBinPickerOpen = false;
    void this.loadManifestForSelectedBin();
  }

  isSelectedBin(node: BinTreeNode): boolean {
    return this.selectedBinKey === node.key;
  }

  isMessageSelected(messageName: string): boolean {
    return this.selectedMessageNames.includes(messageName);
  }

  toggleMessage(messageName: string): void {
    if (this.isMessageSelected(messageName)) {
      this.selectedMessageNames =
        this.selectedMessageNames.filter(selected => selected !== messageName);
    } else {
      this.selectedMessageNames = [
        ...this.selectedMessageNames,
        messageName
      ];
    }

    this.messageSelectionWarning = '';
    this.pruneSignalsOutsideSelectedMessages();
    this.ensureSignalVsSignalSelection();
    this.clearPlotValues();

    if (this.chartType === 'signal-vs-signal' && this.getRequestedSignalsForCurrentChart().length > 0) {
      void this.loadPlotDataForCurrentSelection();
      return;
    }

    this.rebuildChartOptions();
    this.resizeChart();
  }

  isSignalAvailable(signal: PlotSignalOption): boolean {
    return this.selectedMessageNames.includes(signal.messageName);
  }

  canSelectSignal(signal: PlotSignalOption): boolean {
    return this.isSignalAvailable(signal) &&
      (signal.selected || this.selectedSignalCount < this.maxSelectedSignals);
  }

  toggleSignal(signal: PlotSignalOption): void {
    if (!this.isSignalAvailable(signal)) {
      return;
    }

    if (!signal.selected && this.selectedSignalCount >= this.maxSelectedSignals) {
      this.signalSelectionWarning =
        `Maximum of ${this.maxSelectedSignals} signals can be plotted at once.`;
      return;
    }

    signal.selected = !signal.selected;
    this.syncSignalSelectionToFullDataset(signal.id, signal.selected);

    if (!signal.selected) {
      this.hiddenSignalIds.delete(signal.id);
    }

    this.signalSelectionWarning = '';

    if (this.getRequestedSignalsForCurrentChart().length === 0) {
      this.clearPlotValues();
      this.rebuildChartOptions();
      this.resizeChart();
      return;
    }

    void this.loadPlotDataForCurrentSelection();
  }

  selectAllAvailableSignals(): void {
    if (this.availableSignalOptions.length === 0) {
      return;
    }

    const selectedIds =
      new Set(
        this.availableSignalOptions
          .slice(0, this.maxSelectedSignals)
          .map(signal => signal.id)
      );

    this.fullSignalOptions.forEach(signal => {
      if (this.selectedMessageNames.includes(signal.messageName)) {
        signal.selected = selectedIds.has(signal.id);
      }
    });

    this.signalOptions.forEach(signal => {
      if (this.selectedMessageNames.includes(signal.messageName)) {
        signal.selected = selectedIds.has(signal.id);
      }
    });

    this.hiddenSignalIds.clear();

    this.signalSelectionWarning =
      this.availableSignalOptions.length > this.maxSelectedSignals
        ? `Only the first ${this.maxSelectedSignals} available signals were selected.`
        : '';

    void this.loadPlotDataForCurrentSelection();
  }

  deselectAllAvailableSignals(): void {
    this.fullSignalOptions.forEach(signal => {
      if (this.selectedMessageNames.includes(signal.messageName)) {
        signal.selected = false;
        this.hiddenSignalIds.delete(signal.id);
      }
    });

    this.signalOptions.forEach(signal => {
      if (this.selectedMessageNames.includes(signal.messageName)) {
        signal.selected = false;
        this.hiddenSignalIds.delete(signal.id);
      }
    });

    this.signalSelectionWarning = '';
    this.clearPlotValues();
    this.rebuildChartOptions();
    this.resizeChart();
  }

  changeChartType(value: SignalChartType): void {
    this.chartType = value;
    this.isSignalPickerOpen = false;
    this.signalSelectionWarning = '';

    if (this.chartType === 'signal-vs-signal') {
      this.ensureSignalVsSignalSelection();
    }

    this.resetChartBeforeRebuild();

    if (this.getRequestedSignalsForCurrentChart().length > 0) {
      void this.loadPlotDataForCurrentSelection();
      return;
    }

    this.clearPlotValues();
    this.rebuildChartOptions();
    this.resizeChart();
  }

  changeXAxisSignal(signalId: string): void {
    this.selectedXAxisSignalId = signalId;

    if (this.selectedXAxisSignalId === this.selectedYAxisSignalId) {
      this.selectedYAxisSignalId = this.getAlternativeSignalId(this.selectedXAxisSignalId);
    }

    this.ensureSignalVsSignalSelection();

    if (this.getRequestedSignalsForCurrentChart().length > 0) {
      void this.loadPlotDataForCurrentSelection();
      return;
    }

    this.clearPlotValues();
    this.rebuildChartOptions();
    this.resizeChart();
  }

  changeYAxisSignal(signalId: string): void {
    this.selectedYAxisSignalId = signalId;

    if (this.selectedYAxisSignalId === this.selectedXAxisSignalId) {
      this.selectedXAxisSignalId = this.getAlternativeSignalId(this.selectedYAxisSignalId);
    }

    this.ensureSignalVsSignalSelection();

    if (this.getRequestedSignalsForCurrentChart().length > 0) {
      void this.loadPlotDataForCurrentSelection();
      return;
    }

    this.clearPlotValues();
    this.rebuildChartOptions();
    this.resizeChart();
  }

  getSignalColor(index: number): string {
    return this.tracksterSignalColors[index % this.tracksterSignalColors.length];
  }

  isSignalHidden(signal: PlotSignalOption): boolean {
    return this.hiddenSignalIds.has(signal.id);
  }

  toggleSignalVisibility(signal: PlotSignalOption): void {
    if (this.hiddenSignalIds.has(signal.id)) {
      this.hiddenSignalIds.delete(signal.id);
    } else {
      this.hiddenSignalIds.add(signal.id);
    }

    this.rebuildChartOptions();
    this.resizeChart();
  }

  highlightSignal(signal: PlotSignalOption): void {
    if (this.hiddenSignalIds.has(signal.id)) {
      return;
    }

    this.chartInstance?.dispatchAction({
      type: 'highlight',
      seriesName: signal.signalName
    });

    this.forceChartCursor();
  }

  clearSignalHighlight(): void {
    this.chartInstance?.dispatchAction({
      type: 'downplay'
    });

    this.forceChartCursor();
  }

  onChartInit(chart: EChartsType): void {
    this.chartInstance = chart;
    this.registerChartCursorHandlers();
    this.registerChartZoomHandler();
    this.resizeChart();
  }

  private async loadPlotterData(): Promise<void> {
    this.isLoadingBinCatalog = true;

    try {
      const config =
        await this.loadRuntimeConfig();

      const clientId =
        this.resolveClientId(config);

      if (this.shouldUseLocalMock()) {
        const tree =
          this.buildLocalMockTree();

        this.setBinTreeData(tree);

        const firstBinNode =
          this.findFirstBinNode(tree);

        this.selectedBinFile = firstBinNode?.name ?? 'No BIN file available';
        this.selectedBinKey = firstBinNode?.key ?? '';

        await this.loadManifestForSelectedBin();

        return;
      }

      const bucket =
        config.s3Default?.trim();

      if (!bucket) {
        throw new Error(
          'Missing s3Default in assets/config.json'
        );
      }

      const prefix = `${clientId}/`;

      const keys =
        await this.listS3KeysFromBucket(
          bucket,
          prefix
        );

      const tree =
        this.buildTreeFromS3Keys(
          keys,
          clientId
        );

      this.setBinTreeData(tree);

      const firstBinNode =
        this.findFirstBinNode(tree);

      this.selectedBinFile = firstBinNode?.name ?? 'No BIN file available';
      this.selectedBinKey = firstBinNode?.key ?? '';

      if (this.selectedBinKey) {
        await this.loadManifestForSelectedBin();
      } else {
        this.applyPlotterData(
          this.buildEmptyPlotterData(firstBinNode)
        );
      }

    } catch (error) {
      console.error('Failed to load signal plotter data.', error);
      this.clearPlotterData('No plotter data loaded');
    } finally {
      this.isLoadingBinCatalog = false;
    }
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

  private async loadDevelopmentRunManifest(): Promise<RunManifest> {
    return firstValueFrom(
      this.http.get<RunManifest>(this.developmentRunManifestUrl)
    );
  }

  private async loadDevelopmentSignalPlotData(): Promise<SignalPlotDataResponse> {
    return firstValueFrom(
      this.http.get<SignalPlotDataResponse>(this.developmentSignalPlotDataUrl)
    );
  }

  private async loadManifestForSelectedBin(): Promise<void> {
    try {
      const manifest =
        this.shouldUseLocalMock()
          ? await this.loadDevelopmentRunManifest()
          : await this.loadProductionRunManifestForSelectedBin();

      const data =
        this.buildPlotterDataFromManifest(
          manifest,
          this.selectedBinFile,
          this.selectedBinKey
        );

      this.applyPlotterData(data);
    } catch (error) {
      console.error('Failed to load signal plotter manifest.', error);

      this.applyPlotterData({
        selectedBinFile: this.selectedBinFile || 'No BIN file available',
        selectedBinKey: this.selectedBinKey,
        selectedMessageNames: [],
        timeAxisSeconds: [],
        messageOptions: [],
        signalOptions: []
      });
    }
  }

  private async loadProductionRunManifestForSelectedBin(): Promise<RunManifest> {
    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Default?.trim();

    if (!bucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    const manifestKey =
      this.resolveManifestKeyFromSelectedBinKey(this.selectedBinKey);

    const s3Client =
      await this.getS3Client();

    const response =
      await s3Client.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: manifestKey
        })
      );

    const body =
      await response.Body?.transformToString();

    if (!body) {
      throw new Error(
        `Empty run-manifest.json response for ${manifestKey}`
      );
    }

    return JSON.parse(body) as RunManifest;
  }

  private async loadPlotDataForCurrentSelection(): Promise<void> {
    const requestedSignals =
      this.getRequestedSignalsForCurrentChart();

    if (requestedSignals.length === 0) {
      this.clearPlotValues();
      this.rebuildChartOptions();
      this.resizeChart();
      return;
    }

    this.isLoadingPlotData = true;

    try {
      const response =
        this.shouldUseLocalMock()
          ? await this.loadDevelopmentSignalPlotData()
          : await this.loadProductionSignalPlotData(requestedSignals);

      this.applySignalPlotDataResponse(response);
    } catch (error) {
      console.error('Failed to load signal plot data.', error);
      this.clearPlotValues();
      this.rebuildChartOptions();
      this.resizeChart();
    } finally {
      this.isLoadingPlotData = false;
    }
  }

  private async loadProductionSignalPlotData(
    requestedSignals: PlotSignalOption[]
  ): Promise<SignalPlotDataResponse> {

    const config =
      await this.loadRuntimeConfig();

    const bucket =
      config.s3Default?.trim();

    if (!bucket) {
      throw new Error(
        'Missing s3Default in assets/config.json'
      );
    }

    const plotDataUrl =
      this.resolveSignalPlotterDataUrl(config);

    const manifestKey =
      this.resolveManifestKeyFromSelectedBinKey(this.selectedBinKey);

    const request: SignalPlotDataRequest = {
      bucket,
      binKey: this.selectedBinKey,
      manifestKey,
      chartType: this.chartType,
      signals: requestedSignals.map(signal => ({
        id: signal.id,
        canId: signal.canId,
        messageName: signal.messageName,
        signalName: signal.signalName
      }))
    };

    return firstValueFrom(
      this.http.post<SignalPlotDataResponse>(
        plotDataUrl,
        request
      )
    );
  }

  private resolveSignalPlotterDataUrl(
    config: RuntimeConfig
  ): string {

    const url =
      config.decoderApi?.signalPlotterDataUrl || '';

    if (!url.trim()) {
      throw new Error(
        'Missing Signal Plotter API URL in assets/config.json. Expected decoderApi.signalPlotterDataUrl.'
      );
    }

    return url.trim();
  }

  private getRequestedSignalsForCurrentChart(): PlotSignalOption[] {
    if (this.chartType === 'signal-vs-signal') {
      const requestedSignals =
        [
          this.getSignalById(this.selectedXAxisSignalId),
          this.getSignalById(this.selectedYAxisSignalId)
        ]
          .filter((signal): signal is PlotSignalOption => !!signal);

      return Array.from(
        new Map(
          requestedSignals.map(signal => [signal.id, signal])
        ).values()
      );
    }

    return this.selectedSignals;
  }

  private applySignalPlotDataResponse(
    response: SignalPlotDataResponse
  ): void {

    const responseSignals =
      Array.isArray(response.signals)
        ? response.signals
        : [];

    this.fullTimeAxisSeconds =
      Array.isArray(response.timeAxisSeconds)
        ? [...response.timeAxisSeconds]
        : [];

    this.fullSignalOptions =
      this.fullSignalOptions.map(signal => {
        const responseSignal =
          responseSignals.find(item =>
            this.isSameSignalFromPlotData(signal, item)
          );

        if (!responseSignal) {
          return {
            ...signal,
            values: []
          };
        }

        return {
          ...signal,
          unit: responseSignal.unit ?? signal.unit,
          values: Array.isArray(responseSignal.values)
            ? [...responseSignal.values]
            : []
        };
      });

    this.fullStartSeconds = this.fullTimeAxisSeconds[0] ?? 0;
    this.fullEndSeconds =
      this.fullTimeAxisSeconds[this.fullTimeAxisSeconds.length - 1] ?? this.fullStartSeconds;

    this.visibleStartSeconds = this.fullStartSeconds;
    this.visibleEndSeconds = this.fullEndSeconds;

    this.currentZoomStartPercent = 0;
    this.currentZoomEndPercent = 100;

    this.refreshVisibleDataset();
    this.rebuildChartOptions();
    this.resizeChart();
  }

  private isSameSignalFromPlotData(
    signal: PlotSignalOption,
    responseSignal: SignalPlotDataResponseSignal
  ): boolean {

    if (responseSignal.id && responseSignal.id === signal.id) {
      return true;
    }

    return (
      (responseSignal.canId ?? '').toLowerCase() === signal.canId.toLowerCase() &&
      responseSignal.messageName === signal.messageName &&
      responseSignal.signalName === signal.signalName
    );
  }

  private clearPlotValues(): void {
    this.fullTimeAxisSeconds = [];
    this.timeAxisSeconds = [];

    this.fullStartSeconds = 0;
    this.fullEndSeconds = 0;
    this.visibleStartSeconds = 0;
    this.visibleEndSeconds = 0;

    this.currentZoomStartPercent = 0;
    this.currentZoomEndPercent = 100;

    this.fullSignalOptions =
      this.fullSignalOptions.map(signal => ({
        ...signal,
        values: []
      }));

    this.signalOptions =
      this.signalOptions.map(signal => ({
        ...signal,
        values: []
      }));
  }

  private resolveManifestKeyFromSelectedBinKey(selectedBinKey: string): string {
    const parts =
      selectedBinKey
        .split('/')
        .filter(part => part.length > 0);

    if (parts.length < 2) {
      throw new Error(
        `Invalid selected BIN key: ${selectedBinKey}`
      );
    }

    return `${parts.slice(0, -1).join('/')}/run-manifest.json`;
  }

  private buildPlotterDataFromManifest(
    manifest: RunManifest,
    selectedBinFile: string,
    selectedBinKey: string
  ): SignalPlotterData {

    const resolvedFrames =
      this.getResolvedCanFramesFromManifest(manifest);

    const messageOptions =
      resolvedFrames
        .map(frame => frame.messageName?.trim() ?? frame.frame?.n?.trim() ?? '')
        .filter(messageName => messageName.length > 0);

    const uniqueMessageOptions =
      Array.from(new Set(messageOptions));

    const signalOptions =
      this.buildSignalOptionsFromManifest(
        resolvedFrames
      );

    return {
      selectedBinFile: selectedBinFile || 'No BIN file selected',
      selectedBinKey,
      selectedMessageNames: [],
      timeAxisSeconds: [],
      messageOptions: uniqueMessageOptions,
      signalOptions: signalOptions.map(signal => ({
        ...signal,
        selected: false,
        values: []
      }))
    };
  }

  private getResolvedCanFramesFromManifest(
    manifest: RunManifest
  ): RunManifestResolvedCanFrame[] {

    const resolvedFrames =
      manifest.dbc?.resolvedCanFrames ?? [];

    if (resolvedFrames.length > 0) {
      return resolvedFrames;
    }

    return manifest.dbc?.canFrames ?? [];
  }

  private buildSignalOptionsFromManifest(
    frames: RunManifestResolvedCanFrame[]
  ): PlotSignalOption[] {

    const signalOptions: PlotSignalOption[] = [];

    frames.forEach((frame, frameIndex) => {
      const canId =
        frame.canId?.trim() ||
        `frame-${frameIndex}`;

      const messageName =
        frame.messageName?.trim() ||
        frame.frame?.n?.trim() ||
        `Message_${canId}`;

      const signals =
        frame.frame?.s ?? [];

      signals.forEach((rawSignal, signalIndex) => {
        const signalName =
          this.getManifestSignalName(rawSignal, signalIndex);

        const id =
          [
            canId,
            messageName,
            signalName,
            signalIndex
          ].join('__');

        signalOptions.push({
          id,
          canId,
          messageName,
          signalName,
          unit: '',
          selected: false,
          values: []
        });
      });
    });

    return signalOptions;
  }

  private getManifestSignalName(
    rawSignal: unknown[],
    signalIndex: number
  ): string {

    const compactName =
      rawSignal[8];

    return typeof compactName === 'string' && compactName.trim().length > 0
      ? compactName.trim()
      : `Signal_${signalIndex + 1}`;
  }

  private buildEmptyPlotterData(
    selectedBinNode: BinTreeNode | null
  ): SignalPlotterData {

    return {
      selectedBinFile: selectedBinNode?.name ?? 'No BIN file available',
      selectedBinKey: selectedBinNode?.key ?? '',
      selectedMessageNames: [],
      timeAxisSeconds: [],
      messageOptions: [],
      signalOptions: []
    };
  }

  private clearPlotterData(
    selectedBinFile: string
  ): void {

    this.selectedBinFile = selectedBinFile;
    this.selectedBinKey = '';
    this.selectedMessageNames = [];
    this.timeAxisSeconds = [];
    this.messageOptions = [];
    this.signalOptions = [];
    this.fullTimeAxisSeconds = [];
    this.fullSignalOptions = [];
    this.binTreeDataSource.data = [];
    this.binTreeControl.dataNodes = [];
    this.chartOptions = {};
  }

  private async loadRuntimeConfig(): Promise<RuntimeConfig> {
    const response =
      await fetch(`assets/config.json?t=${Date.now()}`);

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

    const clientId =
      config.clientId ||
      config.customerId ||
      localStorage.getItem('clientId') ||
      localStorage.getItem('customerId') ||
      '00000000';

    if (!/^[a-zA-Z0-9]{8}$/.test(clientId)) {
      throw new Error(
        `Invalid clientId: ${clientId}`
      );
    }

    return clientId;
  }

  private buildLocalMockTree(): BinTreeNode[] {
    return [
      {
        name: '20260521152301',
        key: '00000000/20260521152301',
        children: [
          {
            name: 'VINKDT000001KADUT.bin',
            key: '00000000/20260521152301/VINKDT000001KADUT.bin'
          }
        ]
      }
    ];
  }

  private async getS3Client(): Promise<S3Client> {
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
      credentials: session.credentials
    });
  }

  private async listS3KeysFromBucket(
    bucket: string,
    prefix: string
  ): Promise<string[]> {

    const s3Client =
      await this.getS3Client();

    const keys: string[] = [];

    let continuationToken:
      string | undefined;

    do {
      const response =
        await s3Client.send(
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

      continuationToken =
        response.NextContinuationToken;

    } while (continuationToken);

    return keys;
  }

  private setBinTreeData(data: BinTreeNode[]): void {
    this.binTreeControl.expansionModel.clear();
    this.binTreeDataSource.data = data;
    this.binTreeControl.dataNodes = data;

    const firstFolderNode = data.find(node =>
      node.children && node.children.length > 0
    );

    if (firstFolderNode) {
      this.binTreeControl.expand(firstFolderNode);
    }
  }

  private findFirstBinNode(nodes: BinTreeNode[]): BinTreeNode | null {
    for (const node of nodes) {
      if (!node.children && node.name.toLowerCase().endsWith('.bin')) {
        return node;
      }

      const childMatch =
        this.findFirstBinNode(node.children ?? []);

      if (childMatch) {
        return childMatch;
      }
    }

    return null;
  }

  private buildTreeFromS3Keys(
    keys: string[],
    clientId: string
  ): BinTreeNode[] {

    const runs =
      new Map<string, BinTreeNode>();

    const prefix = `${clientId}/`;

    for (const rawKey of keys) {
      const key =
        rawKey.replace(
          /^generated-files\//,
          ''
        );

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

      if (!fileName.toLowerCase().endsWith('.bin')) {
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

  private applyPlotterData(data: SignalPlotterData): void {
    this.selectedBinFile = data.selectedBinFile;
    this.selectedBinKey = data.selectedBinKey;
    this.selectedMessageNames = [...data.selectedMessageNames];

    this.fullTimeAxisSeconds = [...data.timeAxisSeconds];
    this.messageOptions = [...data.messageOptions];

    this.fullSignalOptions = data.signalOptions.map(signal => ({
      ...signal,
      values: [...signal.values]
    }));

    this.fullStartSeconds = this.fullTimeAxisSeconds[0] ?? 0;
    this.fullEndSeconds =
      this.fullTimeAxisSeconds[this.fullTimeAxisSeconds.length - 1] ?? this.fullStartSeconds;

    this.visibleStartSeconds = this.fullStartSeconds;
    this.visibleEndSeconds = this.fullEndSeconds;

    this.currentZoomStartPercent = 0;
    this.currentZoomEndPercent = 100;

    this.hiddenSignalIds.clear();
    this.signalSelectionWarning = '';
    this.messageSelectionWarning = '';

    this.refreshVisibleDataset();
    this.pruneSignalsOutsideSelectedMessages();
    this.ensureSignalVsSignalSelection();
    this.refreshVisibleDataset();
    this.rebuildChartOptions();
    this.resizeChart();
  }

  private refreshVisibleDataset(): void {
    const visibleIndexes = this.getVisibleSourceIndexes();
    const sampledIndexes = this.downsampleIndexes(visibleIndexes);

    this.timeAxisSeconds = sampledIndexes.map(index =>
      this.fullTimeAxisSeconds[index]
    );

    this.signalOptions = this.fullSignalOptions.map(signal => ({
      ...signal,
      values: sampledIndexes.map(index => signal.values[index] ?? 0)
    }));
  }

  private getVisibleSourceIndexes(): number[] {
    const indexes: number[] = [];

    this.fullTimeAxisSeconds.forEach((time, index) => {
      if (time >= this.visibleStartSeconds && time <= this.visibleEndSeconds) {
        indexes.push(index);
      }
    });

    return indexes;
  }

  private downsampleIndexes(sourceIndexes: number[]): number[] {
    if (sourceIndexes.length <= this.maxRenderedPointsPerSignal) {
      return sourceIndexes;
    }

    const sampledIndexes: number[] = [];
    const lastSourcePosition = sourceIndexes.length - 1;

    sampledIndexes.push(sourceIndexes[0]);

    for (let outputIndex = 1; outputIndex < this.maxRenderedPointsPerSignal - 1; outputIndex++) {
      const sourcePosition =
        Math.round(
          outputIndex * lastSourcePosition / (this.maxRenderedPointsPerSignal - 1)
        );

      sampledIndexes.push(sourceIndexes[sourcePosition]);
    }

    sampledIndexes.push(sourceIndexes[lastSourcePosition]);

    return Array.from(new Set(sampledIndexes)).sort((left, right) => left - right);
  }

  private syncSignalSelectionToFullDataset(signalId: string, selected: boolean): void {
    const fullSignal =
      this.fullSignalOptions.find(signal => signal.id === signalId);

    if (fullSignal) {
      fullSignal.selected = selected;
    }
  }

  private ensureSignalVsSignalSelection(): void {
    const availableSignals = this.availableSignalOptions;

    if (availableSignals.length === 0) {
      this.selectedXAxisSignalId = '';
      this.selectedYAxisSignalId = '';

      if (this.chartType === 'signal-vs-signal') {
        this.signalSelectionWarning = 'Select at least one message before comparing signals.';
      } else {
        this.signalSelectionWarning = '';
      }

      return;
    }

    if (!availableSignals.some(signal => signal.id === this.selectedXAxisSignalId)) {
      this.selectedXAxisSignalId = availableSignals[0].id;
    }

    if (!availableSignals.some(signal => signal.id === this.selectedYAxisSignalId)) {
      this.selectedYAxisSignalId =
        availableSignals.length > 1
          ? availableSignals[1].id
          : availableSignals[0].id;
    }

    if (
      availableSignals.length > 1 &&
      this.selectedXAxisSignalId === this.selectedYAxisSignalId
    ) {
      this.selectedYAxisSignalId = this.getAlternativeSignalId(this.selectedXAxisSignalId);
    }

    if (
      availableSignals.length === 1 &&
      this.chartType === 'signal-vs-signal'
    ) {
      this.signalSelectionWarning = 'Select at least two available signals to compare X and Y.';
      return;
    }

    this.signalSelectionWarning = '';
  }

  private getAlternativeSignalId(currentSignalId: string): string {
    const alternative =
      this.availableSignalOptions.find(signal => signal.id !== currentSignalId);

    return alternative?.id ?? currentSignalId;
  }

  private getSignalNameById(signalId: string): string {
    const signal =
      this.signalOptions.find(option => option.id === signalId) ??
      this.fullSignalOptions.find(option => option.id === signalId);

    return signal?.signalName ?? 'No signal';
  }

  private getSignalById(signalId: string): PlotSignalOption | undefined {
    return this.signalOptions.find(signal => signal.id === signalId);
  }

  private buildSignalVsSignalData(
    xSignal: PlotSignalOption,
    ySignal: PlotSignalOption
  ): Array<[number, number]> {
    const groupedValues = new Map<number, { total: number; count: number }>();

    xSignal.values.forEach((xValue, index) => {
      const yValue = ySignal.values[index];

      if (
        typeof xValue !== 'number' ||
        typeof yValue !== 'number' ||
        Number.isNaN(xValue) ||
        Number.isNaN(yValue)
      ) {
        return;
      }

      const existing = groupedValues.get(xValue);

      if (existing) {
        existing.total += yValue;
        existing.count += 1;
      } else {
        groupedValues.set(xValue, {
          total: yValue,
          count: 1
        });
      }
    });

    return Array.from(groupedValues.entries())
      .map(([xValue, aggregate]) => [
        xValue,
        aggregate.total / aggregate.count
      ] as [number, number])
      .sort((left, right) => left[0] - right[0]);
  }

  private getAxisBounds(values: number[]): AxisBounds {
    if (values.length === 0) {
      return {
        min: 0,
        max: 1
      };
    }

    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);

    if (minValue === maxValue) {
      return {
        min: Math.floor(minValue - 1),
        max: Math.ceil(maxValue + 1)
      };
    }

    const padding = (maxValue - minValue) * 0.08;

    return {
      min: Math.floor(minValue - padding),
      max: Math.ceil(maxValue + padding)
    };
  }

  private registerChartCursorHandlers(): void {
    const chart = this.chartInstance;

    if (!chart) {
      return;
    }

    const zr = chart.getZr();

    this.forceChartCursor();

    zr.on('mousemove', () => {
      this.forceChartCursor();

      window.setTimeout(() => {
        this.forceChartCursor();
      });
    });

    zr.on('mouseover', () => {
      this.forceChartCursor();

      window.setTimeout(() => {
        this.forceChartCursor();
      });
    });

    zr.on('globalout', () => {
      this.forceChartCursor();
    });
  }

  private registerChartZoomHandler(): void {
    const chart = this.chartInstance;

    if (!chart) {
      return;
    }

    chart.off('dataZoom');

    chart.on('dataZoom', () => {
      if (this.chartType !== 'signals-over-time') {
        return;
      }

      this.handleChartZoomRefresh();
    });
  }

  private handleChartZoomRefresh(): void {
    if (this.isUpdatingZoom || this.fullTimeAxisSeconds.length === 0) {
      return;
    }

    const chart = this.chartInstance;

    if (!chart) {
      return;
    }

    const option = chart.getOption() as {
      dataZoom?: Array<{
        start?: number;
        end?: number;
      }>;
    };

    const zoom =
      option.dataZoom?.find(item =>
        typeof item.start === 'number' &&
        typeof item.end === 'number'
      );

    if (!zoom) {
      return;
    }

    const nextStartPercent = zoom.start ?? 0;
    const nextEndPercent = zoom.end ?? 100;

    if (
      Math.abs(nextStartPercent - this.currentZoomStartPercent) < 0.01 &&
      Math.abs(nextEndPercent - this.currentZoomEndPercent) < 0.01
    ) {
      return;
    }

    this.currentZoomStartPercent = nextStartPercent;
    this.currentZoomEndPercent = nextEndPercent;

    this.updateVisibleWindowFromZoomPercent();
    this.refreshVisibleDataset();

    this.isUpdatingZoom = true;
    this.rebuildChartOptions();

    window.setTimeout(() => {
      this.isUpdatingZoom = false;
      this.forceChartCursor();
    });
  }

  private updateVisibleWindowFromZoomPercent(): void {
    const fullDuration = this.fullEndSeconds - this.fullStartSeconds;

    this.visibleStartSeconds =
      this.fullStartSeconds + fullDuration * (this.currentZoomStartPercent / 100);

    this.visibleEndSeconds =
      this.fullStartSeconds + fullDuration * (this.currentZoomEndPercent / 100);
  }

  private resetChartBeforeRebuild(): void {
    this.chartInstance?.clear();
  }

  private forceChartCursor(): void {
    const chart = this.chartInstance;

    if (!chart) {
      return;
    }

    const chartDom = chart.getDom();
    const zr = chart.getZr();

    chartDom.style.cursor = 'default';
    zr.setCursorStyle('default');

    const canvases = chartDom.querySelectorAll('canvas');

    canvases.forEach(canvas => {
      canvas.style.cursor = 'default';
    });
  }

  private resizeChart(): void {
    window.setTimeout(() => {
      this.chartInstance?.resize();
      this.forceChartCursor();
    });
  }

  private rebuildChartOptions(): void {
    if (this.chartType === 'signal-vs-signal') {
      this.rebuildSignalVsSignalChartOptions();
      return;
    }

    this.rebuildSignalsOverTimeChartOptions();
  }

  private rebuildSignalsOverTimeChartOptions(): void {
    const shouldShowSymbols =
      this.timeAxisSeconds.length > 0 &&
      this.timeAxisSeconds.length <= this.maxPointsWithSymbols;

    this.chartOptions = {
      animation: false,
      color: this.tracksterSignalColors,
      textStyle: {
        fontFamily: 'inherit',
        color: '#102349',
        fontSize: 12,
        fontWeight: 500
      },
      grid: {
        left: 58,
        right: 16,
        top: 8,
        bottom: 48,
        containLabel: false
      },
      tooltip: {
        trigger: 'axis',
        confine: true,
        backgroundColor: 'rgba(255, 255, 255, 0.96)',
        borderColor: 'rgba(191, 219, 254, 0.52)',
        borderWidth: 1,
        padding: [8, 10],
        extraCssText:
          'border-radius: 10px; box-shadow: 0 10px 20px rgba(15, 23, 42, 0.10); backdrop-filter: blur(6px);',
        textStyle: {
          fontFamily: 'inherit',
          color: '#102349',
          fontSize: 12,
          fontWeight: 500,
          lineHeight: 16
        },
        formatter: (params: any): string => {
          const firstParam = Array.isArray(params) ? params[0] : params;
          const time = Number(firstParam?.value?.[0] ?? 0);

          const rows =
            (Array.isArray(params) ? params : [params])
              .map((item: any) => {
                const signal =
                  this.signalOptions.find(
                    option => option.signalName === item.seriesName
                  );

                if (!signal) {
                  return '';
                }

                const value = Number(item.value?.[1] ?? 0);

                return `
                  <div style="
                    display: flex;
                    justify-content: space-between;
                    gap: 16px;
                    font-size: 11px;
                    font-weight: 600;
                    color: #64748b;
                    line-height: 16px;
                  ">
                    <span>${signal.signalName}</span>
                    <strong style="color: #102349;">
                      ${this.formatSignalValue(value, signal.unit)}
                    </strong>
                  </div>
                `;
              })
              .join('');

          return `
            <div style="
              min-width: 180px;
              color: #102349;
              font-family: inherit;
            ">
              <div style="
                font-size: 12px;
                font-weight: 800;
                line-height: 16px;
                margin-bottom: 6px;
                color: #102349;
              ">
                ${time.toFixed(2)} s
              </div>

              ${rows}
            </div>
          `;
        }
      },
      legend: {
        show: false
      },
      xAxis: {
        type: 'value',
        min: this.fullStartSeconds,
        max: this.fullEndSeconds,
        axisLabel: {
          formatter: '{value} s',
          fontFamily: 'inherit',
          color: '#5a6b82',
          fontSize: 10,
          fontWeight: 700,
          margin: 4
        },
        axisLine: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        axisTick: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(191, 219, 254, 0.45)'
          }
        }
      },
      yAxis: {
        type: 'value',
        min: 0,
        max: 160,
        axisLabel: {
          fontFamily: 'inherit',
          color: '#5a6b82',
          fontSize: 10,
          fontWeight: 700,
          margin: 6
        },
        axisLine: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        axisTick: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(191, 219, 254, 0.45)'
          }
        }
      },
      dataZoom: [
        {
          type: 'inside',
          xAxisIndex: 0,
          filterMode: 'none',
          zoomOnMouseWheel: true,
          moveOnMouseMove: false,
          moveOnMouseWheel: false,
          start: this.currentZoomStartPercent,
          end: this.currentZoomEndPercent
        },
        {
          type: 'slider',
          xAxisIndex: 0,
          height: 21,
          bottom: 6,
          left: 58,
          right: 16,
          filterMode: 'none',
          showDetail: false,
          showDataShadow: false,
          brushSelect: false,
          realtime: false,
          start: this.currentZoomStartPercent,
          end: this.currentZoomEndPercent,
          borderColor: 'rgba(147, 197, 253, 0.58)',
          fillerColor: 'rgba(147, 197, 253, 0.28)',
          backgroundColor: 'rgba(239, 246, 255, 0.68)',
          handleSize: '88%',
          handleIcon: 'path://M8.2,0 L11.8,0 Q13,0 13,1.2 L13,22.8 Q13,24 11.8,24 L8.2,24 Q7,24 7,22.8 L7,1.2 Q7,0 8.2,0 Z',
          handleStyle: {
            color: '#ffffff',
            borderColor: '#0284c7',
            borderWidth: 2,
            shadowBlur: 4,
            shadowColor: 'rgba(15, 23, 42, 0.16)',
            shadowOffsetY: 1
          },
          moveHandleSize: 6,
          moveHandleStyle: {
            color: '#0284c7',
            opacity: 0.68
          },
          emphasis: {
            handleStyle: {
              borderColor: '#0369a1',
              shadowBlur: 6,
              shadowColor: 'rgba(15, 23, 42, 0.2)'
            },
            moveHandleStyle: {
              color: '#0369a1'
            }
          }
        }
      ],
      series: this.visibleSelectedSignals.map((signal) => {
        const selectedSignalIndex =
          this.selectedSignals.findIndex(selected => selected.id === signal.id);

        const signalColor =
          this.tracksterSignalColors[
            selectedSignalIndex % this.tracksterSignalColors.length
          ];

        return {
          name: signal.signalName,
          type: 'line',
          showSymbol: shouldShowSymbols,
          symbolSize: 4,
          smooth: true,
          cursor: 'default',
          emphasis: {
            focus: 'series',
            lineStyle: {
              width: 3
            }
          },
          itemStyle: {
            color: signalColor
          },
          lineStyle: {
            color: signalColor,
            width: 2
          },
          data: this.timeAxisSeconds.map((time, valueIndex) => [
            time,
            signal.values[valueIndex] ?? 0
          ])
        };
      })
    };
  }

  private rebuildSignalVsSignalChartOptions(): void {
    const xSignal = this.getSignalById(this.selectedXAxisSignalId);
    const ySignal = this.getSignalById(this.selectedYAxisSignalId);

    const data =
      xSignal && ySignal
        ? this.buildSignalVsSignalData(xSignal, ySignal)
        : [];

    const xBounds = this.getAxisBounds(data.map(point => point[0]));
    const yBounds = this.getAxisBounds(data.map(point => point[1]));

    this.chartOptions = {
      animation: false,
      color: this.tracksterSignalColors,
      textStyle: {
        fontFamily: 'inherit',
        color: '#102349',
        fontSize: 12,
        fontWeight: 500
      },
      grid: {
        left: 58,
        right: 18,
        top: 12,
        bottom: 42,
        containLabel: false
      },
      tooltip: {
        trigger: 'axis',
        confine: true,
        backgroundColor: 'rgba(255, 255, 255, 0.96)',
        borderColor: 'rgba(191, 219, 254, 0.52)',
        borderWidth: 1,
        padding: [8, 10],
        extraCssText:
          'border-radius: 10px; box-shadow: 0 10px 20px rgba(15, 23, 42, 0.10); backdrop-filter: blur(6px);',
        textStyle: {
          fontFamily: 'inherit',
          color: '#102349',
          fontSize: 12,
          fontWeight: 500,
          lineHeight: 16
        },
        formatter: (params: any): string => {
          const point = Array.isArray(params) ? params[0] : params;
          const xValue = Number(point?.value?.[0] ?? 0);
          const yValue = Number(point?.value?.[1] ?? 0);

          return `
            <div style="
              min-width: 170px;
              color: #102349;
              font-family: inherit;
            ">
              <div style="
                display: flex;
                justify-content: space-between;
                gap: 16px;
                font-size: 11px;
                font-weight: 600;
                color: #64748b;
                line-height: 16px;
              ">
                <span>${xSignal?.signalName ?? 'X'}</span>
                <strong style="color: #102349;">
                  ${this.formatSignalValue(xValue, xSignal?.unit ?? '')}
                </strong>
              </div>

              <div style="
                display: flex;
                justify-content: space-between;
                gap: 16px;
                font-size: 11px;
                font-weight: 600;
                color: #64748b;
                line-height: 16px;
              ">
                <span>${ySignal?.signalName ?? 'Y'}</span>
                <strong style="color: #102349;">
                  ${this.formatSignalValue(yValue, ySignal?.unit ?? '')}
                </strong>
              </div>
            </div>
          `;
        }
      },
      legend: {
        show: false
      },
      xAxis: {
        type: 'value',
        name: xSignal?.signalName ?? 'X Signal',
        nameLocation: 'middle',
        nameGap: 25,
        min: xBounds.min,
        max: xBounds.max,
        axisLabel: {
          fontFamily: 'inherit',
          color: '#5a6b82',
          fontSize: 10,
          fontWeight: 700,
          margin: 4
        },
        axisLine: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        axisTick: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(191, 219, 254, 0.45)'
          }
        }
      },
      yAxis: {
        type: 'value',
        name: ySignal?.signalName ?? 'Y Signal',
        nameLocation: 'middle',
        nameGap: 38,
        min: yBounds.min,
        max: yBounds.max,
        axisLabel: {
          fontFamily: 'inherit',
          color: '#5a6b82',
          fontSize: 10,
          fontWeight: 700,
          margin: 6
        },
        axisLine: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        axisTick: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.38)'
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(191, 219, 254, 0.45)'
          }
        }
      },
      series: [
        {
          name: `${xSignal?.signalName ?? 'X'} x ${ySignal?.signalName ?? 'Y'}`,
          type: 'line',
          showSymbol: true,
          symbolSize: 4,
          smooth: true,
          cursor: 'default',
          itemStyle: {
            color: this.tracksterSignalColors[0]
          },
          lineStyle: {
            color: this.tracksterSignalColors[0],
            width: 2
          },
          emphasis: {
            focus: 'series',
            lineStyle: {
              width: 3
            }
          },
          data
        }
      ]
    };

    this.forceChartCursor();
  }

  private pruneSignalsOutsideSelectedMessages(): void {
    this.fullSignalOptions.forEach(signal => {
      if (!this.selectedMessageNames.includes(signal.messageName)) {
        signal.selected = false;
        this.hiddenSignalIds.delete(signal.id);
      }
    });

    this.signalOptions.forEach(signal => {
      if (!this.isSignalAvailable(signal)) {
        signal.selected = false;
        this.hiddenSignalIds.delete(signal.id);
      }
    });

    if (this.selectedSignalCount <= this.maxSelectedSignals) {
      this.signalSelectionWarning = '';
    }
  }

  private formatSignalValue(value: number, unit: string): string {
    const formattedValue =
      Number.isInteger(value)
        ? value.toString()
        : value.toFixed(2);

    return unit
      ? `${formattedValue} ${unit}`
      : formattedValue;
  }
}