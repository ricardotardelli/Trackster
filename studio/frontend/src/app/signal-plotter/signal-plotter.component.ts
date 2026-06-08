import { CommonModule } from '@angular/common';
import { NestedTreeControl } from '@angular/cdk/tree';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule, MatTreeNestedDataSource } from '@angular/material/tree';
import { EChartsOption, EChartsType } from 'echarts';
import { NgxEchartsDirective, provideEcharts } from 'ngx-echarts';

interface PlotSignalOption {
  id: string;
  messageName: string;
  signalName: string;
  unit: string;
  selected: boolean;
  values: number[];
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

@Component({
  selector: 'app-signal-plotter',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
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

  selectedBinFile = 'VINKDT000001KADUT.bin';
  selectedBinKey = '00000000/20260508183000/VINKDT000001KADUT.bin';
  selectedMessageNames: string[] = [
    'VehicleDynamics',
    'PowertrainStatus',
    'DriverInput',
    'BrakeSystem',
    'ElectricalSystem',
    'VehicleStatus'
  ];

  signalSelectionWarning = '';
  messageSelectionWarning = '';
  isBinPickerOpen = false;
  isMessagePickerOpen = false;
  isSignalPickerOpen = false;

  chartOptions: EChartsOption = {};

  private chartInstance?: EChartsType;
  private readonly hiddenSignalIds = new Set<string>();

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

  readonly timeAxisSeconds: number[] = [
    0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60
  ];

  readonly messageOptions: string[] = [
    'VehicleDynamics',
    'PowertrainStatus',
    'DriverInput',
    'BrakeSystem',
    'ElectricalSystem',
    'VehicleStatus',
    'Environment'
  ];

  readonly signalOptions: PlotSignalOption[] = [
    { id: 'vehicle-speed', messageName: 'VehicleDynamics', signalName: 'VehicleSpeed', unit: 'km/h', selected: true, values: [18, 31, 47, 63, 78, 81, 72, 59, 46, 35, 27, 21, 16] },
    { id: 'engine-rpm', messageName: 'PowertrainStatus', signalName: 'EngineRPM', unit: 'rpm', selected: true, values: [46, 58, 72, 88, 108, 126, 142, 136, 118, 94, 76, 59, 48] },
    { id: 'throttle-position', messageName: 'DriverInput', signalName: 'ThrottlePosition', unit: '%', selected: true, values: [8, 22, 38, 56, 74, 88, 96, 91, 82, 66, 48, 28, 12] },
    { id: 'brake-pressure', messageName: 'BrakeSystem', signalName: 'BrakePressure', unit: 'bar', selected: true, values: [4, 4, 5, 6, 8, 15, 30, 54, 76, 66, 44, 20, 8] },
    { id: 'battery-voltage', messageName: 'ElectricalSystem', signalName: 'BatteryVoltage', unit: 'V', selected: true, values: [124, 123, 122, 121, 119, 118, 116, 115, 113, 111, 109, 108, 107] },
    { id: 'steering-angle', messageName: 'VehicleDynamics', signalName: 'SteeringAngle', unit: 'deg', selected: true, values: [92, 78, 92, 70, 86, 62, 84, 64, 90, 72, 96, 80, 94] },
    { id: 'coolant-temperature', messageName: 'PowertrainStatus', signalName: 'CoolantTemperature', unit: '°C', selected: true, values: [78, 79, 80, 82, 84, 86, 88, 89, 90, 91, 92, 93, 94] },
    { id: 'fuel-level', messageName: 'VehicleStatus', signalName: 'FuelLevel', unit: '%', selected: true, values: [84, 84, 83, 83, 82, 82, 81, 81, 80, 80, 79, 79, 78] },
    { id: 'oil-pressure', messageName: 'PowertrainStatus', signalName: 'OilPressure', unit: 'bar', selected: false, values: [34, 35, 36, 38, 40, 41, 42, 41, 40, 39, 38, 37, 36] },
    { id: 'ambient-temperature', messageName: 'Environment', signalName: 'AmbientTemperature', unit: '°C', selected: false, values: [22, 22, 23, 23, 24, 24, 25, 25, 25, 24, 24, 23, 23] },
    { id: 'accelerator-pedal', messageName: 'DriverInput', signalName: 'AcceleratorPedal', unit: '%', selected: false, values: [5, 12, 24, 36, 48, 60, 72, 64, 52, 40, 28, 16, 8] },
    { id: 'yaw-rate', messageName: 'VehicleDynamics', signalName: 'YawRate', unit: 'deg/s', selected: false, values: [0, 2, 5, 8, 12, 15, 14, 11, 8, 5, 3, 1, 0] }
  ];

  constructor() {
    const nodes: BinTreeNode[] = [
      {
        name: '20260508183000',
        key: '00000000/20260508183000/',
        children: [
          { name: 'VINKDT000001KADUT.bin', key: '00000000/20260508183000/VINKDT000001KADUT.bin' },
          { name: 'VINKDT000002KADUT.bin', key: '00000000/20260508183000/VINKDT000002KADUT.bin' },
          { name: 'VINKDT000003KADUT.bin', key: '00000000/20260508183000/VINKDT000003KADUT.bin' }
        ]
      }
    ];

    this.binTreeDataSource.data = nodes;

    setTimeout(() => {
      this.binTreeControl.expand(nodes[0]);
    });

    this.rebuildChartOptions();
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

  get selectedMessageCount(): number {
    return this.selectedMessageNames.length;
  }

  get selectedMessageSummary(): string {
    if (this.selectedMessageNames.length === 0) {
      return 'No messages selected';
    }

    if (this.selectedMessageNames.length === 1) {
      return this.selectedMessageNames[0];
    }

    return `${this.selectedMessageNames.length} messages selected`;
  }

  get selectedSignalSummary(): string {
    if (this.selectedSignalCount === 0) {
      return 'No signals selected';
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
    this.rebuildChartOptions();
    this.resizeChart();
  }

  isSelectedBin(node: BinTreeNode): boolean {
    return this.selectedBinKey === node.key;
  }

  isMessageSelected(messageName: string): boolean {
    return this.selectedMessageNames.includes(messageName);
  }

  toggleMessage(messageName: string): void {
    if (this.isMessageSelected(messageName)) {
      if (this.selectedMessageNames.length === 1) {
        this.messageSelectionWarning = 'At least one message must remain selected.';
        return;
      }

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

    if (!signal.selected) {
      this.hiddenSignalIds.delete(signal.id);
    }

    this.signalSelectionWarning = '';
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
    this.resizeChart();
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
        trigger: 'item',
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
          const signal =
            this.signalOptions.find(
              item => item.signalName === params.seriesName
            );

          if (!signal) {
            return '';
          }

          const value = Number(params.value?.[1] ?? 0);
          const time = Number(params.value?.[0] ?? 0);

          return `
            <div style="
              min-width: 100px;
              color: #102349;
              font-family: inherit;
            ">
              <div style="
                font-size: 13px;
                font-weight: 700;
                line-height: 15px;
                margin-bottom: 4px;
              ">
                ${signal.signalName}
              </div>

              <div style="
                font-size: 19px;
                font-weight: 800;
                line-height: 20px;
                margin-bottom: 2px;
                color: #102349;
              ">
                ${this.formatSignalValue(value, signal.unit)}
              </div>

              <div style="
                font-size: 10px;
                font-weight: 500;
                color: #64748b;
                line-height: 12px;
                margin-bottom: 6px;
              ">
                ${time.toFixed(2)} s
              </div>

              <div style="
                font-size: 11px;
                font-weight: 500;
                color: #64748b;
                line-height: 15px;
              ">
                Min ${this.formatSignalValue(this.getSignalMin(signal), signal.unit)}<br>
                Max ${this.formatSignalValue(this.getSignalMax(signal), signal.unit)}<br>
                Avg ${this.formatSignalValue(this.getSignalAverage(signal), signal.unit)}
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
          moveOnMouseWheel: false
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
          showDataShadow: true,
          brushSelect: false,
          realtime: true,
          start: 0,
          end: 100,
          borderColor: 'rgba(147, 197, 253, 0.58)',
          fillerColor: 'rgba(147, 197, 253, 0.28)',
          backgroundColor: 'rgba(239, 246, 255, 0.68)',
          dataBackground: {
            lineStyle: {
              color: 'rgba(37, 99, 235, 0.32)',
              width: 1
            },
            areaStyle: {
              color: 'rgba(147, 197, 253, 0.18)'
            }
          },
          selectedDataBackground: {
            lineStyle: {
              color: 'rgba(2, 132, 199, 0.58)',
              width: 1
            },
            areaStyle: {
              color: 'rgba(56, 189, 248, 0.16)'
            }
          },
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
          showSymbol: true,
          symbolSize: 5,
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

  private pruneSignalsOutsideSelectedMessages(): void {
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

  private getSignalMin(signal: PlotSignalOption): number {
    return Math.min(...signal.values);
  }

  private getSignalMax(signal: PlotSignalOption): number {
    return Math.max(...signal.values);
  }

  private getSignalAverage(signal: PlotSignalOption): number {
    if (signal.values.length === 0) {
      return 0;
    }

    const total =
      signal.values.reduce(
        (sum, value) => sum + value,
        0
      );

    return total / signal.values.length;
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