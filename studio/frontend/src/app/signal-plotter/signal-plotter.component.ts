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
  selectedMessageName = 'VehicleDynamics';
  signalSelectionWarning = '';
  isBinPickerOpen = false;

  chartOptions: EChartsOption = {};

  private chartInstance?: EChartsType;

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
    return this.signalOptions.filter(signal => signal.selected);
  }

  get selectedSignalCount(): number {
    return this.selectedSignals.length;
  }

  toggleBinPicker(): void {
    this.isBinPickerOpen = !this.isBinPickerOpen;
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

  canSelectSignal(signal: PlotSignalOption): boolean {
    return signal.selected || this.selectedSignalCount < this.maxSelectedSignals;
  }

  toggleSignal(signal: PlotSignalOption): void {
    if (!signal.selected && this.selectedSignalCount >= this.maxSelectedSignals) {
      this.signalSelectionWarning =
        `Maximum of ${this.maxSelectedSignals} signals can be plotted at once.`;
      return;
    }

    signal.selected = !signal.selected;
    this.signalSelectionWarning = '';
    this.rebuildChartOptions();
    this.resizeChart();
  }

  onChartInit(chart: EChartsType): void {
    this.chartInstance = chart;
    this.resizeChart();
  }

  private resizeChart(): void {
    window.setTimeout(() => {
      this.chartInstance?.resize();
    });
  }

  private rebuildChartOptions(): void {
    const tracksterSignalColors = [
      '#2563eb', 
      '#eab308', 
      '#dc2626', 
      '#22c55e', 
      '#d946ef', 
      '#7c3aed', 
      '#06b6d4', 
      '#64748b'  
    ];

    this.chartOptions = {
      animation: false,
      color: tracksterSignalColors,
      textStyle: {
        fontFamily: 'inherit',
        color: '#102349'
      },
      grid: {
        left: 42,
        right: 16,
        top: 46,
        bottom: 54,
        containLabel: false
      },
      tooltip: {
        trigger: 'item',
        confine: true,
        backgroundColor: '#ffffff',
        borderColor: 'rgba(191, 219, 254, 0.9)',
        borderWidth: 1,
        textStyle: {
          fontFamily: 'inherit',
          color: '#102349',
          fontSize: 12,
          fontWeight: 700
        },
        formatter: (params: any): string => {
          const signal =
            this.signalOptions.find(item => item.signalName === params.seriesName);

          if (!signal) {
            return '';
          }

          const value = Number(params.value?.[1] ?? 0);

          return [
            `<strong>${signal.signalName}</strong>`,
            `Time: ${Number(params.value?.[0] ?? 0).toFixed(2)} s`,
            `Value: ${this.formatSignalValue(value, signal.unit)}`,
            '',
            `Min: ${this.formatSignalValue(this.getSignalMin(signal), signal.unit)}`,
            `Max: ${this.formatSignalValue(this.getSignalMax(signal), signal.unit)}`,
            `Avg: ${this.formatSignalValue(this.getSignalAverage(signal), signal.unit)}`
          ].join('<br/>');
        }
      },
      legend: {
        show: true,
        top: 4,
        left: 4,
        right: 4,
        type: 'scroll',
        itemWidth: 16,
        itemHeight: 8,
        textStyle: {
          fontFamily: 'inherit',
          color: '#102349',
          fontSize: 10,
          fontWeight: 700
        }
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
            color: 'rgba(100, 116, 139, 0.35)'
          }
        },
        axisTick: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.35)'
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
          margin: 4
        },
        axisLine: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.35)'
          }
        },
        axisTick: {
          lineStyle: {
            color: 'rgba(100, 116, 139, 0.35)'
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
          moveOnMouseMove: true,
          moveOnMouseWheel: false
        },
        {
          type: 'slider',
          xAxisIndex: 0,
          height: 25,
          bottom: 12,
          filterMode: 'none',
          showDetail: false,
          showDataShadow: false,
          brushSelect: true,
          realtime: true,
          start: 0,
          end: 100,
          borderColor: 'rgba(96, 165, 250, 0.9)',
          fillerColor: 'rgba(147, 197, 253, 0.38)',
          backgroundColor: 'rgba(248, 251, 255, 1)',
          handleSize: '105%',
          handleStyle: {
            color: '#ffffff',
            borderColor: '#0284c7',
            borderWidth: 2
          },
          moveHandleSize: 7,
          moveHandleStyle: {
            color: '#0284c7'
          }
        }
      ],
      series: this.selectedSignals.map((signal, index) => {
        const signalColor =
          tracksterSignalColors[index % tracksterSignalColors.length];

        return {
          name: signal.signalName,
          type: 'line',
          showSymbol: true,
          symbolSize: 5,
          smooth: true,
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