import { CommonModule } from '@angular/common';
import { AfterViewInit, Component, OnInit, ViewChild } from '@angular/core';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NgxDropzoneModule } from 'ngx-dropzone';

type OriginalDbcStatus = 'pending' | 'validated' | 'rejected';

interface OriginalDbcFile {
  name: string;
  sizeBytes: number;
  lastModified: string;
  status: OriginalDbcStatus;
}

@Component({
  selector: 'app-dbcworkspace',
  standalone: true,
  imports: [
    CommonModule,
    NgxDropzoneModule,
    MatTableModule,
    MatSortModule,
    MatCheckboxModule
  ],
  templateUrl: './dbcworkspace.component.html',
  styleUrls: ['./dbcworkspace.component.css']
})
export class DbcworkspaceComponent implements OnInit, AfterViewInit {
  @ViewChild('intakeSort') intakeSort!: MatSort;

  selectedFiles: File[] = [];
  isUploading = false;

  selectedOriginalFileName: string | null = 'Powertrain.dbc';

  readonly intakeDisplayedColumns: string[] = [
    'select',
    'name',
    'status',
    'size',
    'lastModified'
  ];

  originalFiles: OriginalDbcFile[] = [
    { name: 'Engine_Control.dbc', status: 'validated', sizeBytes: 854200, lastModified: '2026-04-14 09:15' },
    { name: 'Battery_Management.dbc', status: 'pending', sizeBytes: 432100, lastModified: '2026-04-14 08:30' },
    { name: 'Transmission_v2.dbc', status: 'validated', sizeBytes: 981300, lastModified: '2026-04-14 00:00' },
    { name: 'Body_Control.dbc', status: 'validated', sizeBytes: 568500, lastModified: '2026-04-13 18:42' },
    { name: 'Powertrain_Main.dbc', status: 'pending', sizeBytes: 742900, lastModified: '2026-04-13 10:15' },
    { name: 'ADAS_Core_Module.dbc', status: 'pending', sizeBytes: 431000, lastModified: '2026-04-13 09:20' },
    { name: 'Gateway_Central.dbc', status: 'rejected', sizeBytes: 312300, lastModified: '2026-04-12 22:11' },
    { name: 'Braking_System.dbc', status: 'validated', sizeBytes: 125400, lastModified: '2026-04-12 20:05' },
    { name: 'Climate_Control.dbc', status: 'pending', sizeBytes: 234800, lastModified: '2026-04-12 15:30' },
    { name: 'Infotainment_Bus.dbc', status: 'validated', sizeBytes: 1054200, lastModified: '2026-04-12 11:00' },
    { name: 'Sensors_Array_A.dbc', status: 'pending', sizeBytes: 154000, lastModified: '2026-04-11 19:45' },
    { name: 'Sensors_Array_B.dbc', status: 'rejected', sizeBytes: 148000, lastModified: '2026-04-11 19:50' },
    { name: 'Lighting_Exterior.dbc', status: 'validated', sizeBytes: 89000, lastModified: '2026-04-11 14:20' },
    { name: 'Steering_Angle.dbc', status: 'pending', sizeBytes: 67000, lastModified: '2026-04-11 10:10' },
    { name: 'Chassis_CAN_v1.dbc', status: 'validated', sizeBytes: 882000, lastModified: '2026-04-10 23:55' },
    { name: 'Safety_Systems.dbc', status: 'validated', sizeBytes: 345000, lastModified: '2026-04-10 17:30' },
    { name: 'Inverters_Rear.dbc', status: 'pending', sizeBytes: 512000, lastModified: '2026-04-10 12:40' },
    { name: 'HVAC_Compressor.dbc', status: 'rejected', sizeBytes: 122000, lastModified: '2026-04-09 16:15' },
    { name: 'Airbag_Controller.dbc', status: 'validated', sizeBytes: 78000, lastModified: '2026-04-09 14:05' },
    { name: 'Telematics_Unit.dbc', status: 'pending', sizeBytes: 940000, lastModified: '2026-04-09 09:30' },
    { name: 'Drivetrain_Diagnostics.dbc', status: 'validated', sizeBytes: 654000, lastModified: '2026-04-08 21:10' },
    { name: 'Tire_Pressure_TPMS.dbc', status: 'pending', sizeBytes: 45000, lastModified: '2026-04-08 18:45' },
    { name: 'Wheel_Speeds.dbc', status: 'validated', sizeBytes: 112000, lastModified: '2026-04-08 14:20' },
    { name: 'Mirror_Controls.dbc', status: 'rejected', sizeBytes: 33000, lastModified: '2026-04-07 13:00' },
    { name: 'Door_Lock_States.dbc', status: 'validated', sizeBytes: 56000, lastModified: '2026-04-07 11:30' },
    { name: 'Window_Regulator.dbc', status: 'pending', sizeBytes: 89000, lastModified: '2026-04-07 08:20' },
    { name: 'Seat_Adjustment.dbc', status: 'validated', sizeBytes: 128000, lastModified: '2026-04-06 17:55' },
    { name: 'Parking_Assist.dbc', status: 'pending', sizeBytes: 672000, lastModified: '2026-04-06 14:10' },
    { name: 'Ultrasonic_Sensors.dbc', status: 'validated', sizeBytes: 341000, lastModified: '2026-04-06 10:05' },
    { name: 'Front_Camera_Bus.dbc', status: 'rejected', sizeBytes: 1205000, lastModified: '2026-04-05 22:40' },
    { name: 'Radar_Long_Range.dbc', status: 'validated', sizeBytes: 884000, lastModified: '2026-04-05 19:15' },
    { name: 'Instrument_Cluster.dbc', status: 'pending', sizeBytes: 542000, lastModified: '2026-04-05 15:30' },
    { name: 'Fuel_System.dbc', status: 'validated', sizeBytes: 212000, lastModified: '2026-04-05 09:45' },
    { name: 'Exhaust_Monitoring.dbc', status: 'pending', sizeBytes: 135000, lastModified: '2026-04-04 18:20' },
    { name: 'Generic_IO_Module.dbc', status: 'rejected', sizeBytes: 44000, lastModified: '2026-04-04 14:10' }
  ];

  originalFilesDataSource = new MatTableDataSource<OriginalDbcFile>([]);
  checkedOriginalFileNames = new Set<string>();

  ngOnInit(): void {
    this.originalFilesDataSource.data = this.originalFiles;
  }

  ngAfterViewInit(): void {
    this.originalFilesDataSource.sort = this.intakeSort;

    this.originalFilesDataSource.sortingDataAccessor = (
      item: OriginalDbcFile,
      property: string
    ): string | number => {
      switch (property) {
        case 'name':
          return item.name.toLowerCase();
        case 'status':
          return item.status.toLowerCase();
        case 'size':
          return item.sizeBytes;
        case 'lastModified':
          return this.toSortableTimestamp(item.lastModified);
        default:
          return '';
      }
    };
  }

  onSelectFiles(files: File[]): void {
    const dbcFiles = files.filter((file) => file.name.toLowerCase().endsWith('.dbc'));

    const uniqueFiles = dbcFiles.filter((newFile) => {
      return !this.selectedFiles.some(
        (existingFile) =>
          existingFile.name === newFile.name &&
          existingFile.size === newFile.size &&
          existingFile.lastModified === newFile.lastModified
      );
    });

    this.selectedFiles = [...this.selectedFiles, ...uniqueFiles];
  }

  onRemoveSelectedFile(fileToRemove: File): void {
    this.selectedFiles = this.selectedFiles.filter((file) => file !== fileToRemove);
  }

  clearSelectedFiles(): void {
    this.selectedFiles = [];
  }

  async uploadSelectedFiles(): Promise<void> {
    if (this.selectedFiles.length === 0 || this.isUploading) {
      return;
    }

    this.isUploading = true;

    try {
      const now = this.getCurrentTimestamp();

      const uploadedEntries: OriginalDbcFile[] = this.selectedFiles.map((file) => ({
        name: file.name,
        sizeBytes: file.size,
        lastModified: now,
        status: 'rejected'
      }));

      this.originalFiles = [...uploadedEntries, ...this.originalFiles];
      this.originalFilesDataSource.data = this.originalFiles;
      this.selectedFiles = [];
      this.selectedOriginalFileName = uploadedEntries[0]?.name ?? this.selectedOriginalFileName;
    } finally {
      this.isUploading = false;
    }
  }

  hasPendingSelection(): boolean {
    return this.originalFiles.some(
      (file) =>
        (file.status === 'pending' || file.status === 'rejected') &&
        this.checkedOriginalFileNames.has(file.name)
    );
  }

  validateSelectedFiles(): void {
    if (!this.hasPendingSelection()) {
      return;
    }

    this.originalFiles = this.originalFiles.map((file) => {
      if (
        this.checkedOriginalFileNames.has(file.name) &&
        this.isFileValidatable(file)
      ) {
        return {
          ...file,
          status: 'validated',
          lastModified: this.getCurrentTimestamp()
        };
      }

      return file;
    });

    this.checkedOriginalFileNames.clear();
    this.originalFilesDataSource.data = this.originalFiles;
  }

  removeSelectedFiles(): void {
    const remaining = this.originalFiles.filter(
      (file) => !this.checkedOriginalFileNames.has(file.name)
    );

    this.originalFiles = remaining;
    this.originalFilesDataSource.data = remaining;
    this.checkedOriginalFileNames.clear();

    if (
      this.selectedOriginalFileName &&
      !remaining.some((file) => file.name === this.selectedOriginalFileName)
    ) {
      this.selectedOriginalFileName = remaining[0]?.name ?? null;
    }
  }

  selectOriginalFile(file: OriginalDbcFile): void {
    this.selectedOriginalFileName = file.name;
  }

  isOriginalSelected(file: OriginalDbcFile): boolean {
    return this.selectedOriginalFileName === file.name;
  }

  isFileValidatable(file: OriginalDbcFile): boolean {
    return file.status === 'pending' || file.status === 'rejected';
  }

  isFileSelectable(file: OriginalDbcFile): boolean {
    return true;
  }

  isOriginalChecked(file: OriginalDbcFile): boolean {
    return this.checkedOriginalFileNames.has(file.name);
  }

  toggleOriginalChecked(file: OriginalDbcFile, checked: boolean): void {
    if (!this.isFileSelectable(file)) {
      return;
    }

    if (checked) {
      this.checkedOriginalFileNames.add(file.name);
    } else {
      this.checkedOriginalFileNames.delete(file.name);
    }
  }

  toggleAllPendingSelections(checked: boolean): void {
    if (checked) {
      this.originalFiles.forEach((file) => this.checkedOriginalFileNames.add(file.name));
      return;
    }

    this.originalFiles.forEach((file) => this.checkedOriginalFileNames.delete(file.name));
  }

  areAllPendingSelected(): boolean {
    if (this.originalFiles.length === 0) {
      return false;
    }

    return this.originalFiles.every((file) =>
      this.checkedOriginalFileNames.has(file.name)
    );
  }

  isPendingSelectionIndeterminate(): boolean {
    const checkedCount = this.originalFiles.filter((file) =>
      this.checkedOriginalFileNames.has(file.name)
    ).length;

    return checkedCount > 0 && checkedCount < this.originalFiles.length;
  }

  hasAnySelection(): boolean {
    return this.checkedOriginalFileNames.size > 0;
  }

  hasPendingFiles(): boolean {
    return this.originalFiles.some((file) => this.isFileValidatable(file));
  }

  getPendingCount(): number {
    return this.originalFiles.filter((file) => file.status === 'pending').length;
  }

  getValidatedCount(): number {
    return this.originalFiles.filter((file) => file.status === 'validated').length;
  }

  getRejectedCount(): number {
    return this.originalFiles.filter((file) => file.status === 'rejected').length;
  }

  getLastUploadLabel(): string {
    if (this.originalFiles.length === 0) {
      return '-';
    }

    const sorted = [...this.originalFiles].sort(
      (a, b) => this.toSortableTimestamp(b.lastModified) - this.toSortableTimestamp(a.lastModified)
    );

    return sorted[0].lastModified;
  }

  formatFileSize(sizeBytes: number): string {
    if (sizeBytes < 1024) {
      return `${sizeBytes} B`;
    }

    if (sizeBytes < 1024 * 1024) {
      return `${(sizeBytes / 1024).toFixed(1)} KB`;
    }

    return `${(sizeBytes / (1024 * 1024)).toFixed(2)} MB`;
  }

  onNativeFilesSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    const files = input.files ? Array.from(input.files) : [];

    if (files.length > 0) {
      this.onSelectFiles(files);
    }

    input.value = '';
  }

  private getCurrentTimestamp(): string {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}`;
  }

  private toSortableTimestamp(value: string): number {
    return new Date(value.replace(' ', 'T')).getTime();
  }
}