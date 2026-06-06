import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule } from '@angular/material/tree';
import { FormsModule } from '@angular/forms';
import { MatMenuModule } from '@angular/material/menu';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatDividerModule } from '@angular/material/divider';
import { TracksterBinViewerComponent } from './viewers/trackster-bin-viewer/trackster-bin-viewer.component';
import { DecodedSignalsViewerComponent } from './viewers/decodedsignals-viewer/decodedsignals-viewer.component';
import { JsonViewerComponent } from './viewers/json-viewer/json-viewer.component';
import { CsvViewerComponent } from './viewers/csv-viewer/csv-viewer.component';
import { HexDumpViewerComponent } from './viewers/hex-dump-viewer/hex-dump-viewer.component';
import { VectorAscViewerComponent } from './viewers/vector-asc-viewer/vector-asc-viewer.component';
import { CandumpViewerComponent } from './viewers/candump-viewer/candump-viewer.component';
import { BlfViewerComponent } from './viewers/blf-viewer/blf-viewer.component';
import { Mf4ViewerComponent } from './viewers/mf4-viewer/mf4-viewer.component';
import { ParquetViewerComponent } from './viewers/parquet-viewer/parquet-viewer.component';
import { RunmanifestViewerComponent } from './viewers/runmanifest-viewer/runmanifest-viewer.component';
import { LocalFileSaveService } from './export-files/local-file-save.service';
import { DecoderExportService } from './decoder-export.service';
import { DecoderComponentCore } from './decoder.component.core';

export type { S3TreeNode, ExportFileFormat } from './decoder.component.core';

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
    MatCheckboxModule,
    TracksterBinViewerComponent,
    DecodedSignalsViewerComponent,
    MatDividerModule,
    JsonViewerComponent,
    CsvViewerComponent,
    HexDumpViewerComponent,
    VectorAscViewerComponent,
    CandumpViewerComponent,
    BlfViewerComponent,
    Mf4ViewerComponent,
    ParquetViewerComponent,
    RunmanifestViewerComponent
  ],
  templateUrl: './decoder.component.html',
  styleUrl: './decoder.component.css'
})
export class DecoderComponent extends DecoderComponentCore {

  constructor(
    localFileSaveService: LocalFileSaveService,
    decoderExportService: DecoderExportService
  ) {
    super(
      localFileSaveService,
      decoderExportService
    );
  }
}
