import { Injectable } from '@angular/core';
import { BlfExportHandler } from './export-files/blf-export.handler';
import { CandumpExportHandler } from './export-files/candump-export.handler';
import { CsvExportHandler } from './export-files/csv-export.handler';
import { DecodedSignalsExportHandler } from './export-files/decoded-signals-export.handler';
import { DecoderExportHandler } from './export-files/decoder-export-handler';
import { HexDumpExportHandler } from './export-files/hex-dump-export.handler';
import { JsonExportHandler } from './export-files/json-export.handler';
import { Mf4ExportHandler } from './export-files/mf4-export.handler';
import { ParquetExportHandler } from './export-files/parquet-export.handler';
import { RunManifestExportHandler } from './export-files/run-manifest-export.handler';
import { TracksterBinExportHandler } from './export-files/trackster-bin-export.handler';
import { VectorAscExportHandler } from './export-files/vector-asc-export.handler';

export interface DecoderExportHost {
  exportCurrentTracksterBinFile(): Promise<boolean>;
  exportCurrentDecodedSignalsFile(): Promise<boolean>;
  exportCurrentJsonFile(): Promise<boolean>;
  exportCurrentCsvFile(): Promise<boolean>;
  exportCurrentHexDumpFile(): Promise<boolean>;
  exportCurrentVectorAscFile(): Promise<boolean>;
  exportCurrentCandumpFile(): Promise<boolean>;
  exportCurrentBlfFile(): Promise<boolean>;
  exportCurrentMf4File(): Promise<boolean>;
  exportCurrentParquetFile(): Promise<boolean>;
  exportCurrentRunManifestFile(): Promise<boolean>;

  exportSelectedTracksterBinFiles(): Promise<boolean>;
  exportSelectedDecodedSignalsFiles(): Promise<boolean>;
  exportSelectedJsonFiles(): Promise<boolean>;
  exportSelectedCsvFiles(): Promise<boolean>;
  exportSelectedHexDumpFiles(): Promise<boolean>;
  exportSelectedVectorAscFiles(): Promise<boolean>;
  exportSelectedCandumpFiles(): Promise<boolean>;
  exportSelectedBlfFiles(): Promise<boolean>;
  exportSelectedMf4Files(): Promise<boolean>;
  exportSelectedParquetFiles(): Promise<boolean>;
  exportSelectedRunManifestFiles(): Promise<boolean>;

  exportSelectedTracksterBinFolders(): Promise<boolean>;
  exportSelectedDecodedSignalsFolders(): Promise<boolean>;
  exportSelectedJsonFolders(): Promise<boolean>;
  exportSelectedCsvFolders(): Promise<boolean>;
  exportSelectedHexDumpFolders(): Promise<boolean>;
  exportSelectedVectorAscFolders(): Promise<boolean>;
  exportSelectedCandumpFolders(): Promise<boolean>;
  exportSelectedBlfFolders(): Promise<boolean>;
  exportSelectedMf4Folders(): Promise<boolean>;
  exportSelectedParquetFolders(): Promise<boolean>;
  exportSelectedRunManifestFolders(): Promise<boolean>;
}

@Injectable({
  providedIn: 'root'
})
export class DecoderExportService {

  private readonly handlersByViewerMode =
    this.buildHandlersByViewerMode();

  async exportCurrentFile(
    viewerMode: string,
    host: DecoderExportHost
  ): Promise<boolean> {

    const handler =
      this.getHandler(viewerMode);

    return await handler.exportCurrent(host);
  }

  async exportSelectedFiles(
    viewerMode: string,
    host: DecoderExportHost
  ): Promise<boolean> {

    const handler =
      this.getHandler(viewerMode);

    return await handler.exportSelectedFiles(host);
  }

  async exportSelectedFolders(
    viewerMode: string,
    host: DecoderExportHost
  ): Promise<boolean> {

    const handler =
      this.getHandler(viewerMode);

    return await handler.exportSelectedFolders(host);
  }

  private getHandler(
    viewerMode: string
  ): DecoderExportHandler {

    const normalizedViewerMode =
      this.normalizeViewerMode(viewerMode);

    const handler =
      this.handlersByViewerMode.get(normalizedViewerMode);

    if (!handler) {
      throw new Error(
        `Export for viewer mode "${viewerMode}" is not integrated yet.`
      );
    }

    return handler;
  }

  private buildHandlersByViewerMode(): Map<string, DecoderExportHandler> {
    const handlers: DecoderExportHandler[] = [
      new TracksterBinExportHandler(),
      new DecodedSignalsExportHandler(),
      new JsonExportHandler(),
      new CsvExportHandler(),
      new HexDumpExportHandler(),
      new VectorAscExportHandler(),
      new CandumpExportHandler(),
      new BlfExportHandler(),
      new Mf4ExportHandler(),
      new ParquetExportHandler(),
      new RunManifestExportHandler()
    ];

    const handlersByViewerMode =
      new Map<string, DecoderExportHandler>();

    for (const handler of handlers) {
      for (const viewerMode of handler.viewerModes) {
        handlersByViewerMode.set(
          this.normalizeViewerMode(viewerMode),
          handler
        );
      }
    }

    return handlersByViewerMode;
  }

  private normalizeViewerMode(
    viewerMode: string
  ): string {

    if (viewerMode === 'runmanifest') {
      return 'run-manifest';
    }

    return viewerMode;
  }
}
