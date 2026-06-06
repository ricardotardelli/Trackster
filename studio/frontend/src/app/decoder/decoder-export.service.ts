import { Injectable } from '@angular/core';

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

  async exportCurrentFile(
    viewerMode: string,
    host: DecoderExportHost
  ): Promise<boolean> {

    switch (this.normalizeViewerMode(viewerMode)) {
      case 'trackster-bin':
        return await host.exportCurrentTracksterBinFile();
      case 'decoded-signals':
        return await host.exportCurrentDecodedSignalsFile();
      case 'json':
        return await host.exportCurrentJsonFile();
      case 'csv':
        return await host.exportCurrentCsvFile();
      case 'hex-dump':
        return await host.exportCurrentHexDumpFile();
      case 'vector-asc':
        return await host.exportCurrentVectorAscFile();
      case 'candump':
        return await host.exportCurrentCandumpFile();
      case 'blf':
        return await host.exportCurrentBlfFile();
      case 'mf4':
        return await host.exportCurrentMf4File();
      case 'parquet':
        return await host.exportCurrentParquetFile();
      case 'run-manifest':
        return await host.exportCurrentRunManifestFile();
      default:
        throw new Error(
          `Export for viewer mode "${viewerMode}" is not integrated yet.`
        );
    }
  }

  async exportSelectedFiles(
    viewerMode: string,
    host: DecoderExportHost
  ): Promise<boolean> {

    switch (this.normalizeViewerMode(viewerMode)) {
      case 'trackster-bin':
        return await host.exportSelectedTracksterBinFiles();
      case 'decoded-signals':
        return await host.exportSelectedDecodedSignalsFiles();
      case 'json':
        return await host.exportSelectedJsonFiles();
      case 'csv':
        return await host.exportSelectedCsvFiles();
      case 'hex-dump':
        return await host.exportSelectedHexDumpFiles();
      case 'vector-asc':
        return await host.exportSelectedVectorAscFiles();
      case 'candump':
        return await host.exportSelectedCandumpFiles();
      case 'blf':
        return await host.exportSelectedBlfFiles();
      case 'mf4':
        return await host.exportSelectedMf4Files();
      case 'parquet':
        return await host.exportSelectedParquetFiles();
      case 'run-manifest':
        return await host.exportSelectedRunManifestFiles();
      default:
        throw new Error(
          `Selected files export for viewer mode "${viewerMode}" is not integrated yet.`
        );
    }
  }

  async exportSelectedFolders(
    viewerMode: string,
    host: DecoderExportHost
  ): Promise<boolean> {

    switch (this.normalizeViewerMode(viewerMode)) {
      case 'trackster-bin':
        return await host.exportSelectedTracksterBinFolders();
      case 'decoded-signals':
        return await host.exportSelectedDecodedSignalsFolders();
      case 'json':
        return await host.exportSelectedJsonFolders();
      case 'csv':
        return await host.exportSelectedCsvFolders();
      case 'hex-dump':
        return await host.exportSelectedHexDumpFolders();
      case 'vector-asc':
        return await host.exportSelectedVectorAscFolders();
      case 'candump':
        return await host.exportSelectedCandumpFolders();
      case 'blf':
        return await host.exportSelectedBlfFolders();
      case 'mf4':
        return await host.exportSelectedMf4Folders();
      case 'parquet':
        return await host.exportSelectedParquetFolders();
      case 'run-manifest':
        return await host.exportSelectedRunManifestFolders();
      default:
        throw new Error(
          `Folder export for viewer mode "${viewerMode}" is not integrated yet.`
        );
    }
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
