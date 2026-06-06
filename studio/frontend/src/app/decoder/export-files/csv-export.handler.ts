import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class CsvExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['csv'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentCsvFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedCsvFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedCsvFolders();
  }
}
