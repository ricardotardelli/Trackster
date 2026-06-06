import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class CandumpExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['candump'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentCandumpFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedCandumpFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedCandumpFolders();
  }
}
