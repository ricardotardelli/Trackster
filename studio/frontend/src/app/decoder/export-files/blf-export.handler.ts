import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class BlfExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['blf'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentBlfFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedBlfFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedBlfFolders();
  }
}
