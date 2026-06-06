import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class Mf4ExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['mf4'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentMf4File();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedMf4Files();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedMf4Folders();
  }
}
