import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class JsonExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['json'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentJsonFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedJsonFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedJsonFolders();
  }
}
