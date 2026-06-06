import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class VectorAscExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['vector-asc'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentVectorAscFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedVectorAscFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedVectorAscFolders();
  }
}
