import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class TracksterBinExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['trackster-bin'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentTracksterBinFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedTracksterBinFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedTracksterBinFolders();
  }
}
