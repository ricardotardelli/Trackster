import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class DecodedSignalsExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['decoded-signals'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentDecodedSignalsFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedDecodedSignalsFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedDecodedSignalsFolders();
  }
}
