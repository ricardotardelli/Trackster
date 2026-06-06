import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class HexDumpExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['hex-dump'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentHexDumpFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedHexDumpFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedHexDumpFolders();
  }
}
