import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class RunManifestExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['run-manifest', 'runmanifest'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentRunManifestFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedRunManifestFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedRunManifestFolders();
  }
}
