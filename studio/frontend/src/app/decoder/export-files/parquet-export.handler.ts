import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

export class ParquetExportHandler implements DecoderExportHandler {
  readonly viewerModes = ['parquet'];

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await host.exportCurrentParquetFile();
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedParquetFiles();
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await host.exportSelectedParquetFolders();
  }
}
