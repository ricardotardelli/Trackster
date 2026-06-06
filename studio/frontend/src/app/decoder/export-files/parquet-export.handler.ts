import { HostMethodExportHandler } from './host-method-export.handler';

export class ParquetExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['parquet'],
      'exportCurrentParquetFile',
      'exportSelectedParquetFiles',
      'exportSelectedParquetFolders'
    );
  }
}
