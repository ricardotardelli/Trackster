import { HostMethodExportHandler } from './host-method-export.handler';

export class CsvExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['csv'],
      'exportCurrentCsvFile',
      'exportSelectedCsvFiles',
      'exportSelectedCsvFolders'
    );
  }
}
