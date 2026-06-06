import { HostMethodExportHandler } from './host-method-export.handler';

export class CandumpExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['candump'],
      'exportCurrentCandumpFile',
      'exportSelectedCandumpFiles',
      'exportSelectedCandumpFolders'
    );
  }
}
