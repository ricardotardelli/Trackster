import { HostMethodExportHandler } from './host-method-export.handler';

export class HexDumpExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['hex-dump'],
      'exportCurrentHexDumpFile',
      'exportSelectedHexDumpFiles',
      'exportSelectedHexDumpFolders'
    );
  }
}
