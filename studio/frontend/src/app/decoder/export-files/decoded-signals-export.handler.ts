import { HostMethodExportHandler } from './host-method-export.handler';

export class DecodedSignalsExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['decoded-signals'],
      'exportCurrentDecodedSignalsFile',
      'exportSelectedDecodedSignalsFiles',
      'exportSelectedDecodedSignalsFolders'
    );
  }
}
