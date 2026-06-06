import { HostMethodExportHandler } from './host-method-export.handler';

export class BlfExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['blf'],
      'exportCurrentBlfFile',
      'exportSelectedBlfFiles',
      'exportSelectedBlfFolders'
    );
  }
}
