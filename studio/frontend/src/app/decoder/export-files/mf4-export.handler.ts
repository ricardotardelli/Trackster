import { HostMethodExportHandler } from './host-method-export.handler';

export class Mf4ExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['mf4'],
      'exportCurrentMf4File',
      'exportSelectedMf4Files',
      'exportSelectedMf4Folders'
    );
  }
}
