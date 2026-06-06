import { HostMethodExportHandler } from './host-method-export.handler';

export class JsonExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['json'],
      'exportCurrentJsonFile',
      'exportSelectedJsonFiles',
      'exportSelectedJsonFolders'
    );
  }
}
