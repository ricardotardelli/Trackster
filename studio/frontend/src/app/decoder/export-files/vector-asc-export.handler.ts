import { HostMethodExportHandler } from './host-method-export.handler';

export class VectorAscExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['vector-asc'],
      'exportCurrentVectorAscFile',
      'exportSelectedVectorAscFiles',
      'exportSelectedVectorAscFolders'
    );
  }
}
