import { HostMethodExportHandler } from './host-method-export.handler';

export class RunManifestExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['run-manifest', 'runmanifest'],
      'exportCurrentRunManifestFile',
      'exportSelectedRunManifestFiles',
      'exportSelectedRunManifestFolders'
    );
  }
}
