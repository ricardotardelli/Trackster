import { HostMethodExportHandler } from './host-method-export.handler';

export class TracksterBinExportHandler extends HostMethodExportHandler {
  constructor() {
    super(
      ['trackster-bin'],
      'exportCurrentTracksterBinFile',
      'exportSelectedTracksterBinFiles',
      'exportSelectedTracksterBinFolders'
    );
  }
}
