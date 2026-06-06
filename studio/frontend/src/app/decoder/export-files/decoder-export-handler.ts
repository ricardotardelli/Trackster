import type { DecoderExportHost } from '../decoder-export.service';

export type DecoderExportScope =
  | 'current'
  | 'selected-files'
  | 'selected-folders';

export interface DecoderExportHandler {
  readonly viewerModes: string[];

  exportCurrent(host: DecoderExportHost): Promise<boolean>;

  exportSelectedFiles(host: DecoderExportHost): Promise<boolean>;

  exportSelectedFolders(host: DecoderExportHost): Promise<boolean>;
}
