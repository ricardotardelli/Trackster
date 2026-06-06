import type { DecoderExportHost } from '../decoder-export.service';
import type { DecoderExportHandler } from './decoder-export-handler';

type DecoderExportHostMethod = keyof DecoderExportHost;

export class HostMethodExportHandler implements DecoderExportHandler {
  constructor(
    readonly viewerModes: string[],
    private readonly currentMethod: DecoderExportHostMethod,
    private readonly selectedFilesMethod: DecoderExportHostMethod,
    private readonly selectedFoldersMethod: DecoderExportHostMethod
  ) {}

  async exportCurrent(host: DecoderExportHost): Promise<boolean> {
    return await this.executeHostMethod(host, this.currentMethod);
  }

  async exportSelectedFiles(host: DecoderExportHost): Promise<boolean> {
    return await this.executeHostMethod(host, this.selectedFilesMethod);
  }

  async exportSelectedFolders(host: DecoderExportHost): Promise<boolean> {
    return await this.executeHostMethod(host, this.selectedFoldersMethod);
  }

  private async executeHostMethod(
    host: DecoderExportHost,
    methodName: DecoderExportHostMethod
  ): Promise<boolean> {

    const method = host[methodName];

    if (typeof method !== 'function') {
      throw new Error(
        `Decoder export method "${String(methodName)}" is not available.`
      );
    }

    return await method.call(host);
  }
}
