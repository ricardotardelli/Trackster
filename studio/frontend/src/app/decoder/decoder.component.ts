import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { NestedTreeControl } from '@angular/cdk/tree';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule, MatTreeNestedDataSource } from '@angular/material/tree';

import { environment } from '../../environments/environment';
import { AuthService } from '../auth/auth.service';

interface S3TreeNode {
  name: string;
  key?: string;
  children?: S3TreeNode[];
}

interface RuntimeConfig {
  customerId?: string;
  clientId?: string;
  decoderApi?: {
    binFilesCatalogUrl?: string;
  };
}

@Component({
  selector: 'app-decoder',
  standalone: true,
  imports: [
    CommonModule,
    MatTreeModule,
    MatIconModule
  ],
  templateUrl: './decoder.component.html',
  styleUrl: './decoder.component.css'
})
export class DecoderComponent implements OnInit {
  selectedS3Node: S3TreeNode | null = null;

  readonly s3TreeControl = new NestedTreeControl<S3TreeNode>(
    node => node.children
  );

  readonly s3TreeDataSource = new MatTreeNestedDataSource<S3TreeNode>();

  constructor(private readonly authService: AuthService) {}

  async ngOnInit(): Promise<void> {
    await this.loadS3GeneratedFilesTree();
  }

  hasChild = (_: number, node: S3TreeNode): boolean => {
    return !!node.children && node.children.length > 0;
  };

  selectS3Node(node: S3TreeNode): void {
    this.selectedS3Node = node;
  }

  private async loadS3GeneratedFilesTree(): Promise<void> {
    const config = await this.loadRuntimeConfig();
    const clientId = this.resolveClientId(config);

    if (this.shouldUseLocalMock()) {
      this.setTreeData(this.buildLocalMockTree());
      return;
    }

    const catalogUrl = config.decoderApi?.binFilesCatalogUrl;

    if (!catalogUrl) {
      throw new Error(
        'Missing decoderApi.binFilesCatalogUrl in assets/config.json'
      );
    }

    const token = await this.authService.getIdToken();

    const response = await fetch(catalogUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`
      },
      body: JSON.stringify({
        clientId
      })
    });

    if (!response.ok) {
      const text = await response.text();

      throw new Error(
        `Failed to load generated files catalog. HTTP ${response.status}. ${text}`
      );
    }

    const payload = await response.json();

    const keys = this.extractS3Keys(payload);
    const tree = this.buildTreeFromS3Keys(keys, clientId);

    this.setTreeData(tree);
  }

  private buildTreeFromS3Keys( keys: string[], clientId: string ): S3TreeNode[] {

    const runs = new Map<string, S3TreeNode>();

    const prefix = `${clientId}/`;

    for (const rawKey of keys) {
      const key = rawKey.replace(/^generated-files\//, '');

      if (!key.startsWith(prefix)) {
        continue;
      }

      const relativeKey = key.slice(prefix.length);

      const parts = relativeKey
        .split('/')
        .filter(Boolean);

      if (parts.length < 2) {
        continue;
      }

      const runId = parts[0];
      const fileName = parts[parts.length - 1];

      if (!fileName.toLowerCase().endsWith('.bin')) {
        continue;
      }

      let runNode = runs.get(runId);

      if (!runNode) {
        runNode = {
          name: runId,
          key: `${clientId}/${runId}`,
          children: []
        };

        runs.set(runId, runNode);
      }

      runNode.children?.push({
        name: fileName,
        key: `${clientId}/${relativeKey}`
      });
    }

    const runNodes = [...runs.values()]
      .sort((a, b) => b.name.localeCompare(a.name));

    for (const runNode of runNodes) {
      runNode.children = [...(runNode.children ?? [])]
        .sort((a, b) => a.name.localeCompare(b.name));
    }

    return runNodes;
  }

  private extractS3Keys(payload: unknown): string[] {
    if (Array.isArray(payload)) {
      return payload
        .filter((item): item is string => typeof item === 'string');
    }

    if (!payload || typeof payload !== 'object') {
      return [];
    }

    const data = payload as {
      keys?: unknown;
      files?: unknown;
      objects?: unknown;
    };

    const source = data.keys ?? data.files ?? data.objects;

    if (!Array.isArray(source)) {
      return [];
    }

    return source
      .map(item => {
        if (typeof item === 'string') {
          return item;
        }

        if (item && typeof item === 'object' && 'key' in item) {
          const key = (item as { key?: unknown }).key;

          return typeof key === 'string' ? key : '';
        }

        return '';
      })
      .filter(key => key.length > 0);
  }

  private shouldShowFile(fileName: string): boolean {
    const normalized = fileName.toLowerCase();

    return (
      normalized.endsWith('.bin') ||
      normalized === 'run-manifest.json'
    );
  }

  private sortRunNodes(nodes: S3TreeNode[]): S3TreeNode[] {
    return [...nodes].sort((a, b) => b.name.localeCompare(a.name));
  }

  private sortFileNodes(nodes: S3TreeNode[]): S3TreeNode[] {
    return [...nodes].sort((a, b) => {
      const aIsManifest = a.name.toLowerCase() === 'run-manifest.json';
      const bIsManifest = b.name.toLowerCase() === 'run-manifest.json';

      if (aIsManifest && !bIsManifest) {
        return 1;
      }

      if (!aIsManifest && bIsManifest) {
        return -1;
      }

      return a.name.localeCompare(b.name);
    });
  }

  private setTreeData(data: S3TreeNode[]): void {
    this.s3TreeDataSource.data = data;
    this.s3TreeControl.dataNodes = data;
    this.expandInitialTree(data);
  }

  private expandInitialTree(data: S3TreeNode[]): void {
    for (const rootNode of data) {
      this.s3TreeControl.expand(rootNode);

      for (const runNode of rootNode.children ?? []) {
        this.s3TreeControl.expand(runNode);
      }
    }
  }

  private async loadRuntimeConfig(): Promise<RuntimeConfig> {
    const response = await fetch(`assets/config.json?t=${Date.now()}`);

    if (!response.ok) {
      throw new Error(`Failed to load assets/config.json. HTTP ${response.status}`);
    }

    return await response.json() as RuntimeConfig;
  }

  private resolveClientId(config: RuntimeConfig): string {
    const clientId =
      config.clientId ||
      config.customerId ||
      localStorage.getItem('clientId') ||
      localStorage.getItem('customerId') ||
      localStorage.getItem('tracksterClientId') ||
      localStorage.getItem('tracksterCustomerId') ||
      '00000000';

    if (!/^[a-zA-Z0-9]{8}$/.test(clientId)) {
      throw new Error(`Invalid clientId: ${clientId}`);
    }

    return clientId;
  }

  private shouldUseLocalMock(): boolean {
    const hostname = window.location.hostname;

    return (
      environment.disableAuth === true &&
      (hostname === 'localhost' || hostname === '127.0.0.1')
    );
  }

  private buildLocalMockTree(): S3TreeNode[] {
    return [
      {
        name: '20260508183000',
        key: '00000000/20260508183000',
        children: [
          {
            name: 'VIN000001KADUT.bin',
            key: '00000000/20260508183000/VIN000001KADUT.bin'
          },
          {
            name: 'VIN000002KADUT.bin',
            key: '00000000/20260508183000/VIN000002KADUT.bin'
          },
          {
            name: 'VIN000003KADUT.bin',
            key: '00000000/20260508183000/VIN000003KADUT.bin'
          },
          {
            name: 'VIN000004KADUT.bin',
            key: '00000000/20260508183000/VIN000004KADUT.bin'
          },
          {
            name: 'VIN000005KADUT.bin',
            key: '00000000/20260508183000/VIN000005KADUT.bin'
          },
          {
            name: 'VIN000006KADUT.bin',
            key: '00000000/20260508183000/VIN000006KADUT.bin'
          }
        ]
      },
      {
        name: '20260508191542',
        key: '00000000/20260508191542',
        children: [
          {
            name: 'VINTRACKSTER001.bin',
            key: '00000000/20260508191542/VINTRACKSTER001.bin'
          },
          {
            name: 'VINTRACKSTER002.bin',
            key: '00000000/20260508191542/VINTRACKSTER002.bin'
          },
          {
            name: 'VINTRACKSTER003.bin',
            key: '00000000/20260508191542/VINTRACKSTER003.bin'
          },
          {
            name: 'VINTRACKSTER004.bin',
            key: '00000000/20260508191542/VINTRACKSTER004.bin'
          }
        ]
      },
      {
        name: '20260509000211',
        key: '00000000/20260509000211',
        children: [
          {
            name: 'TESLAMODELY0001.bin',
            key: '00000000/20260509000211/TESLAMODELY0001.bin'
          },
          {
            name: 'TESLAMODELY0002.bin',
            key: '00000000/20260509000211/TESLAMODELY0002.bin'
          },
          {
            name: 'TESLAMODELY0003.bin',
            key: '00000000/20260509000211/TESLAMODELY0003.bin'
          },
          {
            name: 'TESLAMODELY0004.bin',
            key: '00000000/20260509000211/TESLAMODELY0004.bin'
          },
          {
            name: 'TESLAMODELY0005.bin',
            key: '00000000/20260509000211/TESLAMODELY0005.bin'
          },
          {
            name: 'TESLAMODELY0006.bin',
            key: '00000000/20260509000211/TESLAMODELY0006.bin'
          },
          {
            name: 'TESLAMODELY0007.bin',
            key: '00000000/20260509000211/TESLAMODELY0007.bin'
          }
        ]
      },
      {
        name: '20260509013055',
        key: '00000000/20260509013055',
        children: [
          {
            name: 'BMWI4SIM0001.bin',
            key: '00000000/20260509013055/BMWI4SIM0001.bin'
          },
          {
            name: 'BMWI4SIM0002.bin',
            key: '00000000/20260509013055/BMWI4SIM0002.bin'
          },
          {
            name: 'BMWI4SIM0003.bin',
            key: '00000000/20260509013055/BMWI4SIM0003.bin'
          }
        ]
      }
    ];
  }
}