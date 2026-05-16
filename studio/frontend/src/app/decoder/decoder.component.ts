import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { NestedTreeControl } from '@angular/cdk/tree';
import { MatIconModule } from '@angular/material/icon';
import { MatTreeModule, MatTreeNestedDataSource } from '@angular/material/tree';
import { FormsModule } from '@angular/forms';
import { MatMenuModule } from '@angular/material/menu';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatCheckboxModule } from '@angular/material/checkbox';

import { environment } from '../../environments/environment';

import {
  ListObjectsV2Command,
  S3Client
} from '@aws-sdk/client-s3';

import { fetchAuthSession } from 'aws-amplify/auth';

import { TracksterBinViewerComponent } from './viewers/trackster-bin-viewer/trackster-bin-viewer.component';
import { DecodedSignalsViewerComponent } from './viewers/decodedsignals-viewer/decodedsignals-viewer.component';
import { MatDividerModule } from '@angular/material/divider';

import { JsonViewerComponent } from './viewers/json-viewer/json-viewer.component';
import { CsvViewerComponent } from './viewers/csv-viewer/csv-viewer.component';
import { HexDumpViewerComponent } from './viewers/hex-dump-viewer/hex-dump-viewer.component';
import { VectorAscViewerComponent } from './viewers/vector-asc-viewer/vector-asc-viewer.component';

export interface S3TreeNode {
  name: string;
  key: string;
  children?: S3TreeNode[];
}

interface RuntimeConfig {
  s3Default?: string;
  s3Region?: string;
  customerId?: string;
  clientId?: string;
}

@Component({
  selector: 'app-decoder',
  standalone: true,
  imports: [
    CommonModule,
    MatTreeModule,
    MatIconModule,
    FormsModule,
    MatMenuModule,
    MatFormFieldModule,
    MatSelectModule,
    MatCheckboxModule,
    TracksterBinViewerComponent,
    DecodedSignalsViewerComponent,
    MatDividerModule,
    JsonViewerComponent,
    CsvViewerComponent,
    HexDumpViewerComponent,
    VectorAscViewerComponent
  ],
  templateUrl: './decoder.component.html',
  styleUrl: './decoder.component.css'
})
export class DecoderComponent implements OnInit {

  selectedViewerMode = 'trackster-bin';

  isDecoderFilterOpen = false;

  isLoadingBinCatalog = false;

  selectedS3Key: string | null = null;

  selectedBinNode: S3TreeNode | null = null;

  selectedBinKeys: string[] = [];

  readonly s3TreeControl = new NestedTreeControl<S3TreeNode>(
    node => node.children ?? []
  );

  readonly s3TreeDataSource =
    new MatTreeNestedDataSource<S3TreeNode>();

  async ngOnInit(): Promise<void> {
    await this.loadS3GeneratedFilesTree();
  }

  hasChild = (_: number, node: S3TreeNode): boolean => {
    return !!node.children && node.children.length > 0;
  };

  async selectS3Node(node: S3TreeNode): Promise<void> {
    this.selectedS3Key = node.key;

    if (!this.isBinFile(node)) {
      return;
    }

    this.selectedBinNode = node;
  }

  isBinFile(node: S3TreeNode): boolean {
    return node.name.toLowerCase().endsWith('.bin');
  }

  isJsonFile(node: S3TreeNode): boolean {
    return node.name.toLowerCase().endsWith('.json');
  }

  isBinSelected(node: S3TreeNode): boolean {
    return this.selectedBinKeys.includes(node.key);
  }

  toggleBinSelection(node: S3TreeNode, checked: boolean): void {
    if (!this.isBinFile(node)) {
      return;
    }

    if (checked) {
      if (!this.selectedBinKeys.includes(node.key)) {
        this.selectedBinKeys = [
          ...this.selectedBinKeys,
          node.key
        ];
      }

      return;
    }

    this.selectedBinKeys = this.selectedBinKeys
      .filter(key => key !== node.key);
  }

  isFolderFullySelected(node: S3TreeNode): boolean {
    const binKeys = this.getBinKeysFromFolder(node);

    if (binKeys.length === 0) {
      return false;
    }

    return binKeys.every(key =>
      this.selectedBinKeys.includes(key)
    );
  }

  isFolderPartiallySelected(node: S3TreeNode): boolean {
    const binKeys = this.getBinKeysFromFolder(node);

    if (binKeys.length === 0) {
      return false;
    }

    const selectedCount = binKeys
      .filter(key => this.selectedBinKeys.includes(key))
      .length;

    return (
      selectedCount > 0 &&
      selectedCount < binKeys.length
    );
  }

  toggleFolderSelection(
    node: S3TreeNode,
    checked: boolean
  ): void {

    const folderBinKeys =
      this.getBinKeysFromFolder(node);

    if (folderBinKeys.length === 0) {
      return;
    }

    if (checked) {
      const mergedKeys = new Set<string>([
        ...this.selectedBinKeys,
        ...folderBinKeys
      ]);

      this.selectedBinKeys = [...mergedKeys];

      return;
    }

    const folderKeys = new Set<string>(
      folderBinKeys
    );

    this.selectedBinKeys = this.selectedBinKeys
      .filter(key => !folderKeys.has(key));
  }

  private getBinKeysFromFolder(
    node: S3TreeNode
  ): string[] {

    const result: string[] = [];

    const walk = (
      currentNode: S3TreeNode
    ): void => {

      if (this.isBinFile(currentNode)) {
        result.push(currentNode.key);
        return;
      }

      for (
        const child of currentNode.children ?? []
      ) {
        walk(child);
      }
    };

    walk(node);

    return result;
  }

  private async loadS3GeneratedFilesTree(): Promise<void> {
    this.isLoadingBinCatalog = true;

    try {

      const config =
        await this.loadRuntimeConfig();

      const clientId =
        this.resolveClientId(config);

      if (this.shouldUseLocalMock()) {

        this.setTreeData(
          this.buildLocalMockTree()
        );

        return;
      }

      const bucket =
        config.s3Default?.trim();

      if (!bucket) {
        throw new Error(
          'Missing s3Default in assets/config.json'
        );
      }

      const prefix = `${clientId}/`;

      const keys =
        await this.listS3KeysFromBucket(
          bucket,
          prefix
        );

      const tree =
        this.buildTreeFromS3Keys(
          keys,
          clientId
        );

      this.setTreeData(tree);

    } finally {
      this.isLoadingBinCatalog = false;
    }
  }

  private setTreeData(data: S3TreeNode[]): void {
    this.s3TreeControl.expansionModel.clear();

    this.s3TreeDataSource.data = data;

    this.s3TreeControl.dataNodes = data;

    this.selectedBinKeys = this.selectedBinKeys.filter(key =>
      this.treeContainsKey(data, key)
    );

    const firstFolderNode = data.find(node =>
      node.children && node.children.length > 0
    );

    if (firstFolderNode) {
      this.s3TreeControl.expand(firstFolderNode);
    }

    const firstBinNode = this.findFirstBinNode(data);

    if (firstBinNode && !this.selectedBinNode) {
      this.selectedBinNode = firstBinNode;
      this.selectedS3Key = firstBinNode.key;
    }
  }

  private treeContainsKey(
    nodes: S3TreeNode[],
    key: string
  ): boolean {

    for (const node of nodes) {

      if (node.key === key) {
        return true;
      }

      if (
        node.children &&
        this.treeContainsKey(
          node.children,
          key
        )
      ) {
        return true;
      }
    }

    return false;
  }

  private buildTreeFromS3Keys(
    keys: string[],
    clientId: string
  ): S3TreeNode[] {

    const runs =
      new Map<string, S3TreeNode>();

    const prefix = `${clientId}/`;

    for (const rawKey of keys) {

      const key =
        rawKey.replace(
          /^generated-files\//,
          ''
        );

      if (!key.startsWith(prefix)) {
        continue;
      }

      const relativeKey =
        key.slice(prefix.length);

      const parts = relativeKey
        .split('/')
        .filter(Boolean);

      if (parts.length < 2) {
        continue;
      }

      const runId = parts[0];

      const fileName =
        parts[parts.length - 1];

      if (
        !fileName.toLowerCase().endsWith('.bin')
      ) {
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
      .sort((a, b) =>
        b.name.localeCompare(a.name)
      );

    for (const runNode of runNodes) {

      runNode.children =
        [...(runNode.children ?? [])]
          .sort((a, b) =>
            a.name.localeCompare(b.name)
          );
    }

    return runNodes;
  }

  private async loadRuntimeConfig():
    Promise<RuntimeConfig> {

    const response = await fetch(
      `assets/config.json?t=${Date.now()}`
    );

    if (!response.ok) {
      throw new Error(
        `Failed to load assets/config.json. HTTP ${response.status}`
      );
    }

    return await response.json();
  }

  private resolveClientId(
    config: RuntimeConfig
  ): string {

    const clientId =
      config.clientId ||
      config.customerId ||
      localStorage.getItem('clientId') ||
      localStorage.getItem('customerId') ||
      '00000000';

    if (
      !/^[a-zA-Z0-9]{8}$/.test(clientId)
    ) {
      throw new Error(
        `Invalid clientId: ${clientId}`
      );
    }

    return clientId;
  }

  private shouldUseLocalMock(): boolean {

    const hostname =
      window.location.hostname;

    return (
      environment.disableAuth === true &&
      (
        hostname === 'localhost' ||
        hostname === '127.0.0.1'
      )
    );
  }

  private buildLocalMockTree():
    S3TreeNode[] {

    return [
      {
        name: '20260508183000',
        key: '00000000/20260508183000',

        children: [
          {
            name: 'VINKDT000001KADUT.bin',
            key: '00000000/20260508183000/VIN000001KADUT.bin'
          }
        ]
      }
    ];
  }

  private async getS3Client():
    Promise<S3Client> {

    const config =
      await this.loadRuntimeConfig();

    const region =
      config.s3Region?.trim() ||
      'us-east-1';

    const session =
      await fetchAuthSession();

    if (!session.credentials) {
      throw new Error(
        'Cognito credentials unavailable.'
      );
    }

    return new S3Client({
      region,
      credentials: session.credentials
    });
  }

  private async listS3KeysFromBucket(
    bucket: string,
    prefix: string
  ): Promise<string[]> {

    const s3Client =
      await this.getS3Client();

    const keys: string[] = [];

    let continuationToken:
      string | undefined;

    do {

      const response =
        await s3Client.send(
          new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix,
            ContinuationToken:
              continuationToken
          })
        );

      for (
        const item of response.Contents ?? []
      ) {

        if (item.Key) {
          keys.push(item.Key);
        }
      }

      continuationToken =
        response.NextContinuationToken;

    } while (continuationToken);

    return keys;
  }

  private findFirstBinNode(nodes: S3TreeNode[]): S3TreeNode | null {
    for (const node of nodes) {
      if (this.isBinFile(node)) {
        return node;
      }

      const childMatch = this.findFirstBinNode(node.children ?? []);

      if (childMatch) {
        return childMatch;
      }
    }

    return null;
  }
}