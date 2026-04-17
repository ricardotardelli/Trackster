import { CommonModule } from '@angular/common';
import { Component, Inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import {
  DefaultMonacoLoader,
  NGX_MONACO_LOADER_PROVIDER,
  NgxMonacoEditorComponent
} from '@jean-merelis/ngx-monaco-editor';

import { DialogShellComponent } from '../dialogshell/dialogshell.component';

const monacoLoader = new DefaultMonacoLoader({
  paths: {
    vs: '/vs'
  }
});

type OriginalDbcStatus = 'pending' | 'validated' | 'rejected';

interface OriginalDbcFile {
  name: string;
  sizeBytes: number;
  lastModified: string;
  status: OriginalDbcStatus;
}

@Component({
  selector: 'app-dbc-editor-dialog',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    DialogShellComponent,
    NgxMonacoEditorComponent
  ],
  providers: [
    { provide: NGX_MONACO_LOADER_PROVIDER, useValue: monacoLoader }
  ],
  templateUrl: './dbceditor.component.html',
  styleUrl: './dbceditor.component.css'
})
export class DbcEditorComponent {
  dbcText = `BO_ 256 VEHICLE_SPEED: 8 Vector__XXX
SG_ Speed : 0|16@1+ (0.01,0) [0|250] "km/h" Vector__XXX`;

  editorOptions = {
    automaticLayout: true,
    minimap: { enabled: false },
    fontSize: 13,
    lineNumbers: 'on' as const,
    roundedSelection: false,
    scrollBeyondLastLine: false,
    wordWrap: 'off' as const,
    tabSize: 2,
    insertSpaces: true
  };

  constructor(
    @Inject(MAT_DIALOG_DATA)
    public readonly data: {
        file: OriginalDbcFile;
        title: string;
        subtitle: string;
        content?: string;
    },
    private readonly dialogRef: MatDialogRef<DbcEditorComponent>
    ) {
    this.dbcText =
      data.content ??
      `BO_ 256 VEHICLE_SPEED: 8 Vector__XXX
SG_ Speed : 0|16@1+ (0.01,0) [0|250] "km/h" Vector__XXX`;
  }

  save(): void {
    this.dialogRef.close({
      saved: true,
      content: this.dbcText
    });
  }

  cancel(): void {
    this.dialogRef.close();
  }

  private editorInstance: any;

  onEditorInit(event: any): void {
    this.editorInstance = event.editor;
    this.editorInstance.setValue(this.dbcText);

    setTimeout(() => {
        this.editorInstance.layout();
    }, 100);
  }

}