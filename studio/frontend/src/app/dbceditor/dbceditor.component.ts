import { MatIconModule } from '@angular/material/icon';
import { registerDbcLanguage } from './dbc-monaco-language';
import { CommonModule } from '@angular/common';
import { Component, Inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import {
  DefaultMonacoLoader,
  NGX_MONACO_LOADER_PROVIDER,
  NgxMonacoEditorComponent,
  type EditorInitializedEvent
} from '@jean-merelis/ngx-monaco-editor';
import * as monaco from 'monaco-editor';

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
  selector: 'app-dbceditor',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    DialogShellComponent,
    NgxMonacoEditorComponent,
    MatIconModule 
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

  originalText = this.dbcText;
  problemsCount = 0;

  editorOptions: monaco.editor.IStandaloneEditorConstructionOptions = {
    automaticLayout: true,
    minimap: { enabled: false },
    fontSize: 15,
    lineHeight: 20,
    lineNumbers: 'on',
    lineNumbersMinChars: 2,
    glyphMargin: false,
    folding: false,
    lineDecorationsWidth: 8,
    roundedSelection: false,
    scrollBeyondLastLine: false,
    wordWrap: 'off',
    tabSize: 2,
    insertSpaces: true,
    language: 'dbc',
    theme: 'dbcLight',
    overviewRulerBorder: false,
    hideCursorInOverviewRuler: true,
    padding: {
      top: 10,
      bottom: 10
    },
    scrollbar: {
      vertical: 'auto',
      horizontal: 'auto',
      verticalScrollbarSize: 8,
      horizontalScrollbarSize: 8,
      alwaysConsumeMouseWheel: false
    }
  };

  private editorInstance: monaco.editor.IStandaloneCodeEditor | null = null;

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
    this.dbcText = data.content ?? this.dbcText;
    this.originalText = this.dbcText;
  }

  onEditorInit(event: EditorInitializedEvent): void {
    this.editorInstance = event.editor;

    registerDbcLanguage(event.monaco);

    const model = this.editorInstance.getModel();
    if (model) {
      event.monaco.editor.setModelLanguage(model, 'dbc');
    }

    event.monaco.editor.setTheme('dbcVsCodeLight');
    //event.monaco.editor.setTheme('dbcVsCodeDark');
    //event.monaco.editor.setTheme('dbcLight');

    console.log('LANG:', this.editorInstance.getModel()?.getLanguageId());

    setTimeout(() => {
      this.editorInstance?.layout();
      this.editorInstance?.focus();
    }, 0);
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

  revert(): void {
    this.dbcText = this.originalText;
    this.editorInstance?.setValue(this.dbcText);
    this.editorInstance?.layout();
  }

  download(): void {
    const blob = new Blob([this.dbcText], { type: 'text/plain;charset=utf-8' });
    const url = window.URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = this.data.file?.name ?? 'file.dbc';
    a.click();

    window.URL.revokeObjectURL(url);
  }

  validate(): void {
    const content = this.editorInstance?.getValue() ?? this.dbcText;
    console.log('Validate DBC content length:', content.length);
    this.problemsCount = 0;
  }

  format(): void {
    this.editorInstance?.getAction('editor.action.formatDocument')?.run();
  }

  find(): void {
    this.editorInstance?.getAction('actions.find')?.run();
  }

  goToLine(): void {
    this.editorInstance?.getAction('editor.action.gotoLine')?.run();
  }

  toggleWrap(): void {
    const nextWrap: monaco.editor.IEditorOptions['wordWrap'] =
      this.editorOptions.wordWrap === 'off' ? 'on' : 'off';

    this.editorOptions.wordWrap = nextWrap;
    this.editorInstance?.updateOptions({ wordWrap: nextWrap });
    this.editorInstance?.layout();
  }
}