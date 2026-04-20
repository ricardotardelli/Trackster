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
import { DbcFindComponent } from './dbcfind.component';
import { ChangeDetectorRef, NgZone } from '@angular/core';

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
    MatIconModule,
    DbcFindComponent
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
  findVisible = false;
  replaceVisible = false;
  initialFindQuery = '';

  editorOptions: monaco.editor.IStandaloneEditorConstructionOptions = {
    automaticLayout: true,
    fixedOverflowWidgets: true,
    minimap: { enabled: false },
    fontSize: 14,
    lineHeight: 20,
    lineNumbers: 'on',
    lineNumbersMinChars: 3,
    glyphMargin: true,
    folding: true,
    lineDecorationsWidth: 8,
    roundedSelection: false,
    scrollBeyondLastLine: false,
    wordWrap: 'off',
    tabSize: 4,
    insertSpaces: true,
    language: 'dbc',
    theme: 'dbcLight',
    overviewRulerBorder: true,
    hideCursorInOverviewRuler: true,
    padding: {
      top: 10,
      bottom: 10
    },
    guides: {
      indentation: true,
      highlightActiveIndentation: true,
      bracketPairs: true,
      bracketPairsHorizontal: true
    },
    bracketPairColorization: {
      enabled: true
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
    private readonly dialogRef: MatDialogRef<DbcEditorComponent>,
    private readonly ngZone: NgZone,
    private readonly cdr: ChangeDetectorRef
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

    this.editorInstance.addCommand(
      monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyF,
      () => {
        this.toggleFind(false);
      }
    );

    this.editorInstance.addCommand(
      monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyH,
      () => {
        this.toggleFind(true);
      }
    );

    this.editorInstance.onKeyDown((keyboardEvent) => {
      if (keyboardEvent.keyCode === monaco.KeyCode.Escape && this.findVisible) {
        keyboardEvent.preventDefault();
        keyboardEvent.stopPropagation();
        this.closeFind();
      }
    });

    setTimeout(() => {
      this.editorInstance?.layout();
    }, 0);
  }

  get editor(): monaco.editor.IStandaloneCodeEditor | null {
    return this.editorInstance;
  }

  toggleFind(showReplace: boolean): void {
    this.ngZone.run(() => {
      if (this.findVisible) {
        if (showReplace && !this.replaceVisible) {
          this.openFind(true);
          return;
        }

        this.closeFind();
        return;
      }

      this.openFind(showReplace);
    });
  }

  openFind(showReplace: boolean): void {
    const selectedText = this.getSelectedEditorText();

    this.initialFindQuery = selectedText;
    this.replaceVisible = showReplace;
    this.findVisible = true;
    this.cdr.detectChanges();
  }

  private getSelectedEditorText(): string {
    if (!this.editorInstance) {
      return '';
    }

    const selection = this.editorInstance.getSelection();
    const model = this.editorInstance.getModel();

    if (!selection || !model || selection.isEmpty()) {
      return '';
    }

    const selectedText = model.getValueInRange(selection);

    if (!selectedText || selectedText.includes('\n')) {
      return '';
    }

    return selectedText;
  }

  closeFind(): void {
    this.ngZone.run(() => {
      this.findVisible = false;
      this.replaceVisible = false;
      this.cdr.detectChanges();
    });

    this.editorInstance?.focus();
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

    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = this.data.file?.name ?? 'file.dbc';
    anchor.click();

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
    this.toggleFind(false);
  }

  goToLine(): void {
    if (!this.editorInstance) {
      return;
    }

    this.editorInstance.focus();
    this.editorInstance.trigger('toolbar', 'editor.action.gotoLine', null);
  }

  toggleWrap(): void {
    const nextWrap: monaco.editor.IEditorOptions['wordWrap'] =
      this.editorOptions.wordWrap === 'off' ? 'on' : 'off';

    this.editorOptions.wordWrap = nextWrap;
    this.editorInstance?.updateOptions({ wordWrap: nextWrap });
    this.editorInstance?.layout();
  }
}