import { MatIconModule } from '@angular/material/icon';
import { registerDbcLanguage } from './dbc-monaco-language';
import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, Inject, NgZone } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {
  DefaultMonacoLoader,
  NGX_MONACO_LOADER_PROVIDER,
  NgxMonacoEditorComponent,
  type EditorInitializedEvent
} from '@jean-merelis/ngx-monaco-editor';
import * as monaco from 'monaco-editor';
import { DialogShellComponent } from '../dialogshell/dialogshell.component';
import { DbcFindComponent } from './dbcfind.component';
import { DbcParser, type DbcFullReport } from './dbcparser';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { fetchAuthSession } from 'aws-amplify/auth';
import { firstValueFrom } from 'rxjs';
import { ViewChild, TemplateRef } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material/dialog';

const tracksterMonacoLoader =
  (globalThis as any).__tracksterMonacoLoader ??
  ((globalThis as any).__tracksterMonacoLoader = new DefaultMonacoLoader({
    paths: {
      vs: '/vs'
    }
  }));

type OriginalDbcStatus = 'pending' | 'validated' | 'rejected';

interface DbcApiConfig {
  uploadUrl: string;
}

interface AppConfig {
  dbcApi?: DbcApiConfig;
}

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
  providers: [{ provide: NGX_MONACO_LOADER_PROVIDER, useValue: tracksterMonacoLoader }],
  templateUrl: './dbceditor.component.html',
  styleUrl: './dbceditor.component.css'
})
export class DbcEditorComponent {
  dbcText = `BO_ 256 VEHICLE_SPEED: 8 Vector__XXX
 SG_ Speed : 0|16@1+ (0.01,0) [0|250] "km/h" Vector__XXX`;

  diagnosticsReport: DbcFullReport | null = null;
  lastDiagnosticsRunAt: string | null = null;
  originalText = this.dbcText;
  problemsCount = 0;
  findVisible = false;
  replaceVisible = false;
  initialFindQuery = '';
  isChangedSinceDiagnostics = false;
  issuesDropdownVisible = false;

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
    theme: 'dbcVsCodeLight',
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

  @ViewChild('confirmSaveDialog') confirmSaveDialog!: TemplateRef<any>;
  confirmDialogRef: any;


  private editorInstance: monaco.editor.IStandaloneCodeEditor | null = null;

  constructor(
    @Inject(MAT_DIALOG_DATA)
    public readonly data: {
      file: OriginalDbcFile;
      title: string;
      subtitle: string;
      content?: string;
      storageMode: 'local' | 'api';
      customerId?: string;
    },
    private readonly dialogRef: MatDialogRef<DbcEditorComponent>,
    private readonly ngZone: NgZone,
    private readonly cdr: ChangeDetectorRef,
    private readonly http: HttpClient,
    private readonly dialog: MatDialog
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

    this.editorInstance.onDidChangeModelContent(() => {
      if (!this.lastDiagnosticsRunAt) {
        return;
      }

      this.ngZone.run(() => {
        this.isChangedSinceDiagnostics = true;
        this.cdr.detectChanges();
      });
    });

    setTimeout(() => {
      this.editorInstance?.layout();
    }, 0);
  }

  get lastDiagnosticsLabel(): string {
    if (!this.lastDiagnosticsRunAt) {
      return 'Never';
    }

    const date = new Date(this.lastDiagnosticsRunAt);

    return date.toLocaleTimeString(); 
  }

  get diagnosticsState(): 'not-verified' | 'ok' | 'error' {
    if (!this.lastDiagnosticsRunAt || this.isChangedSinceDiagnostics) {
      return 'not-verified';
    }

    if (this.problemsCount > 0) {
      return 'error';
    }

    return 'ok';
  }

  get diagnosticsLabel(): string {
    switch (this.diagnosticsState) {
      case 'not-verified':
        return 'Not verified';
      case 'ok':
        return 'No issues';
      case 'error':
        return `Problems: ${this.problemsCount}`;
    }
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

  async save(): Promise<void> {
    const content = this.editorInstance?.getModel()?.getValue() ?? this.dbcText;

    const hasChanges = content !== this.originalText;
    if (!hasChanges) {
      this.dialogRef.close({
        saved: false
      });
      return;
    }
          
    const confirmed = await this.confirmSaveChanges();
    if (!confirmed) {
      return;
    }

    if (this.data.storageMode === 'local') {
      this.dialogRef.close({
        saved: true,
        content
      });
      return;
    }

    const config = await this.loadAppConfig();

    if (!config.dbcApi?.uploadUrl?.trim()) {
      throw new Error('dbcApi.uploadUrl missing or empty in config.json');
    }

    const headers = (await this.getAuthorizationHeaders()).set(
      'Content-Type',
      'application/json'
    );

    await firstValueFrom(
      this.http.post(
        config.dbcApi!.uploadUrl.trim(),
        {
          fileName: this.data.file.name,
          contentBase64: this.textToBase64(content)
        },
        {
          headers,
          params: {
            customerId: this.data.customerId ?? '00000000'
          }
        }
      )
    );

    this.dialogRef.close({
      saved: true,
      content
    });
  }

  cancel(): void {
    this.clearDiagnostics();
    this.dialogRef.close({
      saved: false
    });
  }

  revert(): void {
    this.dbcText = this.originalText;
    this.editorInstance?.setValue(this.dbcText);
    this.clearDiagnostics();
    this.editorInstance?.layout();
  }

  download(): void {
    const content = this.editorInstance?.getModel()?.getValue() ?? this.dbcText;
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
    const url = window.URL.createObjectURL(blob);

    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = this.data.file?.name ?? 'file.dbc';
    anchor.click();

    window.URL.revokeObjectURL(url);
  }

  runDiagnostics(): void {
    if (!this.editorInstance) {
      return;
    }

    const model = this.editorInstance.getModel();

    if (!model) {
      return;
    }

    const content = model.getValue();
    const report = DbcParser.parse(content);

    this.diagnosticsReport = report;
    this.problemsCount = report.errors.length;
    this.lastDiagnosticsRunAt = new Date().toISOString();
    this.isChangedSinceDiagnostics = false;

    this.applyDiagnosticsToEditor(model, report);

    if (report.errors.length > 0) {
      const firstError = report.errors[0];

      this.editorInstance.setPosition({
        lineNumber: firstError.line,
        column: firstError.column ?? 1
      });

      this.editorInstance.revealPositionInCenter({
        lineNumber: firstError.line,
        column: firstError.column ?? 1
      });

      this.editorInstance.focus();
    }
    
    this.issuesDropdownVisible = false;
  }

  private diagnosticsDecorations: string[] = [];

  private applyDiagnosticsToEditor( model: monaco.editor.ITextModel, report: DbcFullReport ): void {
    const markers: monaco.editor.IMarkerData[] = report.errors.map(error => ({
      startLineNumber: error.line,
      startColumn: error.column ?? 1,
      endLineNumber: error.line,
      endColumn: error.endColumn ?? 999,
      message: `[${error.type}] ${error.message}`,
      severity: this.mapSeverityToMonaco(error.severity)
    }));

    monaco.editor.setModelMarkers(model, 'dbc-diagnostics', markers);

    const decorations: monaco.editor.IModelDeltaDecoration[] = report.errors.map(
      error => ({
        range: new monaco.Range(error.line, 1, error.line, 1),
        options: {
          isWholeLine: true,
          className:
            error.severity === 'warning'
              ? 'dbc-warning-line'
              : 'dbc-error-line'
        }
      })
    );

    this.diagnosticsDecorations = this.editorInstance?.deltaDecorations(
      this.diagnosticsDecorations,
      decorations
    ) ?? [];
  }

  private mapSeverityToMonaco(
    severity: 'error' | 'warning'
  ): monaco.MarkerSeverity {
    switch (severity) {
      case 'warning':
        return monaco.MarkerSeverity.Warning;
      case 'error':
      default:
        return monaco.MarkerSeverity.Error;
    }
  }

  clearDiagnostics(): void {
    if (!this.editorInstance) {
      return;
    }

    const model = this.editorInstance.getModel();

    if (!model) {
      return;
    }

    this.diagnosticsReport = null;
    this.problemsCount = 0;
    this.lastDiagnosticsRunAt = null;
    this.lastDiagnosticsRunAt = null;
    this.isChangedSinceDiagnostics = false;

    monaco.editor.setModelMarkers(model, 'dbc-diagnostics', []);
  }

  undo(): void {
    this.editorInstance?.trigger('toolbar', 'undo', null);
  }

  redo(): void {
    this.editorInstance?.trigger('toolbar', 'redo', null);
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

  toggleIssuesDropdown(): void {
    if (this.diagnosticsState !== 'error') {
      return;
    }

    this.issuesDropdownVisible = !this.issuesDropdownVisible;
  }

  goToIssue(error: any): void {
    if (!this.editorInstance) {
      return;
    }

    this.editorInstance.setPosition({
      lineNumber: error.line,
      column: error.column ?? 1
    });

    this.editorInstance.revealPositionInCenter({
      lineNumber: error.line,
      column: error.column ?? 1
    });

    this.editorInstance.focus();

    this.issuesDropdownVisible = false;
  }

  private appConfig: AppConfig | null = null;

  private textToBase64(value: string): string {
    const bytes = new TextEncoder().encode(value);
    let binary = '';

    for (let index = 0; index < bytes.length; index += 1) {
      binary += String.fromCharCode(bytes[index]);
    }

    return btoa(binary);
  }

  private async loadAppConfig(): Promise<AppConfig> {
    if (this.appConfig) {
      return this.appConfig;
    }

    const response = await fetch('/assets/config.json', { cache: 'no-store' });

    if (!response.ok) {
      throw new Error('Unable to load assets/config.json');
    }

    this.appConfig = await response.json() as AppConfig;
    return this.appConfig;
  }

  private async getAuthorizationHeaders(): Promise<HttpHeaders> {
    const session = await fetchAuthSession();
    const accessToken = session.tokens?.accessToken?.toString();

    if (!accessToken) {
      throw new Error('Missing access token for authenticated API call.');
    }

    return new HttpHeaders({
      Authorization: `Bearer ${accessToken}`
    });
  }

  private async confirmSaveChanges(): Promise<boolean> {
    this.confirmDialogRef = this.dialog.open(this.confirmSaveDialog, {
      width: '420px',
      maxWidth: 'calc(100vw - 32px)',
      disableClose: true,
      autoFocus: false,
      restoreFocus: false,
      panelClass: 'trackster-confirm-dialog-panel'
    });

    return await firstValueFrom(this.confirmDialogRef.afterClosed());
  }

}