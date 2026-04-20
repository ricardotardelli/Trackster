import {
  AfterViewInit,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
  ViewChild
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import * as monaco from 'monaco-editor';

interface FindMatchItem {
  range: monaco.Range;
}

@Component({
  selector: 'app-dbcfind',
  standalone: true,
  imports: [CommonModule, FormsModule, MatIconModule],
  templateUrl: './dbcfind.component.html',
  styleUrl: './dbcfind.component.css'
})
export class DbcFindComponent implements AfterViewInit, OnChanges {
  @Input() editor: monaco.editor.IStandaloneCodeEditor | null = null;
  @Input() replaceVisible = false;

  @Output() closed = new EventEmitter<void>();

  @ViewChild('findInput')
  private findInputRef?: ElementRef<HTMLInputElement>;

  @ViewChild('replaceInput')
  private replaceInputRef?: ElementRef<HTMLInputElement>;

  @Input() initialQuery = '';

  findState = {
    query: '',
    replaceText: '',
    matchCase: false,
    wholeWord: false,
    regex: false,
    regexError: false,
    matches: [] as FindMatchItem[],
    currentIndex: -1
  };

  private allMatchDecorationIds: string[] = [];
  private currentMatchDecorationIds: string[] = [];
  private modelContentListener?: monaco.IDisposable;

  ngAfterViewInit(): void {
    this.bindEditorListeners();
    this.findState.query = this.initialQuery ?? '';
    this.refreshMatches(true);

    setTimeout(() => {
      this.findInputRef?.nativeElement.focus();
      this.findInputRef?.nativeElement.select();
    }, 0);
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['editor'] && !changes['editor'].firstChange) {
      this.disposeEditorListeners();
      this.bindEditorListeners();
      this.refreshMatches(true);
    }

    if (changes['replaceVisible'] && !changes['replaceVisible'].firstChange) {
      if (this.replaceVisible) {
        setTimeout(() => {
          this.replaceInputRef?.nativeElement.focus();
          this.replaceInputRef?.nativeElement.select();
        }, 0);
      } else {
        setTimeout(() => {
          this.findInputRef?.nativeElement.focus();
        }, 0);
      }
    }
  }

  close(): void {
    this.clearFindDecorations();
    this.disposeEditorListeners();
    this.closed.emit();
  }

  onFindQueryChange(): void {
    this.refreshMatches(true);
  }

  onFindKeydown(event: KeyboardEvent): void {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      this.close();
      return;
    }

    if (event.key === 'Enter') {
      event.preventDefault();

      if (event.shiftKey) {
        this.findPrevious();
        return;
      }

      this.findNext();
    }
  }

  onReplaceKeydown(event: KeyboardEvent): void {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      this.close();
      return;
    }

    if (event.key === 'Enter') {
      event.preventDefault();

      if (event.ctrlKey || event.metaKey) {
        this.replaceAll();
        return;
      }

      this.replaceCurrent();
    }
  }

  toggleReplace(): void {
    this.replaceVisible = !this.replaceVisible;

    if (this.replaceVisible) {
      setTimeout(() => {
        this.replaceInputRef?.nativeElement.focus();
        this.replaceInputRef?.nativeElement.select();
      }, 0);
      return;
    }

    setTimeout(() => {
      this.findInputRef?.nativeElement.focus();
    }, 0);
  }

  toggleMatchCase(): void {
    this.findState.matchCase = !this.findState.matchCase;
    this.refreshMatches(true);
  }

  toggleWholeWord(): void {
    this.findState.wholeWord = !this.findState.wholeWord;
    this.refreshMatches(true);
  }

  toggleRegex(): void {
    this.findState.regex = !this.findState.regex;
    this.refreshMatches(true);
  }

  findNext(): void {
    if (!this.findState.matches.length) {
      return;
    }

    const nextIndex =
      this.findState.currentIndex < this.findState.matches.length - 1
        ? this.findState.currentIndex + 1
        : 0;

    this.selectMatch(nextIndex);
  }

  findPrevious(): void {
    if (!this.findState.matches.length) {
      return;
    }

    const previousIndex =
      this.findState.currentIndex > 0
        ? this.findState.currentIndex - 1
        : this.findState.matches.length - 1;

    this.selectMatch(previousIndex);
  }

  replaceCurrent(): void {
    if (!this.editor || !this.hasMatches()) {
      return;
    }

    const model = this.editor.getModel();
    if (!model) {
      return;
    }

    const currentMatch = this.findState.matches[this.findState.currentIndex];
    if (!currentMatch) {
      return;
    }

    const currentText = model.getValueInRange(currentMatch.range);
    const replacement = this.buildReplacementText(currentText);

    this.editor.executeEdits('custom-find-replace-one', [
      {
        range: currentMatch.range,
        text: replacement,
        forceMoveMarkers: true
      }
    ]);

    this.refreshMatches(false);
  }

  replaceAll(): void {
    if (!this.editor || !this.findState.query) {
      return;
    }

    const model = this.editor.getModel();
    if (!model) {
      return;
    }

    const fullText = model.getValue();
    const searchRegex = this.buildSearchRegex(true);

    if (!searchRegex) {
      return;
    }

    const nextText = fullText.replace(searchRegex, (...args: unknown[]) => {
      const matchText = String(args[0] ?? '');
      return this.buildReplacementText(matchText);
    });

    this.editor.executeEdits('custom-find-replace-all', [
      {
        range: model.getFullModelRange(),
        text: nextText,
        forceMoveMarkers: true
      }
    ]);

    this.refreshMatches(true);
  }

  hasMatches(): boolean {
    return this.findState.matches.length > 0;
  }

  getMatchesLabel(): string {
    if (this.findState.regexError) {
      return 'Invalid';
    }

    if (!this.findState.query) {
      return '0 / 0';
    }

    if (!this.findState.matches.length) {
      return '0 / 0';
    }

    return `${this.findState.currentIndex + 1} / ${this.findState.matches.length}`;
  }

  private bindEditorListeners(): void {
    if (!this.editor) {
      return;
    }

    this.modelContentListener = this.editor.onDidChangeModelContent(() => {
      this.refreshMatches(false);
    });
  }

  private disposeEditorListeners(): void {
    this.modelContentListener?.dispose();
    this.modelContentListener = undefined;
  }

  private refreshMatches(resetIndex: boolean): void {
    if (!this.editor) {
      return;
    }

    const model = this.editor.getModel();
    if (!model) {
      return;
    }

    const query = this.findState.query;

    if (!query) {
      this.findState.regexError = false;
      this.findState.matches = [];
      this.findState.currentIndex = -1;
      this.clearFindDecorations();
      return;
    }

    const searchSpec = this.buildSearchSpec();
    if (!searchSpec) {
      this.findState.regexError = true;
      this.findState.matches = [];
      this.findState.currentIndex = -1;
      this.clearFindDecorations();
      return;
    }

    try {
      const matches = model.findMatches(
        searchSpec.searchString,
        false,
        searchSpec.isRegex,
        this.findState.matchCase,
        null,
        false,
        9999
      );

      this.findState.regexError = false;
      this.findState.matches = matches.map((match) => ({
        range: new monaco.Range(
          match.range.startLineNumber,
          match.range.startColumn,
          match.range.endLineNumber,
          match.range.endColumn
        )
      }));

      if (!this.findState.matches.length) {
        this.findState.currentIndex = -1;
        this.clearFindDecorations();
        return;
      }

      if (resetIndex || this.findState.currentIndex < 0) {
        this.findState.currentIndex = this.getBestStartingIndex();
      } else if (this.findState.currentIndex >= this.findState.matches.length) {
        this.findState.currentIndex = this.findState.matches.length - 1;
      }

      this.applyFindDecorations();
      this.revealCurrentMatch();
    } catch {
      this.findState.regexError = true;
      this.findState.matches = [];
      this.findState.currentIndex = -1;
      this.clearFindDecorations();
    }
  }

  private getBestStartingIndex(): number {
    if (!this.editor || !this.findState.matches.length) {
      return 0;
    }

    const position = this.editor.getPosition();
    if (!position) {
      return 0;
    }

    const currentOffset = this.editor.getModel()?.getOffsetAt(position) ?? 0;

    const foundIndex = this.findState.matches.findIndex((item) => {
      const matchOffset =
        this.editor?.getModel()?.getOffsetAt(item.range.getStartPosition()) ?? 0;
      return matchOffset >= currentOffset;
    });

    return foundIndex >= 0 ? foundIndex : 0;
  }

  private selectMatch(index: number): void {
    if (!this.findState.matches[index] || !this.editor) {
      return;
    }

    this.findState.currentIndex = index;
    this.applyFindDecorations();
    this.revealCurrentMatch();
  }

  private revealCurrentMatch(): void {
    if (!this.editor || this.findState.currentIndex < 0) {
      return;
    }

    const currentMatch = this.findState.matches[this.findState.currentIndex];
    if (!currentMatch) {
      return;
    }

    this.editor.setSelection(currentMatch.range);
    this.editor.revealRangeInCenter(currentMatch.range);
  }

  private applyFindDecorations(): void {
    if (!this.editor) {
      return;
    }

    const allRanges = this.findState.matches.map((item) => ({
      range: item.range,
      options: {
        inlineClassName: 'dbc-find-match'
      }
    }));

    this.allMatchDecorationIds = this.editor.deltaDecorations(
      this.allMatchDecorationIds,
      allRanges
    );

    const currentMatch = this.findState.matches[this.findState.currentIndex];

    this.currentMatchDecorationIds = this.editor.deltaDecorations(
      this.currentMatchDecorationIds,
      currentMatch
        ? [
            {
              range: currentMatch.range,
              options: {
                inlineClassName: 'dbc-find-match-current'
              }
            }
          ]
        : []
    );
  }

  private clearFindDecorations(): void {
    if (!this.editor) {
      return;
    }

    this.allMatchDecorationIds = this.editor.deltaDecorations(
      this.allMatchDecorationIds,
      []
    );

    this.currentMatchDecorationIds = this.editor.deltaDecorations(
      this.currentMatchDecorationIds,
      []
    );
  }

  private buildSearchSpec():
    | { searchString: string; isRegex: boolean }
    | null {
    const query = this.findState.query;
    if (!query) {
      return null;
    }

    if (this.findState.regex) {
      const regexText = this.findState.wholeWord
        ? `\\b(?:${query})\\b`
        : query;

      return {
        searchString: regexText,
        isRegex: true
      };
    }

    if (this.findState.wholeWord) {
      return {
        searchString: `\\b${this.escapeRegExp(query)}\\b`,
        isRegex: true
      };
    }

    return {
      searchString: query,
      isRegex: false
    };
  }

  private buildSearchRegex(global: boolean): RegExp | null {
    const query = this.findState.query;
    if (!query) {
      return null;
    }

    const flags = `${global ? 'g' : ''}${this.findState.matchCase ? '' : 'i'}`;

    try {
      if (this.findState.regex) {
        const regexText = this.findState.wholeWord
          ? `\\b(?:${query})\\b`
          : query;

        return new RegExp(regexText, flags);
      }

      const plainText = this.escapeRegExp(query);
      const wrappedText = this.findState.wholeWord
        ? `\\b${plainText}\\b`
        : plainText;

      return new RegExp(wrappedText, flags);
    } catch {
      return null;
    }
  }

  private buildReplacementText(currentText: string): string {
    const searchRegex = this.buildSearchRegex(false);
    if (!searchRegex) {
      return this.findState.replaceText;
    }

    return currentText.replace(searchRegex, this.findState.replaceText);
  }

  private escapeRegExp(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}