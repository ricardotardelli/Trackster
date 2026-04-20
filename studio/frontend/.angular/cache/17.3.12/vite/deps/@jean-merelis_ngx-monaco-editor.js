import {
  NG_VALUE_ACCESSOR
} from "./chunk-52R5GEI6.js";
import {
  DOCUMENT,
  NgClass,
  NgStyle
} from "./chunk-DYM76RFS.js";
import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  InjectionToken,
  InputFlags,
  NgZone,
  ViewEncapsulation$1,
  forwardRef,
  inject,
  input,
  model,
  output,
  setClassMetadata,
  signal,
  viewChild,
  ɵɵNgOnChangesFeature,
  ɵɵProvidersFeature,
  ɵɵStandaloneFeature,
  ɵɵadvance,
  ɵɵclassProp,
  ɵɵconditional,
  ɵɵdefineComponent,
  ɵɵelementEnd,
  ɵɵelementStart,
  ɵɵlistener,
  ɵɵproperty,
  ɵɵqueryAdvance,
  ɵɵtemplate,
  ɵɵtext,
  ɵɵviewQuerySignal
} from "./chunk-V3NIGZ5G.js";
import "./chunk-ARYSD6E7.js";
import "./chunk-24ZYNOED.js";
import "./chunk-MF5NBIAP.js";

// node_modules/@jean-merelis/ngx-monaco-editor/fesm2022/jean-merelis-ngx-monaco-editor.mjs
var _c0 = ["editorContainer"];
function NgxMonacoEditorComponent_Conditional_2_Template(rf, ctx) {
  if (rf & 1) {
    ɵɵtext(0, " Monaco was not loaded correctly. ");
  }
}
var NGX_MONACO_LOADER_PROVIDER = new InjectionToken("NGX_MONACO_LOADER_PROVIDER");
var DefaultMonacoLoader = class {
  constructor(config = {
    paths: {
      vs: "vs"
    }
  }, loadOnCreate = true) {
    if (loadOnCreate) {
      this.createPromise();
    }
  }
  monacoLoaded() {
    if (!this.monacoPromise) {
      this.createPromise();
    }
    return this.monacoPromise;
  }
  createPromise() {
    this.monacoPromise = new Promise((resolve, reject) => {
      if (typeof window.monaco === "object") {
        resolve(window.monaco);
        return;
      }
      const loadLoader = () => {
        const loaderScript = document.createElement("script");
        loaderScript.onerror = reject;
        loaderScript.onload = () => {
          window.require.config(this.config);
          window.require(["vs/editor/editor.main"], function(monaco) {
            resolve(monaco);
          }, function(error) {
            reject(error);
          });
        };
        loaderScript.type = "text/javascript";
        loaderScript.src = this.config?.paths?.vs ? `${this.config.paths.vs}/loader.js` : `/vs/loader.js`;
        document.body.appendChild(loaderScript);
      };
      loadLoader();
    });
  }
};
var noop = () => {
};
var NGX_MONACO_EDITOR_CONFIG = new InjectionToken("NGX_MONACO_EDITOR_CONFIG");
var NgxMonacoEditorComponent = class _NgxMonacoEditorComponent {
  constructor() {
    this.value = model("");
    this.options = input({});
    this.language = input("typescript");
    this.editorStyle = input({
      width: "100%",
      height: "100%",
      border: "1px solid grey"
    });
    this.theme = input("vs");
    this.fullScreenKeyBinding = input();
    this.editorInitialized = output();
    this.onFocus = output({
      alias: "focus"
    });
    this.onBlur = output({
      alias: "blur"
    });
    this.editorContainer = viewChild.required("editorContainer");
    this.focused = signal(false);
    this.monacoLoadFailed = signal(false);
    this.zone = inject(NgZone);
    this.document = inject(DOCUMENT);
    this.monacoLoader = inject(NGX_MONACO_LOADER_PROVIDER);
    this.cd = inject(ChangeDetectorRef);
    this.elementRef = inject(ElementRef);
    this.config = inject(NGX_MONACO_EDITOR_CONFIG, {
      optional: true
    });
    this.propagateChange = noop;
    this.onTouched = noop;
    this.changesFromEditor = false;
    this._value = "";
  }
  ngOnInit() {
    this.monacoLoader.monacoLoaded().then((m) => {
      this._monaco = m;
      const containerDiv = this.editorContainer().nativeElement;
      const options = Object.assign({}, this.deepCopyOrEmpty(this.config?.defautlOptions), this.options(), {
        value: this._value ?? "",
        language: this.language(),
        theme: this.theme()
      });
      if (this.config?.runInsideNgZone) {
        this.editor = this._monaco.editor.create(containerDiv, options);
        this.editor.getModel()?.onDidChangeContent((e) => {
          this.changesFromEditor = true;
          this._value = this.editor.getValue();
          this.applyValue();
          this.propagateChange(this._value);
          this.value.set(this._value);
        });
        this.editor.onDidFocusEditorWidget(() => {
          this.focused.set(true);
          this.onFocus.emit();
        });
        this.editor.onDidBlurEditorWidget(() => {
          this.focused.set(false);
          this.onTouched();
          this.onBlur.emit();
        });
      } else {
        this.zone.runOutsideAngular(() => {
          this.editor = this._monaco.editor.create(containerDiv, options);
          this.editor.getModel()?.onDidChangeContent((e) => {
            this.zone.run(() => {
              this.changesFromEditor = true;
              this._value = this.editor.getValue();
              this.applyValue();
              this.propagateChange(this._value);
              this.value.set(this._value);
            });
          });
          this.editor.onDidFocusEditorWidget(() => {
            this.zone.run(() => {
              this.focused.set(true);
              this.onFocus.emit();
            });
          });
          this.editor.onDidBlurEditorWidget(() => {
            this.zone.run(() => {
              this.focused.set(false);
              this.onTouched();
              this.onBlur.emit();
            });
          });
        });
      }
      Promise.resolve().then(() => {
        this.applyValue();
        this.editorInitialized.emit({
          editor: this.editor,
          monaco: this._monaco
        });
      });
      this.addFullScreenModeCommand();
      this.resizeObserver = new ResizeObserver(() => {
        this.layout();
        this.cd.markForCheck();
      });
      this.resizeObserver.observe(this.document.documentElement);
      this.resizeObserver.observe(this.elementRef.nativeElement);
    }).catch(() => this.monacoLoadFailed.set(true));
  }
  focus() {
    if (this.editor) {
      this.editor.focus();
    }
  }
  ngOnChanges(changes) {
    if ("value" in changes) {
      if (this._value !== changes.value.currentValue) {
        this._value = changes.value.currentValue ?? "";
        this.applyValue();
      }
    }
    if (this.editor) {
      if ("theme" in changes && changes.theme.currentValue) {
        this.editor.updateOptions({
          theme: changes.theme.currentValue
        });
      }
      if ("language" in changes && this.editor.getModel() && changes.language.currentValue) {
        this._monaco.editor.setModelLanguage(this.editor.getModel(), changes.language.currentValue);
      }
      if ("options" in changes) {
        this.editor.updateOptions(changes.options.currentValue);
      }
    }
  }
  deepCopyOrEmpty(obj) {
    if (!obj) {
      return {};
    }
    return JSON.parse(JSON.stringify(obj));
  }
  applyValue() {
    if (this.editor && !this.changesFromEditor) {
      this.editor.setValue(this._value ?? "");
    }
    this.changesFromEditor = false;
  }
  /**
   * Implemented as part of ControlValueAccessor.
   */
  writeValue(value) {
    value = value ?? "";
    if (this._value !== value) {
      this._value = value;
      this.applyValue();
    }
  }
  registerOnChange(fn) {
    this.propagateChange = fn;
  }
  registerOnTouched(fn) {
    this.onTouched = fn;
  }
  /**
   * layout method that calls layout method of editor and instructs the editor to remeasure its container
   */
  layout() {
    if (this.editor) {
      this.editor.layout();
    }
  }
  ngOnDestroy() {
    this.resizeObserver?.disconnect();
    if (this.editor) {
      this.editor.dispose();
    }
  }
  showFullScreenEditor() {
    if (this.editor) {
      const codeEditorElement = this.editorContainer().nativeElement;
      codeEditorElement.requestFullscreen();
    }
  }
  /**
   * exitFullScreenEditor request to exit full screen of Code Editor based on its browser type.
   */
  exitFullScreenEditor() {
    if (this.editor) {
      this.document.exitFullscreen();
    }
  }
  /**
   * addFullScreenModeCommand used to add the fullscreen option to the context menu
   */
  addFullScreenModeCommand() {
    this.editor?.addAction({
      // An unique identifier of the contributed action.
      id: "fullScreen",
      // A label of the action that will be presented to the user.
      label: "Full Screen",
      // An optional array of keybindings for the action.
      contextMenuGroupId: "navigation",
      keybindings: this.fullScreenKeyBinding(),
      contextMenuOrder: 1.5,
      // Method that will be executed when the action is triggered.
      // @param editor The editor instance is passed in as a convinience
      run: (ed) => {
        this.showFullScreenEditor();
      }
    });
  }
  static {
    this.ɵfac = function NgxMonacoEditorComponent_Factory(t) {
      return new (t || _NgxMonacoEditorComponent)();
    };
  }
  static {
    this.ɵcmp = ɵɵdefineComponent({
      type: _NgxMonacoEditorComponent,
      selectors: [["ngx-monaco-editor"]],
      viewQuery: function NgxMonacoEditorComponent_Query(rf, ctx) {
        if (rf & 1) {
          ɵɵviewQuerySignal(ctx.editorContainer, _c0, 5);
        }
        if (rf & 2) {
          ɵɵqueryAdvance();
        }
      },
      hostVars: 2,
      hostBindings: function NgxMonacoEditorComponent_HostBindings(rf, ctx) {
        if (rf & 1) {
          ɵɵlistener("click", function NgxMonacoEditorComponent_click_HostBindingHandler() {
            return ctx.focus();
          });
        }
        if (rf & 2) {
          ɵɵclassProp("focused", ctx.focused());
        }
      },
      inputs: {
        value: [InputFlags.SignalBased, "value"],
        options: [InputFlags.SignalBased, "options"],
        language: [InputFlags.SignalBased, "language"],
        editorStyle: [InputFlags.SignalBased, "editorStyle"],
        theme: [InputFlags.SignalBased, "theme"],
        fullScreenKeyBinding: [InputFlags.SignalBased, "fullScreenKeyBinding"]
      },
      outputs: {
        value: "valueChange",
        editorInitialized: "editorInitialized",
        onFocus: "focus",
        onBlur: "blur"
      },
      standalone: true,
      features: [ɵɵProvidersFeature([{
        provide: NG_VALUE_ACCESSOR,
        useExisting: forwardRef(() => _NgxMonacoEditorComponent),
        multi: true
      }]), ɵɵNgOnChangesFeature, ɵɵStandaloneFeature],
      decls: 3,
      vars: 2,
      consts: [["editorContainer", ""], [1, "ngx-editor-container", 3, "ngStyle"]],
      template: function NgxMonacoEditorComponent_Template(rf, ctx) {
        if (rf & 1) {
          ɵɵelementStart(0, "div", 1, 0);
          ɵɵtemplate(2, NgxMonacoEditorComponent_Conditional_2_Template, 1, 0);
          ɵɵelementEnd();
        }
        if (rf & 2) {
          ɵɵproperty("ngStyle", ctx.editorStyle());
          ɵɵadvance(2);
          ɵɵconditional(2, ctx.monacoLoadFailed() ? 2 : -1);
        }
      },
      dependencies: [NgStyle],
      styles: ["ngx-monaco-editor{display:block;position:relative}ngx-monaco-editor .ngx-editor-container{position:absolute;inset:0}\n"],
      encapsulation: 2,
      changeDetection: 0
    });
  }
};
(() => {
  (typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgxMonacoEditorComponent, [{
    type: Component,
    args: [{
      selector: "ngx-monaco-editor",
      standalone: true,
      imports: [NgClass, NgStyle],
      template: `
    <div class="ngx-editor-container" #editorContainer [ngStyle]="editorStyle()">
      @if (monacoLoadFailed()) {
        Monaco was not loaded correctly.
      }
    </div>`,
      host: {
        "[class.focused]": "focused()",
        "(click)": "focus()"
      },
      changeDetection: ChangeDetectionStrategy.OnPush,
      encapsulation: ViewEncapsulation$1.None,
      providers: [{
        provide: NG_VALUE_ACCESSOR,
        useExisting: forwardRef(() => NgxMonacoEditorComponent),
        multi: true
      }],
      styles: ["ngx-monaco-editor{display:block;position:relative}ngx-monaco-editor .ngx-editor-container{position:absolute;inset:0}\n"]
    }]
  }], null, null);
})();
export {
  DefaultMonacoLoader,
  NGX_MONACO_EDITOR_CONFIG,
  NGX_MONACO_LOADER_PROVIDER,
  NgxMonacoEditorComponent
};
//# sourceMappingURL=@jean-merelis_ngx-monaco-editor.js.map
