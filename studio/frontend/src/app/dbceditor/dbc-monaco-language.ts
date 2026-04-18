let dbcLanguageRegistered = false;

type MonacoLike = {
  languages: {
    register(language: {
      id: string;
      extensions?: string[];
      aliases?: string[];
    }): void;

    setLanguageConfiguration(languageId: string, configuration: {
      comments?: {
        lineComment?: string;
        blockComment?: [string, string];
      };
      autoClosingPairs?: Array<{ open: string; close: string }>;
      surroundingPairs?: Array<{ open: string; close: string }>;
      brackets?: Array<[string, string]>;
    }): void;

    setMonarchTokensProvider(languageId: string, languageDef: unknown): void;
  };

  editor: {
    defineTheme(themeName: string, themeData: unknown): void;
  };
};

export function registerDbcLanguage(monacoInstance: MonacoLike): void {
  if (dbcLanguageRegistered) {
    return;
  }

  dbcLanguageRegistered = true;

  monacoInstance.languages.register({
    id: 'dbc',
    extensions: ['.dbc'],
    aliases: ['DBC', 'dbc']
  });

  monacoInstance.languages.setLanguageConfiguration('dbc', {
    comments: {
      lineComment: '//'
    },
    autoClosingPairs: [
      { open: '"', close: '"' },
      { open: '(', close: ')' },
      { open: '[', close: ']' }
    ],
    surroundingPairs: [
      { open: '"', close: '"' },
      { open: '(', close: ')' },
      { open: '[', close: ']' }
    ],
    brackets: [
      ['(', ')'],
      ['[', ']']
    ]
  });

  monacoInstance.languages.setMonarchTokensProvider('dbc', {
    keywords: [
      'VERSION',
      'NS_',
      'BS_',
      'BU_',
      'BO_',
      'SG_',
      'CM_',
      'BA_',
      'BA_DEF_',
      'BA_DEF_DEF_',
      'BA_REL_',
      'BA_DEF_REL_',
      'BA_DEF_DEF_REL_',
      'VAL_',
      'VAL_TABLE_',
      'SIG_VALTYPE_',
      'SIG_GROUP_',
      'CAT_',
      'CAT_DEF_',
      'FILTER',
      'EV_',
      'ENVVAR_DATA_',
      'SGTYPE_',
      'SGTYPE_VAL_',
      'SG_MUL_VAL_'
    ],

    nsKeywords: [
      'NS_DESC_',
      'CM_',
      'BA_DEF_',
      'BA_',
      'VAL_',
      'CAT_DEF_',
      'CAT_',
      'FILTER',
      'BA_DEF_DEF_',
      'EV_DATA_',
      'ENVVAR_DATA_',
      'SGTYPE_',
      'SGTYPE_VAL_',
      'BA_DEF_SGTYPE_',
      'BA_SGTYPE_',
      'SIG_TYPE_REF_',
      'VAL_TABLE_',
      'SIG_GROUP_',
      'SIG_VALTYPE_',
      'SIGTYPE_VALTYPE_',
      'BO_TX_BU_',
      'BA_DEF_REL_',
      'BA_REL_',
      'BA_DEF_DEF_REL_',
      'BU_SG_REL_',
      'BU_EV_REL_',
      'BU_BO_REL_',
      'SG_MUL_VAL_'
    ],

    types: [
      'INT',
      'HEX',
      'FLOAT',
      'STRING',
      'ENUM'
    ],

    tokenizer: {
      root: [
        [/\/\/.*$/, 'comment'],
        [/"/, { token: 'string.quote', next: '@string' }],

        [/-?\d+\.\d+/, 'number.float'],
        [/-?\d+/, 'number'],

        [/@[01][+-]/, 'tag'],
        [/[|:;,]/, 'delimiter'],
        [/[()[\]]/, '@brackets'],
        [/[+-]/, 'operator'],

        [
          /[A-Za-z_][A-Za-z0-9_]*/,
          {
            cases: {
              '@keywords': 'keyword',
              '@nsKeywords': 'keyword.ns',
              '@types': 'type',
              'Vector__XXX': 'type.identifier',
              '@default': 'identifier'
            }
          }
        ]
      ],

      string: [
        [/[^\\"]+/, 'string'],
        [/\\./, 'string.escape'],
        [/"/, { token: 'string.quote', next: '@pop' }]
      ]
    }
  });

  monacoInstance.editor.defineTheme('dbcVsCodeLight', {
    base: 'vs',
    inherit: true,
    rules: [
      { token: 'keyword', foreground: 'AF00DB' },
      { token: 'type', foreground: '267F99' },
      { token: 'type.identifier', foreground: '001080' },
      { token: 'identifier', foreground: '000000' },
      { token: 'number', foreground: '098658' },
      { token: 'string', foreground: 'A31515' },
      { token: 'string.escape', foreground: 'EE0000' },
      { token: 'comment', foreground: '008000' },
      { token: 'delimiter', foreground: '000000' },
      { token: 'tag', foreground: '800000' }
    ],
    colors: {
      'editor.background': '#FFFFFF',
      'editor.foreground': '#000000',
      'editorLineNumber.foreground': '#237893',
      'editorLineNumber.activeForeground': '#0B216F',
      'editorCursor.foreground': '#000000',
      'editor.selectionBackground': '#ADD6FF',
      'editor.inactiveSelectionBackground': '#E5EBF1',
      'editor.lineHighlightBackground': '#F2F2F2',
      'editorGutter.background': '#FFFFFF',

      'scrollbarSlider.background': '#BFDBFE',
      'scrollbarSlider.hoverBackground': '#93C5FD',
      'scrollbarSlider.activeBackground': '#60A5FA'
    }
  });
}