// The SQL editor: CodeMirror 6, with only what is used (ADR 0007 §5.2). Its tokens are classes (`tok-…`), coloured by
// the page's own styles in light and dark (index.css), so the editor follows the frame's theme.

import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { sql } from "@codemirror/lang-sql";
import { bracketMatching, indentOnInput, syntaxHighlighting } from "@codemirror/language";
import { EditorState } from "@codemirror/state";
import {
  drawSelection,
  EditorView,
  highlightActiveLine,
  highlightActiveLineGutter,
  keymap,
  lineNumbers,
  placeholder,
} from "@codemirror/view";
import { classHighlighter } from "@lezer/highlight";
import { useEffect, useRef } from "react";

/** The editor's frame: it fills its box, and takes its colours from the page. */
const look = EditorView.theme({
  "&": { height: "100%", fontSize: "13px", backgroundColor: "transparent", color: "inherit" },
  "&.cm-focused": { outline: "none" },
  ".cm-scroller": { fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", lineHeight: "1.6" },
  ".cm-gutters": { backgroundColor: "transparent", border: "none", color: "var(--gutter)" },
  ".cm-activeLine, .cm-activeLineGutter": { backgroundColor: "var(--active-line)" },
  ".cm-content": { caretColor: "currentColor" },
  ".cm-placeholder": { color: "var(--gutter)" },
});

/**
 * An SQL editor holding `value`; `onChange` is told each edit, and `onRun` is called by Ctrl-Enter (Cmd-Enter on a
 * Mac) with the text to run: the selection, if there is one, else the whole.
 */
export function Editor({
  value,
  onChange,
  onRun,
}: {
  value: string;
  onChange: (value: string) => void;
  onRun: (statement: string) => void;
}) {
  const box = useRef<HTMLDivElement>(null);
  // The latest handlers, so that the editor, made once, calls the page's current ones.
  const handlers = useRef({ onChange, onRun });
  handlers.current = { onChange, onRun };

  // biome-ignore lint/correctness/useExhaustiveDependencies: the editor is made once; `value` is its initial text.
  useEffect(() => {
    if (!box.current) return;
    const run = (view: EditorView) => {
      const { from, to } = view.state.selection.main;
      handlers.current.onRun(from === to ? view.state.doc.toString() : view.state.sliceDoc(from, to));
      return true;
    };
    const view = new EditorView({
      parent: box.current,
      state: EditorState.create({
        doc: value,
        extensions: [
          lineNumbers(),
          highlightActiveLineGutter(),
          highlightActiveLine(),
          history(),
          drawSelection(),
          indentOnInput(),
          bracketMatching(),
          sql(),
          syntaxHighlighting(classHighlighter),
          placeholder("select * from …"),
          keymap.of([
            { key: "Mod-Enter", run, preventDefault: true },
            ...defaultKeymap,
            ...historyKeymap,
            indentWithTab,
          ]),
          EditorView.updateListener.of((update) => {
            if (update.docChanged) handlers.current.onChange(update.state.doc.toString());
          }),
          EditorView.contentAttributes.of({ "aria-label": "SQL" }),
          look,
        ],
      }),
    });
    return () => view.destroy();
  }, []);

  return <div ref={box} className="h-full min-h-0" />;
}
