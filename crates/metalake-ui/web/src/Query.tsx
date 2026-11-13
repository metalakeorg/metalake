import { useState } from "react";
import { Editor } from "./Editor";

/** The keys that run a statement, as this machine names them. */
const RUN_KEYS = /Mac|iPhone|iPad/.test(globalThis.navigator?.platform ?? "") ? "⌘ Enter" : "Ctrl Enter";

/**
 * The Query page, where the UI opens: an SQL editor above, and the rows of what it ran below. A shell for now: running
 * reaches no lake yet, and says so.
 */
export function QueryPage() {
  const [text, setText] = useState("");
  const [ran, setRan] = useState<string | null>(null);
  const run = (statement: string) => setRan(statement.trim());

  return (
    <section aria-label="Query" className="flex h-full flex-col">
      <div className="flex h-11 shrink-0 items-center gap-3 border-zinc-200 border-b px-4 dark:border-zinc-800">
        <h1 className="font-semibold">Query</h1>
        <button
          type="button"
          onClick={() => run(text)}
          className="ml-auto rounded bg-zinc-900 px-3 py-1 font-medium text-sm text-white hover:bg-zinc-700 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
        >
          Run
        </button>
        <kbd className="text-xs text-zinc-500 dark:text-zinc-400">{RUN_KEYS}</kbd>
      </div>
      <div className="h-2/5 min-h-24 shrink-0 border-zinc-200 border-b py-1 dark:border-zinc-800">
        <Editor value={text} onChange={setText} onRun={run} />
      </div>
      <Results ran={ran} />
    </section>
  );
}

/** Where the rows of a statement go. */
function Results({ ran }: { ran: string | null }) {
  return (
    <section aria-label="Results" className="min-h-0 flex-1 overflow-auto p-4 text-sm text-zinc-500 dark:text-zinc-400">
      {ran === null && <p>Run a statement to see its rows here.</p>}
      {ran === "" && <p>There is nothing to run.</p>}
      {ran && <p role="status">Not run: the editor does not reach the lake yet.</p>}
    </section>
  );
}
