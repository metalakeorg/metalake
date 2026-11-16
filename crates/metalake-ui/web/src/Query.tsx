import { useEffect, useRef, useState } from "react";
import { Assets } from "./Assets";
import { runSteps, type Step } from "./client";
import { Editor, type Insert } from "./Editor";
import { usePicked } from "./picked";
import { Results, type Run } from "./Results";

/** The keys that run a statement, as this machine names them. */
const RUN_KEYS = /Mac|iPhone|iPad/.test(globalThis.navigator?.platform ?? "") ? "⌘ Enter" : "Ctrl Enter";

/**
 * The Query page, where the UI opens: the assets of a ref beside it, read from the lake and kept as it is; an SQL
 * editor above, and the rows of what it ran below. A statement runs on the ref picked, as `metalake sql` runs one: a
 * read at the point it is run, a write committed, which the assets then show. A tag is the lake as it was when tagged:
 * read only, its writes refused by the lake.
 */
export function QueryPage() {
  const [text, setText] = useState("");
  const picked = usePicked();
  const { ref: branch, tagged } = picked;

  // A run that ends on another ref, by `USE BRANCH` or `USE TAG`, moves the picker there, and the next run starts there.
  const { state, start, cancel } = useRun(picked.follow);
  const insert = useRef<Insert | null>(null);

  return (
    <div className="flex h-full">
      <Assets
        branches={picked.branches}
        tags={picked.tags}
        branch={picked.branch}
        tag={picked.tag}
        onBranch={picked.pickBranch}
        onTag={picked.pickTag}
        tables={picked.tables}
        schemas={picked.schemas}
        failure={picked.failure}
        columns
        onInsert={(text) => insert.current?.(text)}
      />
      <section aria-label="Query" className="flex min-w-0 flex-1 flex-col">
        <div className="flex h-11 shrink-0 items-center gap-3 border-zinc-200 border-b px-4 dark:border-zinc-800">
          <h1 className="font-semibold">Query</h1>
          {tagged && <span className="text-amber-700 text-xs dark:text-amber-400">tag · read only</span>}
          {state.kind === "running" ? (
            <button
              type="button"
              onClick={cancel}
              className="ml-auto rounded border border-zinc-300 px-3 py-1 font-medium text-sm hover:bg-zinc-100 dark:border-zinc-700 dark:hover:bg-zinc-800"
            >
              Cancel
            </button>
          ) : (
            <button
              type="button"
              onClick={() => start(text, branch)}
              className="ml-auto rounded bg-zinc-900 px-3 py-1 font-medium text-sm text-white hover:bg-zinc-700 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
            >
              Run
            </button>
          )}
          <kbd className="text-xs text-zinc-500 dark:text-zinc-400">{RUN_KEYS}</kbd>
        </div>
        <div className="h-2/5 min-h-24 shrink-0 border-zinc-200 border-b py-1 dark:border-zinc-800">
          <Editor value={text} onChange={setText} onRun={(statement) => start(statement, branch)} insert={insert} />
        </div>
        <Results state={state} />
      </section>
    </div>
  );
}

/**
 * A statement's run: started, cancelled, and what it came to. One at a time: a new one cancels the last. The ref the
 * run is on after its last statement that ran goes to `moved`.
 */
function useRun(moved: (ref: string) => void) {
  const [state, setState] = useState<Run>({ kind: "idle" });
  const current = useRef<AbortController | null>(null);
  const runs = useRef(0);

  const cancel = () => current.current?.abort();

  const start = (statement: string, branch: string) => {
    const sql = statement.trim();
    if (!sql) {
      setState({ kind: "empty" });
      return;
    }
    cancel();
    const stop = new AbortController();
    current.current = stop;
    const id = ++runs.current;
    const steps: Step[] = [];
    let on: string | undefined;
    // Where the run ended, which the picker follows: a run cancelled or failed moved all the same.
    const ended = () => {
      if (on !== undefined && current.current === stop) moved(on);
    };
    setState({ kind: "running", id, steps: [] });
    // Each statement shows as it ends: the run's state is replaced with each, so that it draws again.
    (async () => {
      for await (const step of runSteps(sql, branch, stop.signal)) {
        steps.push(step);
        if (step.ref !== undefined) on = step.ref;
        if (current.current === stop) setState({ kind: "running", id, steps: [...steps] });
      }
    })().then(
      () => {
        ended();
        if (current.current === stop) setState({ kind: "done", id, steps: [...steps] });
      },
      (error: unknown) => {
        ended();
        if (current.current !== stop) return;
        if (stop.signal.aborted) setState({ kind: "cancelled", id, steps: [...steps] });
        else if (steps.length) setState({ kind: "done", id, steps: [...steps] });
        else setState({ kind: "failed", error: error instanceof Error ? error : new Error(String(error)) });
      },
    );
  };

  // Leaving the page drops the statement.
  useEffect(() => () => current.current?.abort(), []);

  return { state, start, cancel };
}
