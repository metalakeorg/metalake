import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { Assets } from "./Assets";
import { refs, run, tables } from "./client";
import { Editor } from "./Editor";
import { CATALOG } from "./live";
import { Results, type Run } from "./Results";

/** The keys that run a statement, as this machine names them. */
const RUN_KEYS = /Mac|iPhone|iPad/.test(globalThis.navigator?.platform ?? "") ? "⌘ Enter" : "Ctrl Enter";

/** The branch every lake has, which the page opens on. */
const MAIN = "main";

/**
 * The Query page, where the UI opens: the assets of a ref beside it, read from the lake and kept as it is; an SQL
 * editor above, and the rows of what it ran below. A statement runs on the ref picked, as `metalake sql` runs one: a
 * read at the point it is run, a write committed, which the assets then show. A tag is the lake as it was when tagged:
 * read only, its writes refused by the lake.
 */
export function QueryPage() {
  const [text, setText] = useState("");
  const [branch, setBranch] = useState(MAIN);

  const known = useQuery({ queryKey: [...CATALOG, "refs"], queryFn: () => refs() });
  const branches = known.data?.filter((r) => r.kind === "branch").map((r) => r.name) ?? [MAIN];
  const tags = known.data?.filter((r) => r.kind === "tag").map((r) => r.name) ?? [];
  const tagged = tags.includes(branch);
  const listed = useQuery({ queryKey: [...CATALOG, "tables", branch], queryFn: () => tables(branch) });
  // A ref deleted while it was picked: back to the one every lake has.
  const gone = known.data !== undefined && !branches.includes(branch) && !tags.includes(branch);
  useEffect(() => {
    if (gone) setBranch(MAIN);
  }, [gone]);

  const { state, start, cancel } = useRun();

  return (
    <div className="flex h-full">
      <Assets
        branches={branches}
        tags={tags}
        branch={branch}
        onBranch={setBranch}
        tables={listed.data}
        failure={listed.error ?? known.error}
      />
      <section aria-label="Query" className="flex min-w-0 flex-1 flex-col">
        <div className="flex h-11 shrink-0 items-center gap-3 border-zinc-200 border-b px-4 dark:border-zinc-800">
          <h1 className="font-semibold">Query</h1>
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            on <span className="font-mono">{branch}</span>
            {tagged && <span className="ml-1.5 text-amber-700 dark:text-amber-400">tag · read only</span>}
          </span>
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
          <Editor value={text} onChange={setText} onRun={(statement) => start(statement, branch)} />
        </div>
        <Results state={state} />
      </section>
    </div>
  );
}

/** A statement's run: started, cancelled, and what it came to. One at a time: a new one cancels the last. */
function useRun() {
  const [state, setState] = useState<Run>({ kind: "idle" });
  const current = useRef<AbortController | null>(null);

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
    const began = performance.now();
    setState({ kind: "running" });
    run(sql, branch, stop.signal).then(
      (result) => {
        if (current.current === stop) setState({ kind: "done", result, ms: performance.now() - began });
      },
      (error: unknown) => {
        if (current.current !== stop) return;
        if (stop.signal.aborted) setState({ kind: "cancelled" });
        else setState({ kind: "failed", error: error instanceof Error ? error : new Error(String(error)) });
      },
    );
  };

  // Leaving the page drops the statement.
  useEffect(() => () => current.current?.abort(), []);

  return { state, start, cancel };
}
