// What the Query page ran: a result per statement, one shown at a time, each with how it ended: a read's rows in a grid of its columns, a page
// of them at a time, of which only the rows in sight are drawn (TanStack Virtual, ADR 0007 §5.2); a write's commit; a
// failure; or that it was not run.

import { useVirtualizer } from "@tanstack/react-virtual";
import { ChevronLeft, ChevronRight, CircleCheck, CircleMinus, CircleX } from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { Failure, type Fault, LIMIT, type Step } from "./client";
import { type Result, show, toJson } from "./result";

/**
 * What a run came to: its statements as each ended, while it runs and after. `failed` is a run refused as a whole,
 * before any statement ran. `id` tells one run from the next.
 */
export type Run =
  | { kind: "idle" }
  | { kind: "empty" }
  | { kind: "running"; id: number; steps: Step[] }
  | { kind: "cancelled"; id: number; steps: Step[] }
  | { kind: "done"; id: number; steps: Step[] }
  | { kind: "failed"; error: Error };

/** The tab a run opens on: the statement that failed, or else the last that ran. */
export function opened(steps: readonly Step[]): number {
  const failed = steps.findIndex((step) => step.state === "error");
  if (failed >= 0) return failed;
  const ran = steps.findLastIndex((step) => step.state !== "skipped");
  return Math.max(ran, 0);
}

/** The height of a row of the grid, in pixels. */
const ROW = 28;
/** The rows of a page of the grid. */
export const PAGE = 100;
const HEAD = "border-zinc-200 border-b px-2 py-1.5 font-semibold whitespace-nowrap dark:border-zinc-800";
const TURN =
  "rounded px-1.5 hover:bg-zinc-100 hover:text-zinc-900 disabled:opacity-40 disabled:hover:bg-transparent dark:hover:bg-zinc-800 dark:hover:text-zinc-100";
const CELL = "max-w-96 truncate border-zinc-100 border-b px-2 py-1 whitespace-nowrap dark:border-zinc-900";

export function Results({ state }: { state: Run }) {
  return (
    <section aria-label="Results" className="flex min-h-0 flex-1 flex-col text-sm">
      <Said state={state} />
    </section>
  );
}

const QUIET = "p-4 text-zinc-500 dark:text-zinc-400";
const STATUS =
  "flex shrink-0 gap-3 border-zinc-200 border-b px-4 py-1.5 text-xs text-zinc-500 dark:border-zinc-800 dark:text-zinc-400";

function Said({ state }: { state: Run }) {
  switch (state.kind) {
    case "idle":
      return <p className={QUIET}>Run a statement to see its rows here.</p>;
    case "empty":
      return <p className={QUIET}>There is nothing to run.</p>;
    case "failed":
      return <Fails kind={state.error instanceof Failure ? state.error.fault.kind : undefined} error={state.error} />;
    case "running":
    case "cancelled":
    case "done":
      return <Steps key={state.id} state={state} />;
  }
}

/** A failure, as the command line says it. */
function Fails({ kind, error }: { kind: string | undefined; error: { message: string } }) {
  return (
    <p role="alert" className="p-4 font-mono text-red-700 text-xs dark:text-red-400">
      metalake{kind ? ` (${kind})` : ""}: {error.message}
    </p>
  );
}

/**
 * A run's statements, one result shown at a time, turned with the arrows of `‹ Result 2 of 5 ›`. Until one is turned to,
 * the one shown follows the run: the latest while it runs, then the statement that failed or the last that ran. One
 * statement alone has no arrows.
 */
function Steps({ state }: { state: Extract<Run, { steps: Step[] }> }) {
  const { steps } = state;
  const [picked, setPicked] = useState<number | null>(null);
  const shown = Math.min(picked ?? (state.kind === "running" ? steps.length - 1 : opened(steps)), steps.length - 1);
  const step = steps[shown];
  const tail =
    state.kind === "running"
      ? "Running…"
      : state.kind === "cancelled"
        ? "Cancelled."
        : steps.length
          ? ""
          : "Nothing ran.";
  return (
    <>
      {steps.length > 1 && step && (
        <nav
          aria-label="Results of the statements"
          className="flex shrink-0 items-center gap-1 border-zinc-200 border-b px-2 py-1 text-xs text-zinc-500 dark:border-zinc-800 dark:text-zinc-400"
        >
          <button
            type="button"
            aria-label="Previous result"
            onClick={() => setPicked(shown - 1)}
            disabled={shown === 0}
            className={TURN}
          >
            <ChevronLeft size={14} aria-hidden />
          </button>
          <span className="flex items-center gap-1.5 tabular-nums" title={step.sql}>
            <Ended step={step} />
            Result {shown + 1} of {steps.length}
          </span>
          <button
            type="button"
            aria-label="Next result"
            onClick={() => setPicked(shown + 1)}
            disabled={shown === steps.length - 1}
            className={TURN}
          >
            <ChevronRight size={14} aria-hidden />
          </button>
        </nav>
      )}
      {step && (
        <div className="flex min-h-0 flex-1 flex-col">
          <Answer step={step} />
        </div>
      )}
      {tail && (
        <p role="status" className={QUIET}>
          {tail}
        </p>
      )}
    </>
  );
}

/** How a statement ended, as an icon its tab shows. */
function Ended({ step }: { step: Step }) {
  const size = { size: 13, "aria-hidden": true } as const;
  switch (step.state) {
    case "ok":
      return <CircleCheck {...size} className="shrink-0 text-emerald-600 dark:text-emerald-400" />;
    case "error":
      return <CircleX {...size} className="shrink-0 text-red-600 dark:text-red-400" />;
    case "skipped":
      return <CircleMinus {...size} className="shrink-0 text-zinc-400" />;
  }
}

/** What one statement came to: its rows, its commit, that it ran, its failure, or that it was not run. */
function Answer({ step }: { step: Step }) {
  const took = step.ms === undefined ? null : <span>{(step.ms / 1000).toFixed(2)} s</span>;
  if (step.state === "skipped") {
    return <p className={QUIET}>Not run: a statement before it failed.</p>;
  }
  if (step.state === "error") {
    const fault: Fault | undefined = step.fault;
    return <Fails kind={fault?.kind} error={{ message: fault?.message ?? "it failed" }} />;
  }
  if (step.result) return <Grid result={step.result} ms={step.ms ?? 0} />;
  return (
    <p role="status" className={STATUS}>
      <span className="text-emerald-700 dark:text-emerald-400">OK</span>
      {step.written && (
        <>
          <span>
            {step.written.rows.toLocaleString("en-US")} {step.written.rows === 1 ? "row" : "rows"} written
          </span>
          <span>{step.written.commit == null ? "no commit: nothing changed" : `commit ${step.written.commit}`}</span>
        </>
      )}
      {took}
    </p>
  );
}

function Grid({ result, ms }: { result: Result; ms: number }) {
  const box = useRef<HTMLDivElement>(null);
  // The page is the result's it was turned on: a new result opens on its first page.
  const [at, setAt] = useState({ result, page: 0 });
  const page = at.result === result ? at.page : 0;
  const pages = Math.max(1, Math.ceil(result.rows.length / PAGE));
  const first = Math.min(page, pages - 1) * PAGE;
  const shown = useMemo(() => result.rows.slice(first, first + PAGE), [result, first]);
  const turn = (to: number) => {
    setAt({ result, page: to });
    if (box.current) box.current.scrollTop = 0;
  };
  const rows = useVirtualizer({
    count: shown.length,
    getScrollElement: () => box.current,
    estimateSize: () => ROW,
    overscan: 20,
  });
  const count = result.rows.length.toLocaleString("en-US");
  // Two columns may share a name: each is known by its position.
  const columns = useMemo(() => result.columns.map((name, at) => ({ id: `${at}:${name}`, name, at })), [result]);
  // Only the rows in sight are drawn; a row above and one below stand for the rest, at their height.
  const items = rows.getVirtualItems();
  const above = items[0]?.start ?? 0;
  const below = rows.getTotalSize() - (items.at(-1)?.end ?? 0);

  return (
    <>
      <p role="status" className={STATUS}>
        <span>
          {count} {result.rows.length === 1 ? "row" : "rows"}
        </span>
        <span>{(ms / 1000).toFixed(2)} s</span>
        {result.truncated && (
          <span className="text-amber-700 dark:text-amber-400">
            the first {LIMIT.toLocaleString("en-US")}: the statement answered more
          </span>
        )}
        <CopyJson result={result} />
      </p>
      {pages > 1 && (
        <nav
          aria-label="Pages of rows"
          className="flex shrink-0 items-center gap-2 border-zinc-200 border-b px-4 py-1 text-xs text-zinc-500 dark:border-zinc-800 dark:text-zinc-400"
        >
          <button type="button" onClick={() => turn(page - 1)} disabled={first === 0} className={TURN}>
            Previous
          </button>
          <span>
            Rows {(first + 1).toLocaleString("en-US")}–{(first + shown.length).toLocaleString("en-US")} · page{" "}
            {first / PAGE + 1} of {pages}
          </span>
          <button type="button" onClick={() => turn(page + 1)} disabled={first / PAGE + 1 >= pages} className={TURN}>
            Next
          </button>
        </nav>
      )}
      <div ref={box} className="min-h-0 flex-1 overflow-auto font-mono text-xs">
        <table aria-label="Rows" className="min-w-full border-separate border-spacing-0">
          <thead className="sticky top-0 z-10 bg-white dark:bg-zinc-950">
            <tr>
              <th className={`${HEAD} w-14 text-right text-zinc-400`}>#</th>
              {columns.map(({ id, name }) => (
                <th key={id} className={`${HEAD} text-left text-zinc-700 dark:text-zinc-200`}>
                  {name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {above > 0 && <tr aria-hidden style={{ height: above }} />}
            {items.map((item) => {
              const row = shown[item.index] ?? [];
              return (
                <tr key={item.key} style={{ height: ROW }} className="hover:bg-zinc-50 dark:hover:bg-zinc-900">
                  <td className={`${CELL} text-right text-zinc-400`}>{first + item.index + 1}</td>
                  {columns.map(({ id, at }) => (
                    <td
                      key={id}
                      className={`${CELL} ${row[at] === null ? "text-zinc-400 italic" : ""}`}
                      title={show(row[at])}
                    >
                      {show(row[at])}
                    </td>
                  ))}
                </tr>
              );
            })}
            {below > 0 && <tr aria-hidden style={{ height: below }} />}
          </tbody>
        </table>
      </div>
    </>
  );
}

/** Copies the rows shown, as JSON, to the clipboard; says it did, or that it could not. */
function CopyJson({ result }: { result: Result }) {
  const [said, setSaid] = useState<"" | "Copied" | "Not copied">("");
  const copy = () => {
    const done = navigator.clipboard?.writeText(toJson(result));
    if (!done) {
      setSaid("Not copied");
      return;
    }
    done.then(
      () => setSaid("Copied"),
      () => setSaid("Not copied"),
    );
  };
  return (
    <button
      type="button"
      onClick={copy}
      onBlur={() => setSaid("")}
      className="ml-auto rounded px-1.5 hover:bg-zinc-100 hover:text-zinc-900 dark:hover:bg-zinc-800 dark:hover:text-zinc-100"
    >
      {said || "Copy JSON"}
    </button>
  );
}
