// The rows of what the Query page ran: a grid of its columns, of which only the rows in sight are drawn, so that ten
// thousand of them cost what a screenful does (TanStack Virtual, ADR 0007 §5.2).

import { useVirtualizer } from "@tanstack/react-virtual";
import { useMemo, useRef } from "react";
import { Failure, LIMIT } from "./client";
import { type Result, show } from "./result";

/** What a statement's run came to. */
export type Run =
  | { kind: "idle" }
  | { kind: "empty" }
  | { kind: "running" }
  | { kind: "cancelled" }
  | { kind: "done"; result: Result; ms: number }
  | { kind: "failed"; error: Error };

/** The height of a row of the grid, in pixels. */
const ROW = 28;
const HEAD = "border-zinc-200 border-b px-2 py-1.5 font-semibold whitespace-nowrap dark:border-zinc-800";
const CELL = "max-w-96 truncate border-zinc-100 border-b px-2 py-1 whitespace-nowrap dark:border-zinc-900";

export function Results({ state }: { state: Run }) {
  return (
    <section aria-label="Results" className="flex min-h-0 flex-1 flex-col text-sm">
      <Said state={state} />
    </section>
  );
}

function Said({ state }: { state: Run }) {
  const quiet = "p-4 text-zinc-500 dark:text-zinc-400";
  switch (state.kind) {
    case "idle":
      return <p className={quiet}>Run a statement to see its rows here.</p>;
    case "empty":
      return <p className={quiet}>There is nothing to run.</p>;
    case "running":
      return (
        <p role="status" className={quiet}>
          Running…
        </p>
      );
    case "cancelled":
      return (
        <p role="status" className={quiet}>
          Cancelled.
        </p>
      );
    case "failed": {
      const kind = state.error instanceof Failure ? ` (${state.error.fault.kind})` : "";
      return (
        <p role="alert" className="p-4 font-mono text-red-700 text-xs dark:text-red-400">
          metalake{kind}: {state.error.message}
        </p>
      );
    }
    case "done":
      return <Grid result={state.result} ms={state.ms} />;
  }
}

function Grid({ result, ms }: { result: Result; ms: number }) {
  const box = useRef<HTMLDivElement>(null);
  const rows = useVirtualizer({
    count: result.rows.length,
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
      <p
        role="status"
        className="flex shrink-0 gap-3 border-zinc-200 border-b px-4 py-1.5 text-xs text-zinc-500 dark:border-zinc-800 dark:text-zinc-400"
      >
        <span>
          {count} {result.rows.length === 1 ? "row" : "rows"}
        </span>
        <span>{(ms / 1000).toFixed(2)} s</span>
        {result.truncated && (
          <span className="text-amber-700 dark:text-amber-400">
            the first {LIMIT.toLocaleString("en-US")}: the statement answered more
          </span>
        )}
      </p>
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
              const row = result.rows[item.index] ?? [];
              return (
                <tr key={item.key} style={{ height: ROW }} className="hover:bg-zinc-50 dark:hover:bg-zinc-900">
                  <td className={`${CELL} text-right text-zinc-400`}>{item.index + 1}</td>
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
