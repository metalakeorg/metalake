// The Catalog's views of an asset, as a catalog of assets shows one: a header, and its tabs. Plain and in the page's
// style, for their look to be shaped later. What each shows is read from the lake and kept as it is (ADR 0007).

import { useQuery } from "@tanstack/react-query";
import { type ReactNode, useState } from "react";
import { commits, refDetails } from "./client";
import { Failed } from "./Failed";
import { CATALOG } from "./live";

/** A count, as the page writes one. */
export function count(n: number): string {
  return n.toLocaleString("en-US");
}

/** Bytes, in the unit that keeps them short. */
export function bytes(n: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit++;
  }
  return unit === 0 ? `${n} B` : `${value.toFixed(1)} ${units[unit]}`;
}

/** A time the lake wrote, as this machine's clock reads it. */
export function when(at: string | null | undefined): string {
  if (!at) return "—";
  const date = new Date(at);
  return Number.isNaN(date.getTime()) ? at : date.toLocaleString();
}

/** An asset's view: its kind and name above, and its tabs. */
export function View({
  kind,
  name,
  badge,
  tabs,
}: {
  kind: string;
  name: string;
  badge?: string | undefined;
  tabs: { title: string; body: () => ReactNode }[];
}) {
  const [at, setAt] = useState(0);
  const shown = tabs[Math.min(at, tabs.length - 1)];
  if (!shown) return null;
  return (
    <section aria-label={`${kind} ${name}`} className="flex h-full min-w-0 flex-col">
      <header className="shrink-0 border-zinc-200 border-b px-6 pt-4 dark:border-zinc-800">
        <p className="text-[10px] text-zinc-500 uppercase tracking-wide dark:text-zinc-400">{kind}</p>
        <h1 className="flex items-center gap-2 font-mono font-semibold text-lg">
          {name}
          {badge && <span className="font-sans font-normal text-amber-700 text-xs dark:text-amber-400">{badge}</span>}
        </h1>
        <div role="tablist" aria-label="Tabs" className="mt-3 flex gap-4 text-sm">
          {tabs.map((tab, i) => (
            <button
              key={tab.title}
              type="button"
              role="tab"
              aria-selected={tab === shown}
              onClick={() => setAt(i)}
              className="-mb-px border-transparent border-b-2 pb-2 text-zinc-500 hover:text-zinc-900 aria-selected:border-zinc-900 aria-selected:text-zinc-900 dark:text-zinc-400 dark:aria-selected:border-zinc-100 dark:aria-selected:text-zinc-100 dark:hover:text-zinc-100"
            >
              {tab.title}
            </button>
          ))}
        </div>
      </header>
      <div role="tabpanel" aria-label={shown.title} className="min-h-0 flex-1 overflow-auto px-6 py-4 text-sm">
        {shown.body()}
      </div>
    </section>
  );
}

/** Facts, as a list of terms and what each is. */
export function Facts({ facts }: { facts: [string, ReactNode][] }) {
  return (
    <dl className="grid max-w-2xl grid-cols-[12rem_1fr] gap-x-4 gap-y-1.5">
      {facts.map(([term, value]) => (
        <div key={term} className="contents">
          <dt className="text-zinc-500 dark:text-zinc-400">{term}</dt>
          <dd className="min-w-0 break-words font-mono">{value ?? "—"}</dd>
        </div>
      ))}
    </dl>
  );
}

const TH = "border-zinc-200 border-b px-2 py-1.5 text-left font-semibold dark:border-zinc-800";
const TD = "border-zinc-100 border-b px-2 py-1 dark:border-zinc-900";

/** Rows of a list: its columns' titles, and each row's cells; a row picked by `onRow` when it is given. */
export function Grid({
  label,
  columns,
  rows,
  onRow,
  empty,
}: {
  label: string;
  columns: string[];
  rows: ReactNode[][];
  onRow?: ((index: number) => void) | undefined;
  empty: string;
}) {
  if (rows.length === 0) return <p className="text-zinc-500 dark:text-zinc-400">{empty}</p>;
  return (
    <table aria-label={label} className="min-w-full border-separate border-spacing-0 text-sm">
      <thead>
        <tr>
          {columns.map((c) => (
            <th key={c} className={TH}>
              {c}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((cells, i) => (
          <tr
            // biome-ignore lint/suspicious/noArrayIndexKey: a row is known by its place in what the lake answered.
            key={i}
            onClick={onRow ? () => onRow(i) : undefined}
            className={onRow ? "cursor-pointer hover:bg-zinc-50 dark:hover:bg-zinc-900" : undefined}
          >
            {cells.map((cell, j) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: a cell is known by its column.
              <td key={j} className={TD}>
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** What a view says while it reads, or why it could not. */
function Reading({ error, what }: { error: Error | null; what: string }) {
  if (error) return <Failed title={`The ${what} could not be read`} error={error} />;
  return <p className="px-6 py-4 text-sm text-zinc-500 dark:text-zinc-400">Reading the {what}…</p>;
}

/** The view of a ref, the catalog the tree shows: what it is and holds, its schemas, and its commits. */
export function RefView({ on, onSchema }: { on: string; onSchema?: (namespace: string[]) => void }) {
  const details = useQuery({ queryKey: [...CATALOG, "ref", on], queryFn: () => refDetails(on) });
  const history = useQuery({ queryKey: [...CATALOG, "commits", on], queryFn: () => commits(on) });
  const d = details.data;
  if (!d) return <Reading error={details.error} what="catalog" />;
  const tag = d.kind === "tag";
  return (
    <View
      kind={tag ? "Tag" : "Branch"}
      name={d.name}
      badge={tag ? "read only" : undefined}
      tabs={[
        {
          title: "Overview",
          body: () => (
            <>
              <Facts
                facts={[
                  ["Kind", d.kind],
                  ["Protected", d.protected ? "yes" : "no"],
                  ["Made from", d.parent ? `${d.parent} at commit ${d.fork_seq}` : "—"],
                  ["Made at commit", d.created_seq],
                  ["Warehouse", d.warehouse],
                  ["Schemas", count(d.schemas.length)],
                  ["Tables", count(d.size.tables)],
                  ["Rows", count(d.size.rows)],
                  ["Size", bytes(d.size.bytes)],
                ]}
              />
              <h2 className="mt-6 mb-2 font-semibold">Schemas</h2>
              <Grid
                label="Schemas"
                columns={["Schema", "Tables", "Rows", "Size"]}
                rows={d.schemas.map((s) => [
                  <span key="n" className="font-mono">
                    {s.namespace.join(".")}
                  </span>,
                  count(s.size.tables),
                  count(s.size.rows),
                  bytes(s.size.bytes),
                ])}
                onRow={
                  onSchema
                    ? (i) => {
                        const schema = d.schemas[i];
                        if (schema) onSchema(schema.namespace);
                      }
                    : undefined
                }
                empty="No schemas."
              />
            </>
          ),
        },
        {
          title: "History",
          body: () =>
            history.data ? (
              <Grid
                label="Commits"
                columns={["Commit", "When", "Author", "Operation", "Message"]}
                rows={history.data.map((c) => [
                  <span key="s" className="font-mono">
                    {c.seq}
                  </span>,
                  when(c.committed_at),
                  c.author ?? "—",
                  c.operation,
                  c.message ?? "",
                ])}
                empty="No commits."
              />
            ) : (
              <Reading error={history.error} what="commits" />
            ),
        },
      ]}
    />
  );
}
