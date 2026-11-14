// The Catalog's views of an asset, as a catalog of assets shows one: a header, and its tabs. Plain and in the page's
// style, for their look to be shaped later. What each shows is read from the lake and kept as it is (ADR 0007).

import { useQuery } from "@tanstack/react-query";
import { type ReactNode, useState } from "react";
import { commits, refDetails, run, schemaDetails, snapshots, tableDetails } from "./client";
import { Failed } from "./Failed";
import { CATALOG } from "./live";
import { Results } from "./Results";

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

/** The view of a schema: what it holds and its tables, and its properties. */
export function SchemaView({
  on,
  namespace,
  onTable,
}: {
  on: string;
  namespace: string[];
  onTable?: ((name: string) => void) | undefined;
}) {
  const details = useQuery({
    queryKey: [...CATALOG, "schema", on, ...namespace],
    queryFn: () => schemaDetails(on, namespace),
  });
  const d = details.data;
  if (!d) return <Reading error={details.error} what="schema" />;
  const properties = Object.entries(d.properties);
  return (
    <View
      kind="Schema"
      name={d.namespace.join(".")}
      badge={`on ${on}`}
      tabs={[
        {
          title: "Overview",
          body: () => (
            <>
              <Facts
                facts={[
                  ["Levels", d.namespace.length > 1 ? d.namespace.join(" › ") : "1"],
                  ["Tables", count(d.size.tables)],
                  ["Rows", count(d.size.rows)],
                  ["Size", bytes(d.size.bytes)],
                ]}
              />
              <h2 className="mt-6 mb-2 font-semibold">Tables</h2>
              <Grid
                label="Tables"
                columns={["Table", "Rows", "Size", "Last change"]}
                rows={d.tables.map((t) => [
                  <span key="n" className="font-mono">
                    {t.name}
                  </span>,
                  count(t.rows),
                  bytes(t.bytes),
                  when(t.changed_at),
                ])}
                onRow={
                  onTable
                    ? (i) => {
                        const table = d.tables[i];
                        if (table) onTable(table.name);
                      }
                    : undefined
                }
                empty="No tables."
              />
            </>
          ),
        },
        {
          title: "Details",
          body: () => (
            <>
              <h2 className="mb-2 font-semibold">Properties</h2>
              <Grid
                label="Properties"
                columns={["Key", "Value"]}
                rows={properties.map(([k, v]) => [
                  <span key="k" className="font-mono">
                    {k}
                  </span>,
                  <span key="v" className="font-mono">
                    {v}
                  </span>,
                ])}
                empty="No properties."
              />
            </>
          ),
        },
      ]}
    />
  );
}

/** An SQL identifier, quoted: a name of the lake may hold any character. */
function quoted(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

/** How many rows a table's sample shows. */
export const SAMPLE = 100;

/** The view of a table: its columns, a sample of its rows, what it is and holds, and its snapshots. */
export function TableView({ on, namespace, name }: { on: string; namespace: string[]; name: string }) {
  const key = [on, ...namespace, name];
  const details = useQuery({
    queryKey: [...CATALOG, "table", ...key],
    queryFn: () => tableDetails(on, namespace, name),
  });
  const history = useQuery({
    queryKey: [...CATALOG, "snapshots", ...key],
    queryFn: () => snapshots(on, namespace, name),
  });
  // The ref is named in the statement, as the schema and the table are: the sample is of this table on this ref.
  const sql = `select * from ${quoted(on)}.${quoted(namespace.join("."))}.${quoted(name)} limit ${SAMPLE}`;
  const sample = useQuery({
    queryKey: [...CATALOG, "sample", ...key],
    queryFn: async () => {
      const started = performance.now();
      const result = await run(sql, on);
      return { result, ms: performance.now() - started };
    },
  });
  const d = details.data;
  if (!d) return <Reading error={details.error} what="table" />;
  // A nested column says the columns it is a field of before its name.
  const byId = new Map(d.columns.map((c) => [c.id, c]));
  const path = (id: number | null | undefined): string[] => {
    const column = id == null ? undefined : byId.get(id);
    return column ? [...path(column.parent), column.name] : [];
  };
  const iceberg = d.iceberg
    ? d.iceberg.behind === 0
      ? "up to date"
      : `${count(d.iceberg.behind)} commits behind, since ${when(d.iceberg.since)}`
    : "not written on commit";
  const json = (value: unknown) => (value == null ? "none" : JSON.stringify(value));
  return (
    <View
      kind="Table"
      name={`${d.namespace.join(".")}.${d.name}`}
      badge={`on ${on}`}
      tabs={[
        {
          title: "Columns",
          body: () => (
            <Grid
              label="Columns"
              columns={["Column", "Type", "Required", "Default", "Comment"]}
              rows={d.columns.map((c) => [
                <span key="n" className="font-mono">
                  {path(c.id).join(".")}
                </span>,
                <span key="t" className="font-mono">
                  {c.type}
                </span>,
                c.required ? "yes" : "no",
                <span key="d" className="font-mono">
                  {c.default == null ? "" : JSON.stringify(c.default)}
                </span>,
                c.doc ?? "",
              ])}
              empty="No columns."
            />
          ),
        },
        {
          title: "Sample data",
          body: () => (
            <div className="-mx-6 -my-4 flex h-full flex-col">
              <Results
                state={
                  sample.data
                    ? { kind: "done", result: sample.data.result, ms: sample.data.ms }
                    : sample.error
                      ? { kind: "failed", error: sample.error }
                      : { kind: "running" }
                }
              />
            </div>
          ),
        },
        {
          title: "Details",
          body: () => (
            <>
              <Facts
                facts={[
                  ["Rows", count(d.rows)],
                  ["Size", bytes(d.bytes)],
                  ["Files", count(d.files)],
                  ["Inline files", count(d.inline_files)],
                  ["Deleted rows", count(d.deleted_rows)],
                  ["Partition", json(d.partition)],
                  ["Sort order", json(d.sort_order)],
                  ["Clustering", d.cluster_key.length ? d.cluster_key.join(", ") : "none"],
                  ["Location", d.location],
                  ["Id", d.id],
                  ["Iceberg", iceberg],
                  ["Iceberg metadata", d.iceberg?.metadata ?? "—"],
                ]}
              />
              <h2 className="mt-6 mb-2 font-semibold">Properties</h2>
              <Grid
                label="Properties"
                columns={["Key", "Value"]}
                rows={Object.entries(d.properties).map(([k, v]) => [
                  <span key="k" className="font-mono">
                    {k}
                  </span>,
                  <span key="v" className="font-mono">
                    {v}
                  </span>,
                ])}
                empty="No properties."
              />
            </>
          ),
        },
        {
          title: "History",
          body: () =>
            history.data ? (
              <Grid
                label="Snapshots"
                columns={["Snapshot", "Parent", "Operation", "When"]}
                rows={history.data.map((s) => [
                  <span key="s" className="font-mono">
                    {s.id}
                  </span>,
                  <span key="p" className="font-mono">
                    {s.parent ?? "—"}
                  </span>,
                  s.operation ?? "—",
                  when(s.committed_at),
                ])}
                empty="No snapshots."
              />
            ) : (
              <Reading error={history.error} what="snapshots" />
            ),
        },
      ]}
    />
  );
}
