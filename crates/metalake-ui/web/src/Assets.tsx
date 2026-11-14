// A page's assets: the meta tables of the catalog on one ref, which the branch selector above them picks, or a tag of
// that branch's, which the tag selector below it picks: the branch as it was when tagged, read only.

import { useQuery } from "@tanstack/react-query";
import { type ReactNode, useState } from "react";
import { type Column, tableDetails } from "./client";
import { CatalogIcon, SchemaIcon, TableIcon, TypeIcon } from "./icons";
import { CATALOG } from "./live";

/** What a page may show of the tree: the ref's catalog, a schema by its levels, or a table. */
export type Asset =
  | { kind: "ref" }
  | { kind: "schema"; namespace: string[] }
  | { kind: "table"; namespace: string[]; name: string };

/** Whether two assets are the same one. */
export function same(a: Asset | undefined, b: Asset): boolean {
  if (!a || a.kind !== b.kind) return false;
  if (a.kind === "ref") return true;
  const levels = (x: Asset) => (x.kind === "ref" ? "" : x.namespace.join("\u001f"));
  if (levels(a) !== levels(b)) return false;
  return a.kind !== "table" || (b.kind === "table" && a.name === b.name);
}

/** A meta table of the catalog: its namespace's parts, and its name. */
export interface Table {
  namespace: string[];
  name: string;
}

/** The namespaces of `tables`, each with its tables, both in name order. */
export function byNamespace(tables: readonly Table[]): [string, string[], string[]][] {
  // Grouped by the levels, which a dot inside one may not tell apart once they are joined by dots.
  const grouped = new Map<string, { levels: string[]; names: string[] }>();
  for (const table of tables) {
    const key = table.namespace.join("\u001f");
    const group = grouped.get(key) ?? { levels: table.namespace, names: [] };
    group.names.push(table.name);
    grouped.set(key, group);
  }
  return [...grouped.values()]
    .map(({ levels, names }): [string, string[], string[]] => [
      levels.join("."),
      names.sort((a, b) => a.localeCompare(b)),
      levels,
    ])
    .sort(([a, , x], [b, , y]) => a.localeCompare(b) || x.length - y.length);
}

/**
 * The panel: the branch, its tags, and the tables of the ref shown, by namespace: the tag picked, or else the branch.
 * `tables` is absent while they are read, and `failure` says why they could not be.
 */
export function Assets({
  branches,
  tags = [],
  branch,
  tag = null,
  onBranch,
  onTag,
  tables,
  failure,
  children,
  onPick,
  picked,
  columns = false,
}: {
  /** Makes each table of the tree fold open on its columns. */
  columns?: boolean;
  /** Makes each name of the tree pick what it names, which the page then shows. */
  onPick?: ((asset: Asset) => void) | undefined;
  picked?: Asset | undefined;
  branches: readonly string[];
  /** The tags made from the branch. */
  tags?: readonly string[];
  /** What a page adds below the refs, above the tree. */
  children?: ReactNode;
  branch: string;
  /** The tag picked, of the branch's; none shows the branch. */
  tag?: string | null;
  onBranch: (branch: string) => void;
  onTag?: (tag: string | null) => void;
  tables: readonly Table[] | undefined;
  failure?: Error | null;
}) {
  const tagged = tag !== null;
  return (
    <aside aria-label="Assets" className="flex w-60 shrink-0 flex-col border-zinc-200 border-r dark:border-zinc-800">
      <div className="border-zinc-200 border-b p-2 dark:border-zinc-800">
        <label className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
          <span className="w-12">Branch</span>
          <select
            value={branch}
            onChange={(event) => onBranch(event.target.value)}
            className="min-w-0 flex-1 rounded border border-zinc-300 bg-transparent px-1.5 py-1 font-mono text-sm text-zinc-900 disabled:opacity-50 dark:border-zinc-700 dark:text-zinc-100"
          >
            {branches.map((name) => (
              <option key={name} value={name} className="bg-white dark:bg-zinc-900">
                {name}
              </option>
            ))}
          </select>
        </label>
        <label className="mt-2 flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
          <span className="w-12">Tag</span>
          <select
            value={tag ?? ""}
            disabled={tags.length === 0}
            onChange={(event) => onTag?.(event.target.value || null)}
            className="min-w-0 flex-1 rounded border border-zinc-300 bg-transparent px-1.5 py-1 font-mono text-sm text-zinc-900 disabled:opacity-50 dark:border-zinc-700 dark:text-zinc-100"
          >
            <option value="" className="bg-white dark:bg-zinc-900">
              {tags.length === 0 ? "no tags" : "—"}
            </option>
            {tags.map((name) => (
              <option key={name} value={name} className="bg-white dark:bg-zinc-900">
                {name}
              </option>
            ))}
          </select>
        </label>
        {children}
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-2 text-sm">
        <Tables
          branch={tag ?? branch}
          tagged={tagged}
          tables={tables}
          failure={failure ?? null}
          onPick={onPick}
          picked={picked}
          columns={columns}
        />
      </div>
    </aside>
  );
}

/** A node of the tree that folds: its name, and what it holds below it, open at first. Its name picks it, if it may. */
function Node({
  icon,
  note,
  name,
  children,
  onName,
  current,
}: {
  icon: ReactNode;
  /** A word on it, at its right. */
  note?: string | undefined;
  name: string;
  children: ReactNode;
  onName?: (() => void) | undefined;
  current?: boolean | undefined;
}) {
  return (
    <details open className="group">
      <summary className="flex cursor-pointer list-none items-center gap-1 rounded px-1 py-0.5 hover:bg-zinc-100 dark:hover:bg-zinc-800">
        <span aria-hidden className="w-3 text-zinc-400 text-xs transition-transform group-open:rotate-90">
          ▸
        </span>
        {icon}
        {onName ? (
          <button
            type="button"
            aria-current={current ? "true" : undefined}
            onClick={(event) => {
              // Picks it, and leaves it folded as it was.
              event.preventDefault();
              onName();
            }}
            className="truncate rounded px-0.5 font-mono hover:underline aria-[current]:bg-zinc-200 dark:aria-[current]:bg-zinc-800"
          >
            {name}
          </button>
        ) : (
          <span className="truncate font-mono">{name}</span>
        )}
        {note && (
          <span className="ml-auto text-[10px] text-amber-700 uppercase tracking-wide dark:text-amber-400">{note}</span>
        )}
      </summary>
      <div className="ml-2 border-zinc-200 border-l pl-2 dark:border-zinc-800">{children}</div>
    </details>
  );
}

/** The tree of what the ref holds: its catalog, which is the ref itself; its schemas; their tables. */
function Tables({
  branch,
  tagged,
  tables,
  failure,
  onPick,
  picked,
  columns,
}: {
  columns: boolean;
  branch: string;
  tagged: boolean;
  tables: readonly Table[] | undefined;
  failure: Error | null;
  onPick?: ((asset: Asset) => void) | undefined;
  picked?: Asset | undefined;
}) {
  if (failure) {
    return (
      <p role="alert" className="px-1 text-red-700 text-xs dark:text-red-400">
        metalake: {failure.message}
      </p>
    );
  }
  if (tables === undefined) {
    return <p className="px-1 text-xs text-zinc-500">Reading the catalog…</p>;
  }
  return (
    <Node
      icon={<CatalogIcon />}
      note={tagged ? "read only" : undefined}
      name={branch}
      onName={onPick && (() => onPick({ kind: "ref" }))}
      current={same(picked, { kind: "ref" })}
    >
      {tables.length === 0 && (
        <p className="px-1 py-0.5 text-xs text-zinc-500">No tables on this {tagged ? "tag" : "branch"}.</p>
      )}
      {byNamespace(tables).map(([namespace, names, levels]) => {
        return (
          <Node
            key={levels.join("\u001f")}
            icon={<SchemaIcon />}
            name={namespace}
            onName={onPick && (() => onPick({ kind: "schema", namespace: levels }))}
            current={same(picked, { kind: "schema", namespace: levels })}
          >
            <ul aria-label={namespace}>
              {names.map((name) => (
                <TableNode
                  key={name}
                  on={branch}
                  levels={levels}
                  name={name}
                  columns={columns}
                  onPick={onPick && (() => onPick({ kind: "table", namespace: levels, name }))}
                  current={same(picked, { kind: "table", namespace: levels, name })}
                />
              ))}
            </ul>
          </Node>
        );
      })}
    </Node>
  );
}

const ROW = "flex items-center gap-1 rounded px-1 py-0.5 font-mono hover:bg-zinc-100 dark:hover:bg-zinc-800";

/** A table of the tree: its name, which may pick it, and its columns, folded at first, if it may show them. */
function TableNode({
  on,
  levels,
  name,
  columns,
  onPick,
  current,
}: {
  on: string;
  levels: string[];
  name: string;
  columns: boolean;
  onPick?: (() => void) | undefined;
  current: boolean;
}) {
  const [open, setOpen] = useState(false);
  const label = onPick ? (
    <button
      type="button"
      aria-current={current ? "true" : undefined}
      onClick={(event) => {
        event.preventDefault();
        onPick();
      }}
      className="min-w-0 flex-1 truncate text-left hover:underline aria-[current]:bg-zinc-200 dark:aria-[current]:bg-zinc-800"
    >
      {name}
    </button>
  ) : (
    <span className="min-w-0 flex-1 truncate">{name}</span>
  );
  if (!columns) {
    return (
      <li className={`${ROW} pl-4`}>
        <TableIcon />
        {label}
      </li>
    );
  }
  return (
    <li>
      <details open={open} onToggle={(event) => setOpen(event.currentTarget.open)} className="group/table">
        <summary aria-label={name} className={`${ROW} cursor-pointer list-none`}>
          <span aria-hidden className="w-3 text-zinc-400 text-xs transition-transform group-open/table:rotate-90">
            ▸
          </span>
          <TableIcon />
          {label}
        </summary>
        {open && <Columns on={on} levels={levels} name={name} />}
      </details>
    </li>
  );
}

/** A table's columns, read when it is unfolded; a nested one below the column it is a field of. */
function Columns({ on, levels, name }: { on: string; levels: string[]; name: string }) {
  // The key the Catalog's view of the table reads it by: one read serves both.
  const details = useQuery({
    queryKey: [...CATALOG, "table", on, ...levels, name],
    queryFn: () => tableDetails(on, levels, name),
  });
  if (details.error) {
    return <p className="py-0.5 pl-8 text-red-700 text-xs dark:text-red-400">metalake: {details.error.message}</p>;
  }
  if (!details.data) return <p className="py-0.5 pl-8 text-xs text-zinc-500">Reading the columns…</p>;
  const all = details.data.columns;
  const below = (parent: number | null) => all.filter((c) => (c.parent ?? null) === parent);
  const rows = (parent: number | null, depth: number): ReactNode[] =>
    below(parent).flatMap((c) => [<ColumnRow key={c.id} column={c} depth={depth} />, ...rows(c.id, depth + 1)]);
  return <ul aria-label={`Columns of ${name}`}>{rows(null, 0)}</ul>;
}

function ColumnRow({ column, depth }: { column: Column; depth: number }) {
  return (
    <li
      title={`${column.name}: ${column.type}${column.required ? ", required" : ""}`}
      style={{ paddingLeft: `${2 + depth}rem` }}
      className="flex items-center gap-1 rounded py-0.5 pr-1 font-mono text-xs hover:bg-zinc-100 dark:hover:bg-zinc-800"
    >
      <TypeIcon type={column.type} />
      <span className="min-w-0 flex-1 truncate">{column.name}</span>
      <span className="shrink-0 text-[10px] text-zinc-400 dark:text-zinc-500">{column.type}</span>
    </li>
  );
}
