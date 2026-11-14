// The Query page's assets: the meta tables of the catalog on one ref, which the branch selector above them picks, or
// a tag of the list below it: the lake as it was when tagged, read only.

import type { ReactNode } from "react";

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
 * The panel: the branch, the tags, and the tables of the ref picked, by namespace. `branch` is the ref picked, a
 * branch or a tag; `tables` is absent while they are read, and `failure` says why they could not be.
 */
export function Assets({
  branches,
  tags = [],
  branch,
  onBranch,
  tables,
  failure,
  children,
  onPick,
  picked,
}: {
  /** Makes each name of the tree pick what it names, which the page then shows. */
  onPick?: ((asset: Asset) => void) | undefined;
  picked?: Asset | undefined;
  branches: readonly string[];
  tags?: readonly string[];
  /** What a page adds below the refs, above the tree. */
  children?: ReactNode;
  branch: string;
  onBranch: (branch: string) => void;
  tables: readonly Table[] | undefined;
  failure?: Error | null;
}) {
  const tagged = tags.includes(branch);
  return (
    <aside aria-label="Assets" className="flex w-60 shrink-0 flex-col border-zinc-200 border-r dark:border-zinc-800">
      <div className="border-zinc-200 border-b p-2 dark:border-zinc-800">
        <label className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
          Branch
          <select
            value={tagged ? "" : branch}
            onChange={(event) => onBranch(event.target.value)}
            className="min-w-0 flex-1 rounded border border-zinc-300 bg-transparent px-1.5 py-1 font-mono text-sm text-zinc-900 dark:border-zinc-700 dark:text-zinc-100"
          >
            {tagged && (
              <option value="" disabled className="bg-white dark:bg-zinc-900">
                —
              </option>
            )}
            {branches.map((name) => (
              <option key={name} value={name} className="bg-white dark:bg-zinc-900">
                {name}
              </option>
            ))}
          </select>
        </label>
        <div className="mt-2">
          <p className="text-xs text-zinc-500 dark:text-zinc-400">Tags</p>
          {tags.length === 0 ? (
            <p className="mt-1 px-1 text-xs text-zinc-400 dark:text-zinc-500">No tags.</p>
          ) : (
            <ul aria-label="Tags" className="mt-1 max-h-32 overflow-auto">
              {tags.map((name) => (
                <li key={name}>
                  <button
                    type="button"
                    aria-pressed={name === branch}
                    onClick={() => onBranch(name)}
                    className="w-full truncate rounded px-1 py-0.5 text-left font-mono text-sm hover:bg-zinc-100 aria-pressed:bg-zinc-200 dark:aria-pressed:bg-zinc-800 dark:hover:bg-zinc-800"
                  >
                    {name}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
        {children}
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-2 text-sm">
        <Tables
          branch={branch}
          tagged={tagged}
          tables={tables}
          failure={failure ?? null}
          onPick={onPick}
          picked={picked}
        />
      </div>
    </aside>
  );
}

/** A node of the tree that folds: its name, and what it holds below it, open at first. Its name picks it, if it may. */
function Node({
  kind,
  name,
  children,
  onName,
  current,
}: {
  kind: string;
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
        <span className="ml-auto text-[10px] text-zinc-400 uppercase tracking-wide dark:text-zinc-500">{kind}</span>
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
}: {
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
      kind={tagged ? "tag · read only" : "catalog"}
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
            kind="schema"
            name={namespace}
            onName={onPick && (() => onPick({ kind: "schema", namespace: levels }))}
            current={same(picked, { kind: "schema", namespace: levels })}
          >
            <ul aria-label={namespace}>
              {names.map((name) => (
                <li
                  key={name}
                  className="truncate rounded px-1 py-0.5 pl-4 font-mono hover:bg-zinc-100 dark:hover:bg-zinc-800"
                >
                  {onPick ? (
                    <button
                      type="button"
                      aria-current={same(picked, { kind: "table", namespace: levels, name }) ? "true" : undefined}
                      onClick={() => onPick({ kind: "table", namespace: levels, name })}
                      className="w-full truncate text-left hover:underline aria-[current]:bg-zinc-200 dark:aria-[current]:bg-zinc-800"
                    >
                      {name}
                    </button>
                  ) : (
                    name
                  )}
                </li>
              ))}
            </ul>
          </Node>
        );
      })}
    </Node>
  );
}
