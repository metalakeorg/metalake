// A page's assets: the meta tables of the catalog on one ref, which the branch selector above them picks, or a tag of
// that branch's, which the tag selector below it picks: the branch as it was when tagged, read only.

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
}: {
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
                <li
                  key={name}
                  className="flex items-center gap-1 truncate rounded px-1 py-0.5 pl-4 font-mono hover:bg-zinc-100 dark:hover:bg-zinc-800"
                >
                  <TableIcon />
                  {onPick ? (
                    <button
                      type="button"
                      aria-current={same(picked, { kind: "table", namespace: levels, name }) ? "true" : undefined}
                      onClick={() => onPick({ kind: "table", namespace: levels, name })}
                      className="min-w-0 flex-1 truncate text-left hover:underline aria-[current]:bg-zinc-200 dark:aria-[current]:bg-zinc-800"
                    >
                      {name}
                    </button>
                  ) : (
                    <span className="truncate">{name}</span>
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

/** An icon of the tree, the size of its text. */
function Icon({ children }: { children: ReactNode }) {
  return (
    <svg
      aria-hidden
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.3"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="size-3.5 shrink-0 text-zinc-500 dark:text-zinc-400"
    >
      {children}
    </svg>
  );
}

/** A catalog: a store of data, as a cylinder. */
function CatalogIcon() {
  return (
    <Icon>
      <ellipse cx="8" cy="3.5" rx="5.5" ry="2" />
      <path d="M2.5 3.5v9c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2v-9" />
      <path d="M2.5 8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2" />
    </Icon>
  );
}

/** A schema: what holds tables, as a folder. */
function SchemaIcon() {
  return (
    <Icon>
      <path d="M1.5 4.5v8a1 1 0 0 0 1 1h11a1 1 0 0 0 1-1v-6.5a1 1 0 0 0-1-1h-6l-1.5-2h-3.5a1 1 0 0 0-1 1z" />
    </Icon>
  );
}

/** A table: rows and columns, as a grid. */
function TableIcon() {
  return (
    <Icon>
      <rect x="1.5" y="2.5" width="13" height="11" rx="1" />
      <path d="M1.5 6h13M1.5 9.5h13M6 6v7.5" />
    </Icon>
  );
}
