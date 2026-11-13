// The Query page's assets: the meta tables of the catalog on one branch, which the branch selector above them picks.

/** A meta table of the catalog: its namespace's parts, and its name. */
export interface Table {
  namespace: string[];
  name: string;
}

/** The namespaces of `tables`, each with its tables, both in name order. */
export function byNamespace(tables: readonly Table[]): [string, string[]][] {
  const grouped = new Map<string, string[]>();
  for (const table of tables) {
    const namespace = table.namespace.join(".");
    grouped.set(namespace, [...(grouped.get(namespace) ?? []), table.name]);
  }
  return [...grouped.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([namespace, names]) => [namespace, names.sort((a, b) => a.localeCompare(b))]);
}

/**
 * The panel: the branch, and the tables on it, by namespace. `tables` is absent while they are read, and `failure`
 * says why they could not be.
 */
export function Assets({
  branches,
  branch,
  onBranch,
  tables,
  failure,
}: {
  branches: readonly string[];
  branch: string;
  onBranch: (branch: string) => void;
  tables: readonly Table[] | undefined;
  failure?: Error | null;
}) {
  return (
    <aside aria-label="Assets" className="flex w-60 shrink-0 flex-col border-zinc-200 border-r dark:border-zinc-800">
      <div className="border-zinc-200 border-b p-2 dark:border-zinc-800">
        <label className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
          Branch
          <select
            value={branch}
            onChange={(event) => onBranch(event.target.value)}
            className="min-w-0 flex-1 rounded border border-zinc-300 bg-transparent px-1.5 py-1 font-mono text-sm text-zinc-900 dark:border-zinc-700 dark:text-zinc-100"
          >
            {branches.map((name) => (
              <option key={name} value={name} className="bg-white dark:bg-zinc-900">
                {name}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-2 text-sm">
        <h2 className="px-1 pb-1 font-medium text-xs text-zinc-500 uppercase tracking-wide dark:text-zinc-400">
          Tables
        </h2>
        <Tables tables={tables} failure={failure ?? null} />
      </div>
    </aside>
  );
}

function Tables({ tables, failure }: { tables: readonly Table[] | undefined; failure: Error | null }) {
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
  if (tables.length === 0) {
    return <p className="px-1 text-xs text-zinc-500">No tables on this branch.</p>;
  }
  return (
    <ul aria-label="Tables" className="space-y-2">
      {byNamespace(tables).map(([namespace, names]) => (
        <li key={namespace}>
          <span className="px-1 font-mono text-xs text-zinc-500 dark:text-zinc-400">{namespace}</span>
          <ul aria-label={namespace}>
            {names.map((name) => (
              <li
                key={name}
                className="truncate rounded px-1 py-0.5 pl-3 font-mono hover:bg-zinc-100 dark:hover:bg-zinc-800"
              >
                {name}
              </li>
            ))}
          </ul>
        </li>
      ))}
    </ul>
  );
}
