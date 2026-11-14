// The Catalog page, where all of the lake is shown: a sidebar of the ref picked, a branch or a tag, its tree, and the
// ways to the lake's lists of branches and tags and of pull requests, which are yet to be built.

import { Assets } from "./Assets";
import { usePicked } from "./picked";

const WAY =
  "w-full rounded border border-zinc-300 px-2 py-1 text-left text-sm disabled:cursor-not-allowed disabled:opacity-50 dark:border-zinc-700";

export function CatalogPage() {
  const picked = usePicked();
  return (
    <div className="flex h-full">
      <Assets
        branches={picked.branches}
        tags={picked.tags}
        branch={picked.ref}
        onBranch={picked.pick}
        tables={picked.tables}
        failure={picked.failure}
      >
        <nav aria-label="Catalog views" className="mt-3 flex flex-col gap-1.5">
          <button type="button" disabled title="Not built yet" className={WAY}>
            Branches &amp; tags
          </button>
          <button type="button" disabled title="Not built yet" className={WAY}>
            Pull requests
          </button>
        </nav>
      </Assets>
      <section aria-label="Catalog" className="min-w-0 flex-1" />
    </div>
  );
}
