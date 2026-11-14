// The Catalog page, where all of the lake is shown: a sidebar of the ref picked, a branch or a tag, its tree, and the
// ways to the lake's lists of branches and tags and of pull requests, which are yet to be built; and the view of what
// the tree picks, the ref's catalog at first.

import { useState } from "react";
import { type Asset, Assets } from "./Assets";
import { usePicked } from "./picked";
import { RefView, SchemaView, TableView } from "./views";

const WAY =
  "w-full rounded border border-zinc-300 px-2 py-1 text-left text-sm disabled:cursor-not-allowed disabled:opacity-50 dark:border-zinc-700";

export function CatalogPage() {
  const picked = usePicked();
  // What the view shows, of the ref picked: a ref picked anew shows its catalog.
  const [shown, setShown] = useState<{ on: string; asset: Asset }>({ on: picked.ref, asset: { kind: "ref" } });
  const asset: Asset = shown.on === picked.ref ? shown.asset : { kind: "ref" };
  const show = (next: Asset) => setShown({ on: picked.ref, asset: next });
  // A ref picked shows its catalog, whatever was shown of it before.
  const pickBranch = (branch: string) => {
    picked.pickBranch(branch);
    setShown({ on: branch, asset: { kind: "ref" } });
  };
  const pickTag = (tag: string | null) => {
    picked.pickTag(tag);
    setShown({ on: tag ?? picked.branch, asset: { kind: "ref" } });
  };
  return (
    <div className="flex h-full">
      <Assets
        branches={picked.branches}
        tags={picked.tags}
        branch={picked.branch}
        tag={picked.tag}
        onBranch={pickBranch}
        onTag={pickTag}
        tables={picked.tables}
        schemas={picked.schemas}
        failure={picked.failure}
        onPick={show}
        picked={asset}
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
      <section aria-label="Catalog" className="min-w-0 flex-1">
        {asset.kind === "ref" && (
          <RefView key={picked.ref} on={picked.ref} onSchema={(namespace) => show({ kind: "schema", namespace })} />
        )}
        {asset.kind === "schema" && (
          <SchemaView
            key={`${picked.ref}${asset.namespace.join("")}`}
            on={picked.ref}
            namespace={asset.namespace}
            onTable={(name) => show({ kind: "table", namespace: asset.namespace, name })}
          />
        )}
        {asset.kind === "table" && (
          <TableView
            key={`${picked.ref}\u001f${asset.namespace.join("\u001f")}\u001f${asset.name}`}
            on={picked.ref}
            namespace={asset.namespace}
            name={asset.name}
          />
        )}
      </section>
    </div>
  );
}
