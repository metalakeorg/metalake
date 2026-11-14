// The ref a page shows, a branch or a tag, and what the lake says of its refs and of the tables on it, kept as it is
// (ADR 0007): the Query and Catalog pages pick one each, `main` at first.

import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { refs, type Table, tables } from "./client";
import { CATALOG } from "./live";

/** The branch every lake has, which a page opens on. */
export const MAIN = "main";

export interface Picked {
  /** The ref picked: a branch, or a tag. */
  ref: string;
  pick: (ref: string) => void;
  branches: string[];
  tags: string[];
  /** Whether the ref picked is a tag: the lake as it was when tagged, read only. */
  tagged: boolean;
  /** The tables on the ref picked; absent while they are read. */
  tables: Table[] | undefined;
  failure: Error | null;
}

export function usePicked(): Picked {
  const [ref, pick] = useState(MAIN);
  const known = useQuery({ queryKey: [...CATALOG, "refs"], queryFn: () => refs() });
  const branches = known.data?.filter((r) => r.kind === "branch").map((r) => r.name) ?? [MAIN];
  const tags = known.data?.filter((r) => r.kind === "tag").map((r) => r.name) ?? [];
  const listed = useQuery({ queryKey: [...CATALOG, "tables", ref], queryFn: () => tables(ref) });
  // A ref deleted while it was picked: back to the one every lake has.
  const gone = known.data !== undefined && !branches.includes(ref) && !tags.includes(ref);
  useEffect(() => {
    if (gone) pick(MAIN);
  }, [gone]);
  return {
    ref,
    pick,
    branches,
    tags,
    tagged: tags.includes(ref),
    tables: listed.data,
    failure: listed.error ?? known.error,
  };
}
