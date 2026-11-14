// The ref a page shows, a branch or a tag, and what the lake says of its refs and of the tables on it, kept as it is
// (ADR 0007): the Query and Catalog pages pick one each, `main` at first, and no tag.

import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { refDetails, refs, type Table, tables } from "./client";
import { CATALOG } from "./live";

/** The branch every lake has, which a page opens on. */
export const MAIN = "main";

export interface Picked {
  /** The ref shown: the tag picked, or else the branch. */
  ref: string;
  /** The branch picked, whose tags may be picked. */
  branch: string;
  /** The tag picked, of the branch's; none at first. */
  tag: string | null;
  /** Picks a branch, and no tag. */
  pickBranch: (branch: string) => void;
  /** Picks a tag of the branch, or none: the branch again. */
  pickTag: (tag: string | null) => void;
  branches: string[];
  /** The tags made from the branch picked: each the branch as it was at a commit of it. */
  tags: string[];
  /** Whether the ref shown is a tag: the lake as it was when tagged, read only. */
  tagged: boolean;
  /** The tables on the ref shown; absent while they are read. */
  tables: Table[] | undefined;
  /** The schemas on the ref shown, by their levels, those without tables too; absent while they are read. */
  schemas: string[][] | undefined;
  failure: Error | null;
}

export function usePicked(): Picked {
  const [branch, setBranch] = useState(MAIN);
  const [tag, setTag] = useState<string | null>(null);
  const ref = tag ?? branch;
  const known = useQuery({ queryKey: [...CATALOG, "refs"], queryFn: () => refs() });
  const branches = known.data?.filter((r) => r.kind === "branch").map((r) => r.name) ?? [MAIN];
  const allTags = known.data?.filter((r) => r.kind === "tag") ?? [];
  const tags = allTags.filter((r) => r.parent === branch).map((r) => r.name);
  const listed = useQuery({ queryKey: [...CATALOG, "tables", ref], queryFn: () => tables(ref) });
  // The ref's details, which the Catalog's view of it reads by the same key: they name every schema, an empty one too.
  const details = useQuery({ queryKey: [...CATALOG, "ref", ref], queryFn: () => refDetails(ref) });
  // A ref deleted while it was picked: back to the branch, or to the one every lake has.
  const branchGone = known.data !== undefined && !branches.includes(branch);
  const tagGone = known.data !== undefined && tag !== null && !tags.includes(tag);
  useEffect(() => {
    if (branchGone) setBranch(MAIN);
  }, [branchGone]);
  useEffect(() => {
    if (tagGone) setTag(null);
  }, [tagGone]);
  return {
    ref,
    branch,
    tag,
    pickBranch: (next) => {
      setBranch(next);
      setTag(null);
    },
    pickTag: setTag,
    branches,
    tags,
    tagged: tag !== null,
    tables: listed.data,
    schemas: details.data?.schemas.map((s) => s.namespace),
    failure: listed.error ?? known.error,
  };
}
