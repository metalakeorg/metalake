// The pages of ADR 0007 §7, as the frame lists them: one per surface of the lake. A page not built yet is in the list
// all the same, so that the frame is whole from the first, and each milestone fills a page it already has.

export interface Page {
  path: "/" | "/refs" | "/catalog" | "/log" | "/sql" | "/history" | "/execution";
  title: string;
  /** What it shows, once built. */
  shows: string;
  built: boolean;
}

export const PAGES: readonly Page[] = [
  { path: "/", title: "Lake", shows: "What the lake is: its catalog, warehouse, versions and refs", built: true },
  { path: "/refs", title: "Refs", shows: "Branches and tags, their parents and fork points", built: false },
  { path: "/catalog", title: "Catalog", shows: "Namespaces and tables, on a ref and at a point", built: false },
  { path: "/log", title: "Log", shows: "A ref's commits, the latest first", built: false },
  { path: "/sql", title: "SQL", shows: "An editor, and the rows a statement answers", built: false },
  { path: "/history", title: "History", shows: "A table's snapshots, its files and its changes", built: false },
  {
    path: "/execution",
    title: "Execution",
    shows: "A statement's plan and its metrics, local or on a cluster",
    built: false,
  },
];
