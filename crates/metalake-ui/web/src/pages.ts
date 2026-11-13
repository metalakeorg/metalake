// The pages the sidebar lists, in its order. Query is the first, and where the UI opens; the catalog, and all of the
// lake it shows, comes next, once it is designed; the Notebook after it (ADR 0008).

export interface Page {
  path: "/" | "/notebook";
  title: string;
}

export const PAGES: readonly Page[] = [
  { path: "/", title: "Query" },
  { path: "/notebook", title: "Notebook" },
];
