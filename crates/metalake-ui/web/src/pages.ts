// The pages the sidebar lists, in its order. Query is the first, and where the UI opens; the Notebook (ADR 0008); and
// the Catalog, where all of the lake is shown.

export interface Page {
  path: "/" | "/notebook" | "/catalog";
  title: string;
}

export const PAGES: readonly Page[] = [
  { path: "/", title: "Query" },
  { path: "/notebook", title: "Notebook" },
  { path: "/catalog", title: "Catalog" },
];
