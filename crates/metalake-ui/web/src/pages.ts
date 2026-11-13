// The pages the sidebar lists, in its order. Query is the first, and where the UI opens; the catalog, and all of the
// lake it shows, comes next, once it is designed.

export interface Page {
  path: "/";
  title: string;
}

export const PAGES: readonly Page[] = [{ path: "/", title: "Query" }];
