// The frontend's routes: one per page of the sidebar, under the frame. Paths are the server's too: any path that is not
// under `/api` gets `index.html`, so a page's address may be bookmarked and reloaded (ADR 0007 §4.3).

import { createRootRoute, createRoute, createRouter, Link, Outlet } from "@tanstack/react-router";
import { CatalogPage } from "./Catalog";
import { Frame, Notice } from "./Frame";
import { useLive } from "./live";
import { NotebookPage } from "./Notebook";
import { PAGES } from "./pages";
import { QueryPage } from "./Query";
import { useServer } from "./server";

function Shell() {
  const server = useServer();
  // The catalog shown is kept as the lake is, once the server is known to answer this page.
  useLive(server.state === "ready");
  return (
    <Frame server={server} nav={<Nav />}>
      <Outlet />
    </Frame>
  );
}

function Nav() {
  return (
    <nav aria-label="Pages" className="w-44 shrink-0 border-zinc-200 border-r p-2 dark:border-zinc-800">
      <ul className="space-y-0.5 text-sm">
        {PAGES.map((page) => (
          <li key={page.path}>
            <Link
              to={page.path}
              className="block rounded px-2 py-1 text-zinc-700 hover:bg-zinc-100 dark:text-zinc-300 dark:hover:bg-zinc-800"
              activeProps={{ className: "bg-zinc-100 font-medium text-zinc-950 dark:bg-zinc-800 dark:text-zinc-50" }}
              activeOptions={{ exact: true }}
            >
              {page.title}
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  );
}

const root = createRootRoute({
  component: Shell,
  notFoundComponent: () => (
    <Notice title="No such page">
      This address is not a page of MetaLake's UI. <Link to="/">Go to Query</Link>.
    </Notice>
  ),
});

const query = createRoute({ getParentRoute: () => root, path: "/", component: QueryPage });
const notebook = createRoute({ getParentRoute: () => root, path: "/notebook", component: NotebookPage });
const catalog = createRoute({ getParentRoute: () => root, path: "/catalog", component: CatalogPage });

export const router = createRouter({ routeTree: root.addChildren([query, notebook, catalog]) });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
