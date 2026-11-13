// The frontend's routes: one per page of `PAGES`, under the frame (ADR 0007 §7). Paths are the server's too: any path
// that is not under `/api` gets `index.html`, so a page's address may be bookmarked and reloaded (§4.3).

import { useQuery } from "@tanstack/react-query";
import { createRootRoute, createRoute, createRouter, Link, Outlet } from "@tanstack/react-router";
import { Frame, Notice } from "./Frame";
import { LakePage, lakeQuery } from "./Lake";
import { PAGES, type Page } from "./pages";
import { useServer } from "./server";

function Shell() {
  const server = useServer();
  const lake = useQuery({ ...lakeQuery, enabled: server.state === "ready" });
  return (
    <Frame server={server} lake={lake.data} nav={<Nav />}>
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

/** A page whose surface is not built yet. */
export function Unbuilt({ page }: { page: Page }) {
  return (
    <section className="max-w-3xl p-6">
      <h1 className="font-semibold text-lg">{page.title}</h1>
      <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-400">{page.shows}.</p>
      <p className="mt-4 text-sm text-zinc-500">Not built yet: it comes with a later version of MetaLake.</p>
    </section>
  );
}

const root = createRootRoute({
  component: Shell,
  notFoundComponent: () => (
    <Notice title="No such page">
      This address is not a page of MetaLake's UI. <Link to="/">Go to the lake</Link>.
    </Notice>
  ),
});

const pages = PAGES.map((page) =>
  createRoute({
    getParentRoute: () => root,
    path: page.path,
    component: page.built ? LakePage : () => <Unbuilt page={page} />,
  }),
);

export const router = createRouter({ routeTree: root.addChildren(pages) });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
