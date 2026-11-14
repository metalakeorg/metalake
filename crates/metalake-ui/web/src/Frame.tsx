import type { ReactNode } from "react";
import { Grapes } from "./Grapes";
import type { Server } from "./server";

/** The frame every page sits in: a header naming the product and the server's state; the sidebar beside the page; or,
 * in place of both, what keeps the page from showing. */
export function Frame({ server, nav, children }: { server?: Server; nav?: ReactNode; children?: ReactNode }) {
  const ready = server === undefined || server.state === "ready";
  return (
    <div className="flex h-full flex-col">
      <header className="flex h-12 shrink-0 items-center gap-2 border-zinc-200 border-b px-4 dark:border-zinc-800">
        <Grapes className="size-7" />
        <span className="font-semibold tracking-tight">Metalake UI</span>
      </header>
      <div className="flex min-h-0 flex-1">
        {ready && nav}
        <main className="min-h-0 flex-1 overflow-auto">{ready ? children : <Blocked server={server} />}</main>
      </div>
    </div>
  );
}

/** What the page says when it cannot show. */
function Blocked({ server }: { server: Server | undefined }) {
  switch (server?.state) {
    case undefined:
    case "ready":
    case "asking":
      return null;
    case "unauthorized":
      return (
        <Notice title="This page lacks the run's token">
          Open the URL that <code>metalake ui</code> printed in its terminal: it carries the token of this run, which a
          page of an earlier run, or one opened by hand, does not have.
        </Notice>
      );
    case "other":
      return (
        <Notice title="The server is of another version">
          It speaks version {server.api} of the UI's API. Reload the page: the server serves its own frontend.
        </Notice>
      );
    case "unreachable":
      return (
        <Notice title="The server does not answer">
          Is <code>metalake ui</code> still running? ({server.message})
        </Notice>
      );
  }
}

export function Notice({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section role="alert" className="mx-auto mt-16 max-w-lg px-4">
      <h1 className="font-semibold text-lg">{title}</h1>
      <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-400">{children}</p>
    </section>
  );
}
