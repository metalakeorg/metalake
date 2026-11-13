import type { ReactNode } from "react";
import type { Server } from "./server";

/** The frame every page sits in: a header naming the product and the server's state, and the page below it, or what
 * keeps the page from showing. */
export function Frame({ server, children }: { server?: Server; children?: ReactNode }) {
  return (
    <div className="flex h-full flex-col">
      <header className="flex h-12 shrink-0 items-center gap-2 border-zinc-200 border-b px-4 dark:border-zinc-800">
        <img src="/favicon.svg" alt="" className="size-6" />
        <span className="font-semibold tracking-tight">MetaLake</span>
        {server?.state === "ready" && (
          <span className="ml-auto text-xs text-zinc-500 dark:text-zinc-400">API {server.api}</span>
        )}
      </header>
      <main className="min-h-0 flex-1 overflow-auto">
        {server === undefined || server.state === "ready" ? children : <Blocked server={server} />}
      </main>
    </div>
  );
}

/** What the page says when it cannot show. */
function Blocked({ server }: { server: Exclude<Server, { state: "ready" }> }) {
  switch (server.state) {
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

function Notice({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section role="alert" className="mx-auto mt-16 max-w-lg px-4">
      <h1 className="font-semibold text-lg">{title}</h1>
      <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-400">{children}</p>
    </section>
  );
}
