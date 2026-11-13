import type { ReactNode } from "react";

/** The frame every page sits in: a header naming the product, and the page below it. Empty until the shell is on a
 * lake (ADR 0007, milestone 6). */
export function Frame({ children }: { children?: ReactNode }) {
  return (
    <div className="flex h-full flex-col">
      <header className="flex h-12 shrink-0 items-center gap-2 border-zinc-200 border-b px-4 dark:border-zinc-800">
        <img src="/favicon.svg" alt="" className="size-6" />
        <span className="font-semibold tracking-tight">MetaLake</span>
      </header>
      <main className="min-h-0 flex-1 overflow-auto">{children}</main>
    </div>
  );
}
