// The Notebook (ADR 0008): JupyterLab, run by the Python metalake was installed in, embedded in the UI. This page says
// whether this `metalake` can give one, and how to make it able: without Python there is none; without JupyterLab, the
// line that installs it.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { type NotebookState, notebook } from "./client";
import { Failed } from "./Failed";

export function NotebookPage() {
  const state = useQuery({ queryKey: ["notebook"], queryFn: () => notebook() });
  return (
    <section aria-label="Notebook" className="max-w-2xl p-6">
      <h1 className="font-semibold text-lg">Notebook</h1>
      {state.error && <Failed title="Whether JupyterLab is here could not be told" error={state.error} />}
      {state.isPending && <p className="mt-4 text-sm text-zinc-500 dark:text-zinc-400">Looking for JupyterLab…</p>}
      {state.data && <Said state={state.data} again={() => state.refetch()} looking={state.isFetching} />}
    </section>
  );
}

/** What this `metalake` can do, and what to do about it. */
export function Said({ state, again, looking }: { state: NotebookState; again: () => void; looking: boolean }) {
  const text = "mt-2 text-sm text-zinc-600 dark:text-zinc-400";
  if (!state.python) {
    return (
      <div role="status" className="mt-4">
        <p className="font-medium">This metalake runs without Python</p>
        <p className={text}>
          The Notebook runs in the Python metalake was installed in. Install metalake with pip, then run{" "}
          <code>metalake ui</code> from it.
        </p>
      </div>
    );
  }
  if (!state.jupyterlab) {
    return (
      <div role="status" className="mt-4">
        <p className="font-medium">JupyterLab isn't installed</p>
        <p className={text}>The Notebook is JupyterLab, in the Python metalake was installed in. Install it there:</p>
        <Install line={state.install} />
        <button
          type="button"
          onClick={again}
          className="mt-4 rounded border border-zinc-300 px-3 py-1 text-sm hover:bg-zinc-100 dark:border-zinc-700 dark:hover:bg-zinc-800"
        >
          {looking ? "Looking…" : "Check again"}
        </button>
      </div>
    );
  }
  return (
    <div role="status" className="mt-4">
      <p className="font-medium">JupyterLab is installed</p>
      <p className={text}>The Notebook starts it here.</p>
    </div>
  );
}

/** The line that installs it, to copy. */
function Install({ line }: { line: string }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    void navigator.clipboard?.writeText(line).then(() => setCopied(true));
  };
  return (
    <div className="mt-3 flex items-center gap-2 rounded border border-zinc-200 bg-zinc-50 px-3 py-2 font-mono text-sm dark:border-zinc-800 dark:bg-zinc-900">
      <code className="flex-1">{line}</code>
      <button
        type="button"
        onClick={copy}
        className="text-xs text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100"
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}
