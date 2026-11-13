// The Notebook's ref selector (ADR 0008 §6): in each notebook's toolbar, the ref its cells read and write on, of the
// lake's branches and tags. It holds no state of the lake: `lake`, metalake-py in the notebook's kernel, does. The
// selector asks it, by silent executions that leave nothing in the notebook, and tells it, with `lake.on(…)`.
//
// The notebook keeps its ref in its metadata (`metadata.metalake.ref`); one with none is on `main`. It is applied when
// the notebook opens and each time its kernel starts again, since a new kernel's `lake` starts on `main`, and when the
// selector changes. After a cell runs, the selector reads `lake.ref` again, so that a `lake.on(…)` of a cell is seen
// and kept as the notebook's.

import type { JupyterFrontEnd, JupyterFrontEndPlugin } from "@jupyterlab/application";
import { Notification } from "@jupyterlab/apputils";
import type { DocumentRegistry } from "@jupyterlab/docregistry";
import { type INotebookModel, NotebookActions, type NotebookPanel } from "@jupyterlab/notebook";
import type { Kernel, KernelMessage } from "@jupyterlab/services";
import { DisposableDelegate, type IDisposable } from "@lumino/disposable";
import { Widget } from "@lumino/widgets";

/** The key of the notebook's metadata that keeps its ref. */
const KEY = "metalake";
/** The ref of a notebook that keeps none. */
const MAIN = "main";

/** What `lake` says: the ref it is on and, when asked, the lake's refs. */
interface State {
  ref: string;
  refs?: [string, "branch" | "tag"][];
}

/** What a silent execution answered: the state, or why there is none. */
type Answer = { state: State } | { error: { name: string; value: string } };

/** The expression the kernel evaluates, as JSON: the ref `lake` is on, and with `all`, every ref of the lake. */
function expression(all: boolean): string {
  const refs = all ? ', "refs": [[r.name, r.kind] for r in lake.refs()]' : "";
  return `__import__("IPython.display", fromlist=["JSON"]).JSON({"ref": lake.ref${refs}})`;
}

/** Runs `code` silently in the kernel, then evaluates the state. */
async function ask(kernel: Kernel.IKernelConnection, code: string, all: boolean): Promise<Answer> {
  const future = kernel.requestExecute({
    code,
    silent: true,
    store_history: false,
    allow_stdin: false,
    stop_on_error: false,
    user_expressions: { state: expression(all) },
  });
  const reply = (await future.done) as KernelMessage.IExecuteReplyMsg;
  const content = reply.content;
  if (content.status === "error") {
    return { error: { name: content.ename, value: content.evalue } };
  }
  if (content.status !== "ok") {
    return { error: { name: content.status, value: "the kernel did not run it" } };
  }
  const evaluated = content.user_expressions.state as {
    status: string;
    data?: Record<string, unknown>;
    ename?: string;
    evalue?: string;
  };
  if (evaluated?.status !== "ok") {
    return { error: { name: evaluated?.ename ?? "error", value: evaluated?.evalue ?? "" } };
  }
  return { state: evaluated.data?.["application/json"] as State };
}

/** The selector of one notebook, in its toolbar. */
export class RefSelector extends Widget {
  private readonly select: HTMLSelectElement;
  /** Set once its kernel starts again, so that the notebook's ref is applied once it is idle. */
  private fresh = false;
  /** The missing ref last said, so that a notebook opened, whose ref is applied as it opens and as its kernel is
   * ready, says it once. */
  private warned = "";
  /** The refs last listed, kept so that a choice shows among them while the list is asked again. */
  private listed: [string, "branch" | "tag"][] = [[MAIN, "branch"]];

  constructor(private readonly panel: NotebookPanel) {
    super();
    this.addClass("jp-MetaLakeRef");
    const label = document.createElement("label");
    label.className = "jp-MetaLakeRef-label";
    label.textContent = "Ref";
    this.select = document.createElement("select");
    this.select.className = "jp-MetaLakeRef-select";
    this.select.setAttribute("aria-label", "Ref");
    label.append(this.select);
    this.node.append(label);
    this.show_(this.stored(), false);
    this.select.addEventListener("change", () => void this.choose(this.select.value));
    // The lake's refs, asked again each time the list is opened: branches come and go.
    this.select.addEventListener("focus", () => void this.look(true));
    const session = panel.sessionContext;
    session.kernelChanged.connect(this.restarted, this);
    session.statusChanged.connect(this.status, this);
    NotebookActions.executed.connect(this.executed, this);
    void Promise.all([panel.context.ready, session.ready]).then(() => this.apply(this.stored()));
  }

  dispose(): void {
    if (this.isDisposed) {
      return;
    }
    NotebookActions.executed.disconnect(this.executed, this);
    super.dispose();
  }

  /** The ref the notebook keeps. */
  stored(): string {
    const kept = this.panel.model?.getMetadata(KEY) as { ref?: unknown } | undefined;
    return typeof kept?.ref === "string" && kept.ref !== "" ? kept.ref : MAIN;
  }

  /** Keeps `ref` as the notebook's: none for `main`, so that a notebook never moved off it keeps no metadata. */
  private keep(ref: string): void {
    const model = this.panel.model;
    if (!model || ref === this.stored()) {
      return;
    }
    if (ref === MAIN) {
      model.deleteMetadata(KEY);
    } else {
      model.setMetadata(KEY, { ref });
    }
  }

  private get kernel(): Kernel.IKernelConnection | null {
    return this.panel.sessionContext.session?.kernel ?? null;
  }

  /** The user chose a ref: the notebook keeps it, and `lake` goes on it. */
  private async choose(ref: string): Promise<void> {
    this.keep(ref);
    await this.apply(ref);
  }

  /** `lake` goes on `ref`. A ref that no longer exists is said, and the selector shows the ref `lake` is on. */
  async apply(ref: string): Promise<void> {
    const kernel = this.kernel;
    if (!kernel) {
      return;
    }
    const answer = await ask(kernel, `lake.on(${JSON.stringify(ref)})`, true);
    if ("state" in answer) {
      this.shown(answer.state);
      return;
    }
    if (answer.error.name === "UsageError") {
      // The notebook's ref is gone, or never was: its cells run where `lake` is, which the selector shows, flagged.
      // The notebook keeps the ref it names until the user chooses another, or a cell moves `lake`.
      const now = await ask(kernel, "", true);
      if ("state" in now) {
        this.shown(now.state, false);
      }
      const on = "state" in now ? now.state.ref : MAIN;
      this.select.classList.add("jp-mod-missing");
      this.select.title = `The notebook's ref ${ref} does not exist: its cells run on ${on}`;
      if (this.warned !== ref) {
        this.warned = ref;
        Notification.warning(`This notebook's ref ${ref} does not exist: its cells run on ${on}.`, {
          autoClose: false,
        });
      }
      return;
    }
    this.unreachable(answer.error.value);
  }

  /** Reads what `lake` is on, and with `all`, the lake's refs. */
  private async look(all: boolean): Promise<void> {
    const kernel = this.kernel;
    if (!kernel) {
      return;
    }
    const answer = await ask(kernel, "", all);
    if ("state" in answer) {
      // Asked only for the list, a ref that is missing stays flagged; after a cell, what `lake` is on is the notebook's.
      this.shown(answer.state, !all || !this.select.classList.contains("jp-mod-missing"));
    } else {
      this.unreachable(answer.error.value);
    }
  }

  /** The state `lake` gave: the refs to choose from, and its ref, which, with `kept`, the notebook keeps as its own. */
  private shown(state: State, kept = true): void {
    this.select.disabled = false;
    if (state.refs) {
      this.listed = state.refs;
    }
    if (kept) {
      this.select.classList.remove("jp-mod-missing");
      this.select.title = "";
      this.warned = "";
      this.keep(state.ref);
    }
    this.show_(state.ref, true);
  }

  /** `lake` is not there, or cannot be asked: the selector says why, and offers nothing. */
  private unreachable(why: string): void {
    this.select.disabled = true;
    this.select.title = why;
  }

  /** The options, branches then tags, with `ref` chosen. */
  private show_(ref: string, known: boolean): void {
    const refs = this.listed.some(([name]) => name === ref) ? this.listed : [...this.listed, [ref, "branch"] as const];
    this.select.replaceChildren();
    for (const [kind, title] of [
      ["branch", "Branches"],
      ["tag", "Tags"],
    ] as const) {
      const of = refs.filter(([, k]) => k === kind);
      if (of.length === 0) {
        continue;
      }
      const group = document.createElement("optgroup");
      group.label = title;
      for (const [name] of of) {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = kind === "tag" ? `${name} (tag, read-only)` : name;
        group.append(option);
      }
      this.select.append(group);
    }
    this.select.value = ref;
    this.select.dataset.ref = known ? ref : "";
  }

  /** A new kernel: its `lake` is on `main` until the notebook's ref is applied, once it is idle. */
  private restarted(): void {
    this.fresh = true;
  }

  private status(_: unknown, status: string): void {
    if (status === "starting" || status === "restarting" || status === "autorestarting") {
      this.fresh = true;
    } else if (status === "idle" && this.fresh) {
      this.fresh = false;
      void this.apply(this.stored());
    }
  }

  /** A cell of this notebook ran: what `lake` is on now, which a `lake.on(…)` of the cell may have changed. */
  private executed(_: unknown, ran: { notebook: unknown }): void {
    if (ran.notebook === this.panel.content) {
      void this.look(false);
    }
  }
}

/** Puts a selector in each notebook's toolbar. */
class RefExtension implements DocumentRegistry.IWidgetExtension<NotebookPanel, INotebookModel> {
  createNew(panel: NotebookPanel): IDisposable {
    const selector = new RefSelector(panel);
    panel.toolbar.insertItem(10, "metalakeRef", selector);
    return new DisposableDelegate(() => selector.dispose());
  }
}

const plugin: JupyterFrontEndPlugin<void> = {
  id: "@metalake/jupyterlab-ref:plugin",
  description: "The ref a notebook reads and writes on, kept in it",
  autoStart: true,
  activate: (app: JupyterFrontEnd) => {
    app.docRegistry.addWidgetExtension("Notebook", new RefExtension());
  },
};

export default plugin;
