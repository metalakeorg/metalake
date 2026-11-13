import { EditorView } from "@codemirror/view";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { columnFromArray, int64, tableFromColumns, tableToIPC } from "@uwdata/flechette";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryPage } from "./Query";

/** A lake of two branches: `main` with `sales.orders`, `dev` with `default.t` too. */
const LAKE: Record<string, unknown> = {
  "/api/v1/refs": [
    { name: "main", kind: "branch", parent: null, fork_seq: null, created_seq: 1, protected: true },
    { name: "dev", kind: "branch", parent: "main", fork_seq: 3, created_seq: 4, protected: false },
    { name: "v1", kind: "tag", parent: "main", fork_seq: 3, created_seq: 5, protected: false },
  ],
  "/api/v1/refs/main/tables": [{ namespace: ["sales"], name: "orders" }],
  "/api/v1/refs/dev/tables": [
    { namespace: ["sales"], name: "orders" },
    { namespace: ["default"], name: "t" },
  ],
};

/** What each `POST /api/v1/sql` asked. */
let asked: { sql: string; branch: string; limit: number }[] = [];

/** The rows a statement answers: `k` of 1 and 2, and more it left. One that says `fail` conflicts. */
async function runs(request: Request): Promise<Response> {
  const statement = (await request.json()) as { sql: string; branch: string; limit: number };
  asked.push(statement);
  if (statement.sql === "fail") {
    const fault = { kind: "conflict", message: "a write conflicts with another", retryable: true };
    return new Response(JSON.stringify(fault), { status: 409, headers: { "content-type": "application/json" } });
  }
  const table = tableFromColumns({ k: columnFromArray([1n, 2n], int64()) });
  const bytes = tableToIPC(table, { format: "stream" }) as Uint8Array;
  return new Response(bytes.slice(), {
    headers: { "content-type": "application/vnd.apache.arrow.stream", "metalake-truncated": "true" },
  });
}

beforeEach(() => {
  asked = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    if (request.method === "POST") return runs(request);
    const body = LAKE[new URL(request.url).pathname];
    return new Response(JSON.stringify(body ?? { kind: "usage", message: "no such path", retryable: false }), {
      status: body ? 200 : 404,
      headers: { "content-type": "application/json" },
    });
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function page() {
  const queries = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queries}>
      <QueryPage />
    </QueryClientProvider>,
  );
}

/** Writes `text` in the editor, as typing would. */
function write(text: string) {
  const content = screen.getByRole("textbox", { name: "SQL" });
  const view = EditorView.findFromDOM(content);
  act(() => view?.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: text } }));
}

describe("QueryPage", () => {
  it("holds an SQL editor, and an empty place for results", () => {
    page();
    expect(screen.getByRole("textbox", { name: "SQL" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Results" })).toHaveTextContent("Run a statement");
  });

  it("says there is nothing to run when the editor is empty", () => {
    page();
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(screen.getByRole("region", { name: "Results" })).toHaveTextContent("nothing to run");
  });

  it("lists the branches, not the tags, and the tables of the one picked", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    expect(await assets.findByRole("option", { name: "dev" })).toBeInTheDocument();
    expect(assets.queryByRole("option", { name: "v1" })).toBeNull();
    expect(await assets.findByRole("list", { name: "sales" })).toHaveTextContent("orders");
    expect(assets.queryByRole("list", { name: "default" })).toBeNull();

    fireEvent.change(assets.getByRole("combobox", { name: "Branch" }), { target: { value: "dev" } });
    expect(await assets.findByRole("list", { name: "default" })).toHaveTextContent("t");
  });

  it("runs the statement on the branch picked, and shows its rows", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("option", { name: "dev" });
    fireEvent.change(assets.getByRole("combobox", { name: "Branch" }), { target: { value: "dev" } });
    write("select k from default.t");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));

    const rows = await screen.findByRole("table", { name: "Rows" });
    expect(asked).toEqual([{ sql: "select k from default.t", branch: "dev", limit: 10_000 }]);
    expect(
      within(rows)
        .getAllByRole("columnheader")
        .map((h) => h.textContent),
    ).toEqual(["#", "k"]);
    const results = screen.getByRole("region", { name: "Results" });
    expect(within(results).getByRole("status")).toHaveTextContent("2 rows");
    expect(within(results).getByRole("status")).toHaveTextContent("the statement answered more");
  });

  it("says a failure as the command line does", async () => {
    page();
    write("fail");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("metalake (conflict): a write conflicts with another");
  });
});
