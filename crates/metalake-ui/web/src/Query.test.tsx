import { EditorView } from "@codemirror/view";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { columnFromArray, int64, tableFromColumns, tableToIPC } from "@uwdata/flechette";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryPage } from "./Query";

/** A lake of two branches: `main` with `default.sales.orders`, `dev` with `default.default.t` too (I7). */
const LAKE: Record<string, unknown> = {
  "/api/v1/refs": [
    { name: "main", kind: "branch", parent: null, fork_seq: null, created_seq: 1, protected: true },
    { name: "dev", kind: "branch", parent: "main", fork_seq: 3, created_seq: 4, protected: false },
    { name: "v1", kind: "tag", parent: "main", fork_seq: 3, created_seq: 5, protected: false },
  ],
  "/api/v1/refs/main/tables": [{ namespace: ["default", "sales"], name: "orders" }],
  "/api/v1/refs/v1/tables": [{ namespace: ["default", "sales"], name: "orders_then" }],
  "/api/v1/refs/main/tables/default%1Fsales/orders": {
    namespace: ["default", "sales"],
    name: "orders",
    id: "0e0c4a",
    location: "file:///lake/data/sales/orders",
    columns: [
      { id: 1, parent: null, name: "id", type: "long", required: true, doc: null, default: null },
      { id: 2, parent: null, name: "at", type: "timestamptz", required: false, doc: null, default: null },
      { id: 3, parent: null, name: "ship", type: "struct", required: false, doc: null, default: null },
      { id: 4, parent: 3, name: "city", type: "string", required: false, doc: null, default: null },
    ],
    rows: 0,
    bytes: 0,
    files: 0,
    inline_files: 0,
    deleted_rows: 0,
    partition: null,
    sort_order: null,
    cluster_key: [],
    properties: {},
    iceberg: null,
  },
  "/api/v1/refs/dev/tables": [
    { namespace: ["default", "sales"], name: "orders" },
    { namespace: ["default", "default"], name: "t" },
  ],
};

/** What each `POST /api/v1/sql/run` asked. */
let asked: { sql: string; branch: string; limit: number }[] = [];

/** The branches a run made, with `create branch x`, which `/api/v1/refs` then lists. */
let made: string[] = [];

/**
 * A run's lines, a statement each, split at `;`: a read of `k` 1 and 2, and more it left. A statement that says `fail`
 * fails, as a write that conflicts; those after it are not run. `refuse` is refused as a whole. Each that ran says the
 * ref the run is on: `use branch x` and `use tag x` move it, and `create branch x` makes `x`, as the lake does.
 */
async function runs(request: Request): Promise<Response> {
  const statement = (await request.json()) as { sql: string; branch: string; limit: number };
  asked.push(statement);
  if (statement.sql === "refuse") {
    const fault = { kind: "usage", message: "the text cannot be read", retryable: false };
    return new Response(JSON.stringify(fault), { status: 400, headers: { "content-type": "application/json" } });
  }
  const table = tableFromColumns({ k: columnFromArray([1n, 2n], int64()) });
  const bytes = tableToIPC(table, { format: "stream" }) as Uint8Array;
  const arrow = btoa(String.fromCharCode(...bytes));
  let failed = false;
  let on = statement.branch;
  const lines = statement.sql.split(";").map((sql, index) => {
    sql = sql.trim();
    if (failed) return { index, sql, state: "skipped" };
    if (sql === "fail") {
      failed = true;
      const fault = { kind: "conflict", message: "a write conflicts with another", retryable: true };
      return { index, sql, state: "error", ms: 1, fault, ref: on };
    }
    const used = /^use (?:branch|tag) (\S+)$/i.exec(sql);
    if (used?.[1]) on = used[1];
    const created = /^create branch (\S+)$/i.exec(sql);
    if (created?.[1]) made.push(created[1]);
    return { index, sql, state: "ok", ms: 2, rows: 2, truncated: true, arrow, ref: on };
  });
  const body = lines
    .map(
      (line) => `${JSON.stringify(line)}
`,
    )
    .join("");
  return new Response(body, { headers: { "content-type": "application/x-ndjson" } });
}

beforeEach(() => {
  asked = [];
  made = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    if (request.method === "POST") return runs(request);
    const path = new URL(request.url).pathname;
    const branches = made.map((name) => ({
      name,
      kind: "branch",
      parent: "main",
      fork_seq: 9,
      created_seq: 10,
      protected: false,
    }));
    const body = path === "/api/v1/refs" ? [...(LAKE[path] as unknown[]), ...branches] : LAKE[path];
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
  act(() =>
    view?.dispatch({
      changes: { from: 0, to: view.state.doc.length, insert: text },
      selection: { anchor: text.length },
    }),
  );
}

/** What the editor holds. */
function text(): string {
  return EditorView.findFromDOM(screen.getByRole("textbox", { name: "SQL" }))?.state.doc.toString() ?? "";
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
    const branch = within(assets.getByRole("combobox", { name: "Branch" }));
    expect(branch.queryByRole("option", { name: "v1" })).toBeNull();
    expect(await assets.findByRole("list", { name: "default.sales" })).toHaveTextContent("orders");
    expect(assets.queryByRole("list", { name: "default.default" })).toBeNull();

    fireEvent.change(assets.getByRole("combobox", { name: "Branch" }), { target: { value: "dev" } });
    expect(await assets.findByRole("list", { name: "default.default" })).toHaveTextContent("t");
    // v1 was made from main: dev has no tags.
    const tag = assets.getByRole("combobox", { name: "Tag" });
    expect(within(tag).queryByRole("option", { name: "v1" })).toBeNull();
    expect(tag).toBeDisabled();
  });

  it("unfolds a table on its columns, each with its type, a nested one below its column", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    const sales = within(await assets.findByRole("list", { name: "default.sales" }));
    expect(assets.queryByRole("list", { name: "Columns of orders" })).toBeNull();
    fireEvent.click(sales.getByText("orders"));
    const columns = within(await assets.findByRole("list", { name: "Columns of orders" }));
    expect(columns.getAllByRole("listitem").map((c) => c.textContent)).toEqual(["id", "at", "ship", "city"]);
  });

  it("puts a table's path and a column's name in the editor, from the arrow of its row", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("list", { name: "default.sales" });
    write("select ");
    fireEvent.click(assets.getByRole("button", { name: "Put orders in the editor" }));
    expect(text()).toBe("select default.sales.orders");
    fireEvent.click(within(assets.getByRole("list", { name: "default.sales" })).getByText("orders"));
    await assets.findByRole("list", { name: "Columns of orders" });
    fireEvent.click(assets.getByRole("button", { name: "Put id in the editor" }));
    fireEvent.click(assets.getByRole("button", { name: "Put city in the editor" }));
    expect(text()).toBe("select default.sales.orders id ship['city']");
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

  it("lists the branch's tags apart, none picked at first, and a tag picked is its catalog, read only, where statements run", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("option", { name: "v1" });
    const tag = assets.getByRole("combobox", { name: "Tag" });
    expect(tag).toHaveValue("");
    fireEvent.change(tag, { target: { value: "v1" } });
    expect(tag).toHaveValue("v1");
    expect(await assets.findByRole("list", { name: "default.sales" })).toHaveTextContent("orders_then");
    expect(screen.getByRole("region", { name: "Query" })).toHaveTextContent("tag · read only");

    write("select 1");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByRole("table", { name: "Rows" });
    expect(asked.at(-1)?.branch).toBe("v1");
  });

  it("follows the ref a run ends on, by USE BRANCH, and runs the next there", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("option", { name: "dev" });
    expect(assets.getByRole("combobox", { name: "Branch" })).toHaveValue("main");
    write("use branch dev; select 1");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByRole("table", { name: "Rows" });
    await vi.waitFor(() => expect(assets.getByRole("combobox", { name: "Branch" })).toHaveValue("dev"));

    write("select 2");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await vi.waitFor(() => expect(asked).toHaveLength(2));
    expect(asked[1]?.branch).toBe("dev");
  });

  it("follows a branch the same run made, once the refs are read again", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("option", { name: "dev" });
    write("create branch fresh; use branch fresh");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await vi.waitFor(() => expect(assets.getByRole("combobox", { name: "Branch" })).toHaveValue("fresh"));
    expect(assets.getByRole("option", { name: "fresh" })).toBeInTheDocument();
  });

  it("follows a tag a run ends on, with its branch", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("option", { name: "dev" });
    fireEvent.change(assets.getByRole("combobox", { name: "Branch" }), { target: { value: "dev" } });
    write("use tag v1");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await vi.waitFor(() => expect(assets.getByRole("combobox", { name: "Tag" })).toHaveValue("v1"));
    expect(assets.getByRole("combobox", { name: "Branch" })).toHaveValue("main");
    expect(screen.getByRole("region", { name: "Query" })).toHaveTextContent("tag · read only");
  });

  it("runs several statements in one request, a result each, and opens on the one that failed", async () => {
    page();
    write("select 1; fail; select 2");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    const turns = within(await screen.findByRole("navigation", { name: "Results of the statements" }));
    expect(turns.getByText(/Result 2 of 3/)).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("metalake (conflict): a write conflicts with another");
    fireEvent.click(turns.getByRole("button", { name: "Previous result" }));
    expect(screen.getByRole("table", { name: "Rows" })).toBeInTheDocument();
    expect(asked).toHaveLength(1);
  });

  it("says a text refused as a whole as the command line does", async () => {
    page();
    write("refuse");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("metalake (usage): the text cannot be read");
  });

  it("says a failure as the command line does", async () => {
    page();
    write("fail");
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("metalake (conflict): a write conflicts with another");
  });
});
