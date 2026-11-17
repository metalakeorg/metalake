import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CatalogPage } from "./Catalog";
import { SAMPLE } from "./views";

/** A lake of a branch and a tag, each with a table of its own, in the schema `shop` of the catalog `default` (I7). */
const LAKE: Record<string, unknown> = {
  "/api/v1/refs": [
    { name: "main", kind: "branch", parent: null, fork_seq: null, created_seq: 1, protected: true },
    { name: "v1", kind: "tag", parent: "main", fork_seq: 3, created_seq: 5, protected: false },
  ],
  "/api/v1/refs/main/tables": [{ namespace: ["default", "shop"], name: "customers" }],
  "/api/v1/refs/v1/tables": [{ namespace: ["default", "shop"], name: "customers_then" }],
  "/api/v1/refs/main": {
    name: "main",
    kind: "branch",
    parent: null,
    fork_seq: null,
    created_seq: 1,
    protected: true,
    warehouse: "file:///lake/data",
    size: { tables: 1, rows: 3, bytes: 2048 },
    schemas: [{ namespace: ["default", "shop"], size: { tables: 1, rows: 3, bytes: 2048 } }],
  },
  // A schema's levels travel joined by U+001F, escaped in the path.
  "/api/v1/refs/main/schemas/default%1Fshop": {
    namespace: ["default", "shop"],
    properties: { owner_team: "sales" },
    size: { tables: 1, rows: 3, bytes: 2048 },
    tables: [{ name: "customers", rows: 3, bytes: 2048, changed_at: "2026-11-14T10:00:00Z" }],
  },
  "/api/v1/refs/main/tables/default%1Fshop/customers": {
    namespace: ["default", "shop"],
    name: "customers",
    id: "0e0c4a",
    location: "file:///lake/data/shop/customers",
    columns: [
      { id: 1, parent: null, name: "id", type: "long", required: true, doc: "the key", default: null },
      { id: 2, parent: null, name: "name", type: "string", required: false, doc: null, default: null },
    ],
    rows: 3,
    bytes: 2048,
    files: 0,
    inline_files: 2,
    deleted_rows: 1,
    partition: null,
    sort_order: null,
    cluster_key: [],
    properties: {},
    iceberg: null,
  },
  "/api/v1/refs/main/tables/default%1Fshop/customers/snapshots": [
    { id: 77, parent: 76, operation: "append", committed_at: "2026-11-14T10:00:00Z" },
  ],
  "/api/v1/refs/main/commits": [
    { seq: 4, committed_at: "2026-11-14T10:00:00Z", author: "ana", operation: "append", message: "first orders" },
  ],
};

/** What each `POST /api/v1/sql` asked: a table's sample. */
let asked: { sql: string; branch: string }[] = [];

beforeEach(() => {
  asked = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    const path = new URL(request.url).pathname;
    if (path === "/api/v1/sql") asked.push(await request.clone().json());
    const body = LAKE[path];
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
      <CatalogPage />
    </QueryClientProvider>,
  );
}

describe("CatalogPage", () => {
  it("shows the tree of the branch picked, and of a tag of it picked", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    expect(await assets.findByRole("list", { name: "default.shop" })).toHaveTextContent("customers");
    await assets.findByRole("option", { name: "v1" });
    fireEvent.change(await assets.findByRole("combobox", { name: "Tag" }), { target: { value: "v1" } });
    expect(await assets.findByText("customers_then")).toBeInTheDocument();
  });

  it("shows the ref's catalog at first: what it holds, its schemas, and its commits", async () => {
    page();
    const view = await screen.findByRole("region", { name: "Branch main" });
    const overview = within(view);
    expect(overview.getByText("file:///lake/data")).toBeInTheDocument();
    expect(overview.getAllByText("2.0 KB").length).toBeGreaterThan(0);
    expect(within(overview.getByRole("table", { name: "Schemas" })).getByText("default.shop")).toBeInTheDocument();

    fireEvent.click(overview.getByRole("tab", { name: "History" }));
    const history = within(await overview.findByRole("table", { name: "Commits" }));
    expect(history.getByText("first orders")).toBeInTheDocument();
    expect(history.getByText("append")).toBeInTheDocument();
  });

  it("shows a schema picked in the tree: its tables, and its properties", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("list", { name: "default.shop" });
    fireEvent.click(assets.getByRole("button", { name: "shop" }));
    const view = within(await screen.findByRole("region", { name: "Schema default.shop" }));
    expect(within(await view.findByRole("table", { name: "Tables" })).getByText("customers")).toBeInTheDocument();
    fireEvent.click(view.getByRole("tab", { name: "Details" }));
    expect(within(view.getByRole("table", { name: "Properties" })).getByText("sales")).toBeInTheDocument();
  });

  it("shows a table picked in the tree: its columns, what it holds, and its snapshots", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("list", { name: "default.shop" });
    fireEvent.click(assets.getByRole("button", { name: "customers" }));
    const view = within(await screen.findByRole("region", { name: "Table default.shop.customers" }));
    expect(view.getByText("main.default.shop")).toBeInTheDocument();
    // Its sample names its catalog, its schema and itself, and runs on the ref: no name says a ref (I7).
    const sample = { sql: `select * from "default"."shop"."customers" limit ${SAMPLE}`, branch: "main" };
    await vi.waitFor(() => expect(asked).toContainEqual(expect.objectContaining(sample)));
    expect(view.getByRole("heading", { level: 1 })).toHaveTextContent("customerstable");
    const columns = within(await view.findByRole("table", { name: "Columns" }));
    expect(columns.getByText("the key")).toBeInTheDocument();
    expect(columns.getByText("long")).toBeInTheDocument();
    fireEvent.click(view.getByRole("tab", { name: "Details" }));
    expect(view.getByText("file:///lake/data/shop/customers")).toBeInTheDocument();
    expect(view.getByText("not written on commit")).toBeInTheDocument();
    fireEvent.click(view.getByRole("tab", { name: "History" }));
    expect(within(await view.findByRole("table", { name: "Snapshots" })).getByText("77")).toBeInTheDocument();
  });

  it("shows a ref's catalog again when the ref is picked again", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("list", { name: "default.shop" });
    fireEvent.click(assets.getByRole("button", { name: "shop" }));
    await screen.findByRole("region", { name: "Schema default.shop" });
    fireEvent.change(await assets.findByRole("combobox", { name: "Tag" }), { target: { value: "v1" } });
    await assets.findByText("customers_then");
    fireEvent.change(assets.getByRole("combobox", { name: "Tag" }), { target: { value: "" } });
    expect(await screen.findByRole("region", { name: "Branch main" })).toBeInTheDocument();
  });

  it("holds the ways to the lists of branches and tags and of pull requests, not built yet", () => {
    page();
    const views = within(screen.getByRole("navigation", { name: "Catalog views" }));
    expect(views.getByRole("button", { name: "Branches & tags" })).toBeDisabled();
    expect(views.getByRole("button", { name: "Pull requests" })).toBeDisabled();
  });
});
