import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CatalogPage } from "./Catalog";

/** A lake of a branch and a tag, each with a table of its own. */
const LAKE: Record<string, unknown> = {
  "/api/v1/refs": [
    { name: "main", kind: "branch", parent: null, fork_seq: null, created_seq: 1, protected: true },
    { name: "v1", kind: "tag", parent: "main", fork_seq: 3, created_seq: 5, protected: false },
  ],
  "/api/v1/refs/main/tables": [{ namespace: ["shop"], name: "customers" }],
  "/api/v1/refs/v1/tables": [{ namespace: ["shop"], name: "customers_then" }],
  "/api/v1/refs/main": {
    name: "main",
    kind: "branch",
    parent: null,
    fork_seq: null,
    created_seq: 1,
    protected: true,
    warehouse: "file:///lake/data",
    size: { tables: 1, rows: 3, bytes: 2048 },
    schemas: [{ namespace: ["shop"], size: { tables: 1, rows: 3, bytes: 2048 } }],
  },
  "/api/v1/refs/main/schemas/shop": {
    namespace: ["shop"],
    properties: { owner_team: "sales" },
    size: { tables: 1, rows: 3, bytes: 2048 },
    tables: [{ name: "customers", rows: 3, bytes: 2048, changed_at: "2026-11-14T10:00:00Z" }],
  },
  "/api/v1/refs/main/commits": [
    { seq: 4, committed_at: "2026-11-14T10:00:00Z", author: "ana", operation: "append", message: "first orders" },
  ],
};

beforeEach(() => {
  vi.stubGlobal("fetch", async (request: Request) => {
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
      <CatalogPage />
    </QueryClientProvider>,
  );
}

describe("CatalogPage", () => {
  it("shows the tree of the branch picked, and of a tag pressed", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    expect(await assets.findByRole("list", { name: "shop" })).toHaveTextContent("customers");
    fireEvent.click(within(await assets.findByRole("list", { name: "Tags" })).getByRole("button", { name: "v1" }));
    expect(await assets.findByText("customers_then")).toBeInTheDocument();
  });

  it("shows the ref's catalog at first: what it holds, its schemas, and its commits", async () => {
    page();
    const view = await screen.findByRole("region", { name: "Branch main" });
    const overview = within(view);
    expect(overview.getByText("file:///lake/data")).toBeInTheDocument();
    expect(overview.getByText("2.0 KB")).toBeInTheDocument();
    expect(within(overview.getByRole("table", { name: "Schemas" })).getByText("shop")).toBeInTheDocument();

    fireEvent.click(overview.getByRole("tab", { name: "History" }));
    const history = within(await overview.findByRole("table", { name: "Commits" }));
    expect(history.getByText("first orders")).toBeInTheDocument();
    expect(history.getByText("append")).toBeInTheDocument();
  });

  it("shows a schema picked in the tree: its tables, and its properties", async () => {
    page();
    const assets = within(screen.getByRole("complementary", { name: "Assets" }));
    await assets.findByRole("list", { name: "shop" });
    fireEvent.click(assets.getByRole("button", { name: "shop" }));
    const view = within(await screen.findByRole("region", { name: "Schema shop" }));
    expect(within(await view.findByRole("table", { name: "Tables" })).getByText("customers")).toBeInTheDocument();
    fireEvent.click(view.getByRole("tab", { name: "Details" }));
    expect(within(view.getByRole("table", { name: "Properties" })).getByText("sales")).toBeInTheDocument();
  });

  it("holds the ways to the lists of branches and tags and of pull requests, not built yet", () => {
    page();
    const views = within(screen.getByRole("navigation", { name: "Catalog views" }));
    expect(views.getByRole("button", { name: "Branches & tags" })).toBeDisabled();
    expect(views.getByRole("button", { name: "Pull requests" })).toBeDisabled();
  });
});
