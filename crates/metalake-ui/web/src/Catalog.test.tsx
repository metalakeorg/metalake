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

  it("holds the ways to the lists of branches and tags and of pull requests, not built yet", () => {
    page();
    const views = within(screen.getByRole("navigation", { name: "Catalog views" }));
    expect(views.getByRole("button", { name: "Branches & tags" })).toBeDisabled();
    expect(views.getByRole("button", { name: "Pull requests" })).toBeDisabled();
  });
});
