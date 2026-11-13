import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, within } from "@testing-library/react";
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
      <QueryPage />
    </QueryClientProvider>,
  );
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
});
