import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Assets, byNamespace } from "./Assets";

const TABLES = [
  { namespace: ["sales"], name: "orders" },
  { namespace: ["default"], name: "t" },
  { namespace: ["sales"], name: "customers" },
  { namespace: ["sales", "eu"], name: "orders" },
];

describe("byNamespace", () => {
  it("groups by namespace, both in name order", () => {
    expect(byNamespace(TABLES)).toEqual([
      ["default", ["t"]],
      ["sales", ["customers", "orders"]],
      ["sales.eu", ["orders"]],
    ]);
  });
});

describe("Assets", () => {
  it("lists the tables of the branch by namespace", () => {
    render(<Assets branches={["main", "dev"]} branch="dev" onBranch={() => {}} tables={TABLES} />);
    expect(screen.getByRole("combobox", { name: "Branch" })).toHaveValue("dev");
    const sales = within(screen.getByRole("list", { name: "sales" }));
    expect(sales.getAllByRole("listitem").map((item) => item.textContent)).toEqual(["customers", "orders"]);
  });

  it("tells the page which branch was picked", () => {
    const onBranch = vi.fn();
    render(<Assets branches={["main", "dev"]} branch="main" onBranch={onBranch} tables={[]} />);
    fireEvent.change(screen.getByRole("combobox", { name: "Branch" }), { target: { value: "dev" } });
    expect(onBranch).toHaveBeenCalledWith("dev");
    expect(screen.getByRole("complementary", { name: "Assets" })).toHaveTextContent("No tables on this branch");
  });

  it("says the catalog is not wired while its tables are not known", () => {
    render(<Assets branches={["main"]} branch="main" onBranch={() => {}} tables={null} />);
    expect(screen.getByRole("complementary", { name: "Assets" })).toHaveTextContent("not wired yet");
  });
});
