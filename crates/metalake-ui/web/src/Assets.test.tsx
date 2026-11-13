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

  it("holds the tables in their schema, and the schemas in the branch's catalog", () => {
    render(<Assets branches={["main", "dev"]} branch="dev" onBranch={() => {}} tables={TABLES} />);
    const catalog = screen.getByText("dev", { selector: "summary span" }).closest("details");
    expect(catalog?.querySelector("summary")).toHaveTextContent("devcatalog");
    const schemas = [...(catalog?.querySelectorAll("details") ?? [])].map(
      (d) => d.querySelector("summary")?.textContent,
    );
    expect(schemas).toEqual(["▸defaultschema", "▸salesschema", "▸sales.euschema"]);
  });

  it("tells the page which branch was picked", () => {
    const onBranch = vi.fn();
    render(<Assets branches={["main", "dev"]} branch="main" onBranch={onBranch} tables={[]} />);
    fireEvent.change(screen.getByRole("combobox", { name: "Branch" }), { target: { value: "dev" } });
    expect(onBranch).toHaveBeenCalledWith("dev");
    expect(screen.getByRole("complementary", { name: "Assets" })).toHaveTextContent("No tables on this branch");
  });

  it("says it reads the catalog while the tables are not known", () => {
    render(<Assets branches={["main"]} branch="main" onBranch={() => {}} tables={undefined} />);
    expect(screen.getByRole("complementary", { name: "Assets" })).toHaveTextContent("Reading the catalog");
  });

  it("says why the tables could not be read", () => {
    const failure = new Error("no ref gone");
    render(<Assets branches={["main"]} branch="gone" onBranch={() => {}} tables={undefined} failure={failure} />);
    expect(screen.getByRole("alert")).toHaveTextContent("metalake: no ref gone");
  });
});
