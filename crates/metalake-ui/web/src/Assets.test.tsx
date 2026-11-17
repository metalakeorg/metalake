import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Assets, byCatalog, byNamespace, identifier } from "./Assets";

/** A lake's tables (I7): a namespace's first level is its catalog, the levels after it its schema. */
const TABLES = [
  { namespace: ["default", "sales"], name: "orders" },
  { namespace: ["default", "default"], name: "t" },
  { namespace: ["default", "sales"], name: "customers" },
  { namespace: ["default", "sales", "eu"], name: "orders" },
  { namespace: ["ventas", "eu"], name: "pedidos" },
];

describe("identifier", () => {
  it("leaves a plain name bare, and quotes any other", () => {
    expect(identifier("orders")).toBe("orders");
    expect(identifier("sales.eu")).toBe('"sales.eu"');
    expect(identifier("Total")).toBe('"Total"');
    expect(identifier('a"b')).toBe('"a""b"');
  });
});

describe("byNamespace", () => {
  it("holds a schema with no tables, empty, among the others", () => {
    expect(byNamespace([{ namespace: ["sales"], name: "orders" }], [["demo"], ["sales"]])).toEqual([
      ["demo", [], ["demo"]],
      ["sales", ["orders"], ["sales"]],
    ]);
  });

  it("keeps apart schemas whose names, joined by dots, are the same", () => {
    const groups = byNamespace([
      { namespace: ["a.b"], name: "one" },
      { namespace: ["a", "b"], name: "two" },
    ]);
    expect(groups.map(([, names, levels]) => [levels, names])).toEqual([
      [["a.b"], ["one"]],
      [["a", "b"], ["two"]],
    ]);
  });

  it("groups by namespace, both in name order", () => {
    expect(byNamespace(TABLES)).toEqual([
      ["default.default", ["t"], ["default", "default"]],
      ["default.sales", ["customers", "orders"], ["default", "sales"]],
      ["default.sales.eu", ["orders"], ["default", "sales", "eu"]],
      ["ventas.eu", ["pedidos"], ["ventas", "eu"]],
    ]);
  });
});

describe("byCatalog", () => {
  it("groups the schemas by their catalog, the first level, both in name order", () => {
    expect(byCatalog(TABLES)).toEqual([
      [
        "default",
        [
          ["default", ["t"], ["default", "default"]],
          ["sales", ["customers", "orders"], ["default", "sales"]],
          ["sales.eu", ["orders"], ["default", "sales", "eu"]],
        ],
      ],
      ["ventas", [["eu", ["pedidos"], ["ventas", "eu"]]]],
    ]);
  });

  it("holds a catalog with no schema, and a schema with no tables, empty", () => {
    expect(byCatalog([], [["ventas"], ["default", "demo"]])).toEqual([
      ["default", [["demo", [], ["default", "demo"]]]],
      ["ventas", []],
    ]);
  });
});

describe("Assets", () => {
  it("lists the tables of the branch by namespace", () => {
    render(<Assets branches={["main", "dev"]} branch="dev" onBranch={() => {}} tables={TABLES} />);
    expect(screen.getByRole("combobox", { name: "Branch" })).toHaveValue("dev");
    const sales = within(screen.getByRole("list", { name: "default.sales" }));
    expect(sales.getAllByRole("listitem").map((item) => item.textContent)).toEqual(["customers", "orders"]);
  });

  it("holds the tables in their schema, the schemas in their catalog, and the catalogs in the ref", () => {
    render(<Assets branches={["main", "dev"]} branch="dev" onBranch={() => {}} tables={TABLES} />);
    const ref = screen.getByText("dev", { selector: "summary span" }).closest("details");
    expect(ref?.querySelector("summary")).toHaveTextContent("▸dev");
    const below = (node: Element | null | undefined) =>
      [...(node?.querySelectorAll(":scope > div > details") ?? [])].map((d) => d.querySelector("summary"));
    const catalogs = below(ref);
    expect(catalogs.map((s) => s?.textContent)).toEqual(["▸default", "▸ventas"]);
    expect(below(catalogs[0]?.parentElement).map((s) => s?.textContent)).toEqual(["▸default", "▸sales", "▸sales.eu"]);
    expect(below(catalogs[1]?.parentElement).map((s) => s?.textContent)).toEqual(["▸eu"]);
  });

  it("puts a table in the editor by its catalog, its schema and its name: no ref", () => {
    const onInsert = vi.fn();
    render(<Assets branches={["main", "dev"]} branch="dev" onBranch={() => {}} tables={TABLES} onInsert={onInsert} />);
    const eu = within(screen.getByRole("list", { name: "default.sales.eu" }));
    fireEvent.click(eu.getByRole("button", { name: "Put orders in the editor" }));
    expect(onInsert).toHaveBeenCalledWith('default."sales.eu".orders');
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
