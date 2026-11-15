import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Step } from "./client";
import { opened, PAGE, Results, type Run } from "./Results";

/** A read of `n` rows of one column. */
function read(n: number, index = 0): Step {
  const result = { columns: ["k"], rows: Array.from({ length: n }, (_, i) => [i + 1]), truncated: false };
  return { index, sql: `select k from t${index}`, state: "ok", ms: 5, result };
}

function done(...steps: Step[]): Run {
  return { kind: "done", id: 1, steps };
}

const write: Step = {
  index: 1,
  sql: "insert into t values (1)",
  state: "ok",
  ms: 40,
  written: { commit: 57, rows: 1 },
};
const created: Step = { index: 0, sql: "create table t (k int)", state: "ok", ms: 410 };
const failed: Step = {
  index: 2,
  sql: "select nope",
  state: "error",
  ms: 1,
  fault: { kind: "usage", message: "no column nope", retryable: false },
};
const skipped: Step = { index: 3, sql: "drop table t", state: "skipped" };

describe("Results' pages", () => {
  it("shows the rows a page at a time, and turns them", () => {
    render(<Results state={done(read(PAGE * 2 + 50))} />);
    const pages = within(screen.getByRole("navigation", { name: "Pages of rows" }));
    expect(pages.getByText(/page 1 of 3/)).toHaveTextContent("Rows 1–100");
    expect(pages.getByRole("button", { name: "Previous" })).toBeDisabled();

    fireEvent.click(pages.getByRole("button", { name: "Next" }));
    fireEvent.click(pages.getByRole("button", { name: "Next" }));
    expect(pages.getByText(/page 3 of 3/)).toHaveTextContent("Rows 201–250");
    expect(pages.getByRole("button", { name: "Next" })).toBeDisabled();

    fireEvent.click(pages.getByRole("button", { name: "Previous" }));
    expect(pages.getByText(/page 2 of 3/)).toHaveTextContent("Rows 101–200");
  });

  it("says nothing of pages for rows that fit in one", () => {
    render(<Results state={done(read(PAGE))} />);
    expect(screen.queryByRole("navigation", { name: "Pages of rows" })).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("100 rows");
  });
});

describe("Results' statements", () => {
  const turns = () => within(screen.getByRole("navigation", { name: "Results of the statements" }));

  it("has no arrows for one statement alone", () => {
    render(<Results state={done(read(2))} />);
    expect(screen.queryByRole("navigation", { name: "Results of the statements" })).toBeNull();
    expect(screen.getByRole("table", { name: "Rows" })).toBeInTheDocument();
  });

  it("shows one result at a time, opening on the one that failed, and turns them with the arrows", () => {
    render(<Results state={done(created, write, failed, skipped)} />);
    expect(turns().getByText(/Result 3 of 4/)).toHaveAttribute("title", "select nope");
    expect(screen.getByRole("alert")).toHaveTextContent("metalake (usage): no column nope");

    fireEvent.click(turns().getByRole("button", { name: "Previous result" }));
    expect(turns().getByText(/Result 2 of 4/)).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("OK1 row writtencommit 570.04 s");
    fireEvent.click(turns().getByRole("button", { name: "Previous result" }));
    expect(screen.getByRole("status")).toHaveTextContent("OK0.41 s");
    expect(turns().getByRole("button", { name: "Previous result" })).toBeDisabled();

    fireEvent.click(turns().getByRole("button", { name: "Next result" }));
    fireEvent.click(turns().getByRole("button", { name: "Next result" }));
    fireEvent.click(turns().getByRole("button", { name: "Next result" }));
    expect(turns().getByText(/Result 4 of 4/)).toBeInTheDocument();
    expect(screen.getByText("Not run: a statement before it failed.")).toBeInTheDocument();
    expect(turns().getByRole("button", { name: "Next result" })).toBeDisabled();
  });

  it("opens on the last that ran, a read's rows, when none failed", () => {
    render(<Results state={done(created, write, read(3, 2))} />);
    expect(turns().getByText(/Result 3 of 3/)).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "Rows" })).toBeInTheDocument();
  });

  it("follows the run while it runs, and says it runs", () => {
    render(<Results state={{ kind: "running", id: 1, steps: [created, write] }} />);
    expect(turns().getByText(/Result 2 of 2/)).toBeInTheDocument();
    expect(screen.getByText("Running…")).toBeInTheDocument();
  });
});

describe("opened", () => {
  it("is the failure, or else the last that ran", () => {
    expect(opened([created, write, failed, skipped])).toBe(2);
    expect(opened([created, write])).toBe(1);
    expect(opened([])).toBe(0);
  });
});
