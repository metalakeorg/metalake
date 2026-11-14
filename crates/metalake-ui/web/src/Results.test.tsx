import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PAGE, Results } from "./Results";

/** A result of `n` rows of one column. */
function done(n: number) {
  const result = { columns: ["k"], rows: Array.from({ length: n }, (_, i) => [i + 1]), truncated: false };
  return { kind: "done" as const, result, ms: 5 };
}

describe("Results' pages", () => {
  it("shows the rows a page at a time, and turns them", () => {
    render(<Results state={done(PAGE * 2 + 50)} />);
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
    render(<Results state={done(PAGE)} />);
    expect(screen.queryByRole("navigation", { name: "Pages of rows" })).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("100 rows");
  });
});
