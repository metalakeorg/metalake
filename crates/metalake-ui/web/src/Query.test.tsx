import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { QueryPage } from "./Query";

describe("QueryPage", () => {
  it("holds an SQL editor, and an empty place for results", () => {
    render(<QueryPage />);
    expect(screen.getByRole("textbox", { name: "SQL" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Results" })).toHaveTextContent("Run a statement");
  });

  it("says there is nothing to run when the editor is empty", () => {
    render(<QueryPage />);
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(screen.getByRole("region", { name: "Results" })).toHaveTextContent("nothing to run");
  });
});
