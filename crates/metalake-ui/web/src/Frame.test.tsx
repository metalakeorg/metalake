import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Frame } from "./Frame";

describe("Frame", () => {
  it("names the product", () => {
    render(<Frame />);
    expect(screen.getByRole("banner")).toHaveTextContent("MetaLake");
  });

  it("renders the page in main", () => {
    render(<Frame>a page</Frame>);
    expect(screen.getByRole("main")).toHaveTextContent("a page");
  });
});
