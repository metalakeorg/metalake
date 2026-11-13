import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Failure } from "./client";
import { Failed } from "./Failed";

describe("Failed", () => {
  it("says the failure as the command line does", () => {
    const error = new Failure({ kind: "failed", message: "the catalog said no", retryable: false });
    render(<Failed title="The lake could not be read" error={error} />);
    expect(screen.getByRole("alert")).toHaveTextContent("The lake could not be read (failed)");
    expect(screen.getByRole("alert")).toHaveTextContent("metalake: the catalog said no");
  });
});
