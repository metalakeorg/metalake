import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Said } from "./Notebook";

const INSTALL = '"/envs/data/bin/python" -m pip install "metalake[notebook]"';

describe("the Notebook page", () => {
  it("says a metalake without Python has none", () => {
    render(<Said state={{ python: false, jupyterlab: false, install: INSTALL }} again={() => {}} looking={false} />);
    expect(screen.getByRole("status")).toHaveTextContent("runs without Python");
    expect(screen.queryByText(INSTALL)).toBeNull();
  });

  it("gives the line that installs JupyterLab, and looks again", () => {
    const again = vi.fn();
    render(<Said state={{ python: true, jupyterlab: false, install: INSTALL }} again={again} looking={false} />);
    expect(screen.getByRole("status")).toHaveTextContent("JupyterLab isn't installed");
    expect(screen.getByText(INSTALL)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Check again" }));
    expect(again).toHaveBeenCalledOnce();
  });

  it("says JupyterLab is there", () => {
    render(<Said state={{ python: true, jupyterlab: true, install: INSTALL }} again={() => {}} looking={false} />);
    expect(screen.getByRole("status")).toHaveTextContent("JupyterLab is installed");
  });
});
