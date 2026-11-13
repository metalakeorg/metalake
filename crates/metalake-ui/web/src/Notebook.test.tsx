import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Said, Starting } from "./Notebook";

const INSTALL = '"/envs/data/bin/python" -m pip install "metalake[notebook]"';

describe("the Notebook page", () => {
  it("says a metalake without Python has none", () => {
    render(
      <Said
        state={{ python: false, jupyterlab: false, running: false, open: null, install: INSTALL }}
        again={() => {}}
        looking={false}
      />,
    );
    expect(screen.getByRole("status")).toHaveTextContent("runs without Python");
    expect(screen.queryByText(INSTALL)).toBeNull();
  });

  it("gives the line that installs JupyterLab, and looks again", () => {
    const again = vi.fn();
    render(
      <Said
        state={{ python: true, jupyterlab: false, running: false, open: null, install: INSTALL }}
        again={again}
        looking={false}
      />,
    );
    expect(screen.getByRole("status")).toHaveTextContent("JupyterLab isn't installed");
    expect(screen.getByText(INSTALL)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Check again" }));
    expect(again).toHaveBeenCalledOnce();
  });

  it("says JupyterLab is there", () => {
    render(
      <Said
        state={{ python: true, jupyterlab: true, running: false, open: null, install: INSTALL }}
        again={() => {}}
        looking={false}
      />,
    );
    expect(screen.getByRole("status")).toHaveTextContent("JupyterLab is installed");
  });

  it("says JupyterLab starting, then running", () => {
    const { rerender } = render(<Starting running={false} failure={null} again={() => {}} />);
    expect(screen.getByRole("status")).toHaveTextContent("Starting JupyterLab");
    rerender(<Starting running={true} failure={null} again={() => {}} />);
    expect(screen.getByRole("status")).toHaveTextContent("JupyterLab is running");
  });

  it("says why JupyterLab did not start, and tries again", () => {
    const again = vi.fn();
    render(<Starting running={false} failure={new Error("it ended (exit code: 1)")} again={again} />);
    expect(screen.getByRole("alert")).toHaveTextContent("it ended (exit code: 1)");
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(again).toHaveBeenCalledOnce();
  });
});
