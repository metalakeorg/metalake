import { afterEach, describe, expect, it, vi } from "vitest";
import { applyTheme, toggleTheme } from "./theme";

describe("theme", () => {
  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute("data-theme");
    vi.unstubAllGlobals();
  });

  it("is the one picked last", () => {
    localStorage.setItem("metalake-theme", "dark");
    expect(applyTheme()).toBe("dark");
    expect(document.documentElement.getAttribute("data-theme")).toBe("dark");
  });

  it("is the system's when none was picked", () => {
    vi.stubGlobal("matchMedia", (query: string) => ({ matches: query === "(prefers-color-scheme: dark)" }));
    expect(applyTheme()).toBe("dark");
    vi.stubGlobal("matchMedia", () => ({ matches: false }));
    expect(applyTheme()).toBe("light");
  });

  it("toggles, and remembers the toggle", () => {
    applyTheme();
    const other = toggleTheme();
    expect(document.documentElement.getAttribute("data-theme")).toBe(other);
    expect(localStorage.getItem("metalake-theme")).toBe(other);
  });
});
