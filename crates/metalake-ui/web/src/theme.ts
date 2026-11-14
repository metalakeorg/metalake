// The theme, light or dark: set by the bundle before anything renders, the one picked last or else the system's, as the
// landing does; the sidebar's toggle changes it, and remembers it. Until the bundle runs, the page's CSS follows the
// system.

const KEY = "metalake-theme";

export type Theme = "light" | "dark";

/** Sets the theme picked last, or the system's when none was: on the page's root, which the CSS's `dark` reads. */
export function applyTheme(): Theme {
  let picked: string | null = null;
  try {
    picked = localStorage.getItem(KEY);
  } catch {
    // A page that may not read storage follows the system.
  }
  const chosen: Theme =
    picked === "light" || picked === "dark"
      ? picked
      : globalThis.matchMedia?.("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light";
  document.documentElement.setAttribute("data-theme", chosen);
  return chosen;
}

export function theme(): Theme {
  return document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
}

/** Sets the other theme, and remembers it. */
export function toggleTheme(): Theme {
  const next: Theme = theme() === "dark" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", next);
  try {
    localStorage.setItem(KEY, next);
  } catch {
    // A page that may not store keeps the theme until it is closed.
  }
  return next;
}
