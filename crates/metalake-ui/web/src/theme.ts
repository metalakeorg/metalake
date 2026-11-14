// The theme, light or dark: `public/theme.js` sets it before the page draws; the sidebar's toggle changes it, and
// remembers it.

const KEY = "metalake-theme";

export type Theme = "light" | "dark";

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
