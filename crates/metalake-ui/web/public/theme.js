// The theme, light or dark, set before the page draws: the one picked last, else the system's (as the landing does).
(() => {
  let theme = null;
  try {
    theme = localStorage.getItem("metalake-theme");
  } catch {}
  if (theme !== "light" && theme !== "dark") {
    theme = matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  document.documentElement.setAttribute("data-theme", theme);
})();
