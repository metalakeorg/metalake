import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The frame on a lake (ADR 0007, milestone 6): `metalake ui` runs on one, as the end-to-end job starts it.

test("the Lake page says what the lake is, and the header names it", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  const refs = page.getByRole("list", { name: "Refs" });
  await expect(refs).toContainText("main");
  await expect(refs).toContainText("branch");
  await expect(page.getByRole("main")).toContainText("Warehouse");
  await expect(page.getByRole("banner")).toContainText("·");
});

test("every page is in the navigation, and one not built says so", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  const nav = page.getByRole("navigation", { name: "Pages" });
  for (const title of ["Lake", "Refs", "Catalog", "Log", "SQL", "History", "Execution"]) {
    await expect(nav.getByRole("link", { name: title, exact: true })).toBeVisible();
  }
  await nav.getByRole("link", { name: "SQL", exact: true }).click();
  await expect(page).toHaveURL(/\/sql$/);
  await expect(page.getByRole("main")).toContainText("Not built yet");
  // An address of a page is the server's too: reloaded, it is the same page, with the token kept.
  await page.reload();
  await expect(page.getByRole("main")).toContainText("Not built yet");
  await nav.getByRole("link", { name: "Lake", exact: true }).click();
  await expect(page.getByRole("list", { name: "Refs" })).toContainText("main");
});

test("an address that is no page says so", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  await page.goto("/nowhere/at/all");
  await expect(page.getByRole("alert")).toContainText("No such page");
});
