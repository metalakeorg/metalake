import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The frame on a lake (ADR 0007, milestone 6): `metalake ui` runs on one, as the end-to-end job starts it.

test("the UI opens on Query, the sidebar's first entry", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  const sidebar = page.getByRole("navigation", { name: "Pages" });
  await expect(sidebar.getByRole("link")).toHaveText(["Query"]);
  await expect(page.getByRole("main").getByRole("heading", { name: "Query" })).toBeVisible();
  await page.reload();
  await expect(page.getByRole("main").getByRole("heading", { name: "Query" })).toBeVisible();
});

test("an address that is no page says so", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  await page.goto("/nowhere/at/all");
  await expect(page.getByRole("alert")).toContainText("No such page");
});
