import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

test("the URL with the run's token reaches the API, and leaves the address bar", async ({ page }) => {
  // The pages are listed once the API answers the token.
  const pages = page.getByRole("navigation", { name: "Pages" });
  await page.goto(`/#token=${TOKEN}`);
  await expect(pages).toBeVisible();
  expect(page.url()).not.toContain(TOKEN);
  await page.reload();
  await expect(pages).toBeVisible();
});

test("a page without the token says to open the printed URL", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("alert")).toContainText("lacks the run's token");
});

test("a token of another run is refused", async ({ page }) => {
  await page.goto("/#token=00000000000000000000000000000000");
  await expect(page.getByRole("alert")).toContainText("lacks the run's token");
});
