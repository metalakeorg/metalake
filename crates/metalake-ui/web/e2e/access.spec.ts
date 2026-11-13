import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

test("the URL with the run's token reaches the API, and leaves the address bar", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  await expect(page.getByRole("banner")).toContainText("API 1");
  expect(page.url()).not.toContain(TOKEN);
  await page.reload();
  await expect(page.getByRole("banner")).toContainText("API 1");
});

test("a page without the token says to open the printed URL", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("alert")).toContainText("lacks the run's token");
});

test("a token of another run is refused", async ({ page }) => {
  await page.goto("/#token=00000000000000000000000000000000");
  await expect(page.getByRole("alert")).toContainText("lacks the run's token");
});
