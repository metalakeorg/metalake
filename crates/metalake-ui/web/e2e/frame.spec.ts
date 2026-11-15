import { expect, test } from "@playwright/test";

test("the frame names the product", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveTitle("MetaLake");
  await expect(page.getByRole("banner")).toContainText("Metalake UI");
  await expect(page.getByRole("main")).toBeVisible();
});

test("a route of the frontend's router loads the application", async ({ page }) => {
  await page.goto("/refs/main");
  await expect(page.getByRole("banner")).toContainText("Metalake UI");
});

test("the script arrives compressed and cached forever", async ({ page }) => {
  const scripts: { encoding: string | undefined; cache: string | undefined }[] = [];
  page.on("response", (response) => {
    if (response.url().endsWith(".js")) {
      const headers = response.headers();
      scripts.push({ encoding: headers["content-encoding"], cache: headers["cache-control"] });
    }
  });
  await page.goto("/");
  await expect(page.getByRole("banner")).toBeVisible();
  expect(scripts).toEqual([{ encoding: "br", cache: "public, max-age=31536000, immutable" }]);
});
