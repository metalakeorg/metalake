import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The Notebook (ADR 0008). Where `METALAKE_E2E_JUPYTER` is set, `METALAKE_PYTHON` has JupyterLab, as CI's end-to-end
// job gives it: the page starts it and shows it, and a cell runs. Else, what this `metalake` can give: with a Python
// and no JupyterLab in it, the line that installs it; with no Python, that it has none.

test.describe.configure({ timeout: 180_000 });

test("the Notebook entry says what this metalake can give", async ({ page }) => {
  test.skip(!!process.env.METALAKE_E2E_JUPYTER, "JupyterLab is there");
  await page.goto(`/#token=${TOKEN}`);
  await page.getByRole("navigation", { name: "Pages" }).getByRole("link", { name: "Notebook" }).click();
  await expect(page).toHaveURL(/\/notebook$/);
  const said = page.getByRole("region", { name: "Notebook" }).getByRole("status");
  if (process.env.METALAKE_PYTHON) {
    await expect(said).toContainText("JupyterLab isn't installed");
    await expect(said).toContainText('pip install "metalake[notebook]"');
  } else {
    await expect(said).toContainText("runs without Python");
  }
});

test("JupyterLab opens in the page, on the UI's origin, and a cell runs", async ({ page }) => {
  test.skip(!process.env.METALAKE_E2E_JUPYTER, "no JupyterLab");
  await page.goto(`/#token=${TOKEN}`);
  await page.getByRole("navigation", { name: "Pages" }).getByRole("link", { name: "Notebook" }).click();
  const frame = page.frameLocator('iframe[title="JupyterLab"]');
  await expect(page.locator('iframe[title="JupyterLab"]')).toHaveAttribute("src", /^\/jupyter\/lab\?token=/, {
    timeout: 120_000,
  });
  // A notebook of Python, from JupyterLab's launcher.
  await frame.locator('.jp-LauncherCard[data-category="Notebook"]').first().click({ timeout: 60_000 });
  const cell = frame.locator(".jp-Cell .cm-content").first();
  await cell.click();
  await page.keyboard.type("1 + 1");
  await page.keyboard.press("Shift+Enter");
  await expect(frame.locator(".jp-OutputArea-output").first()).toHaveText("2", { timeout: 60_000 });
});
