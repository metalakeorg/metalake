import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The Notebook's entry (ADR 0008, N1): what this `metalake` can give. With a Python and no JupyterLab in it, as CI runs
// it, the line that installs it; with no Python, that it has none.

test("the Notebook entry says what this metalake can give", async ({ page }) => {
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
