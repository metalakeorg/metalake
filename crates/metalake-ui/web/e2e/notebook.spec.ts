import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The Notebook (ADR 0008). Where `METALAKE_E2E_JUPYTER` is set, `METALAKE_PYTHON` has JupyterLab, as CI's end-to-end
// job gives it, with metalake installed in it: the page starts it and shows it, and a cell of MetaLake's kernel reads
// the lake. Else, what this `metalake` can give: with a Python
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

test("JupyterLab opens in the page, on the UI's origin, and MetaLake's kernel reads the lake", async ({ page }) => {
  test.skip(!process.env.METALAKE_E2E_JUPYTER, "no JupyterLab");
  await page.goto(`/#token=${TOKEN}`);
  await page.getByRole("navigation", { name: "Pages" }).getByRole("link", { name: "Notebook" }).click();
  const frame = page.frameLocator('iframe[title="JupyterLab"]');
  await expect(page.locator('iframe[title="JupyterLab"]')).toHaveAttribute("src", /^\/jupyter\/lab\?token=/, {
    timeout: 120_000,
  });
  // A notebook of MetaLake's kernel, the launcher's only one: `lake` is there as it starts, on `main`.
  const notebooks = frame.locator('.jp-LauncherCard[data-category="Notebook"]');
  await expect(notebooks).toHaveCount(1, { timeout: 60_000 });
  await expect(notebooks).toContainText("MetaLake");
  await notebooks.click();
  // Its start imports metalake and reaches the lake: a cell run before it is ready is not run.
  await expect(frame.locator('.jp-StatusBar-TextItem[title^="Change kernel"]')).toHaveText("MetaLake | Idle", {
    timeout: 90_000,
  });
  const cell = frame.locator(".jp-Cell .cm-content").first();
  await cell.click();
  await page.keyboard.type('lake.ref, lake.sql("select 1 as one").columns');
  await page.keyboard.press("Shift+Enter");
  await expect(frame.locator(".jp-OutputArea-output").first()).toHaveText("('main', ['one'])", { timeout: 90_000 });
});

// `METALAKE` is the command that reaches the lake the UI serves, as the end-to-end job sets it.
const lake = process.env.METALAKE;

test("the notebook's ref: chosen in it, where its cells write, and applied again to a new kernel", async ({ page }) => {
  test.skip(!process.env.METALAKE_E2E_JUPYTER || !lake, "no JupyterLab, or no lake");
  const branch = `nb_${Date.now()}`;
  execSync(`${lake} branch create ${branch}`);
  await page.goto(`/#token=${TOKEN}`);
  await page.getByRole("navigation", { name: "Pages" }).getByRole("link", { name: "Notebook" }).click();
  const frame = page.frameLocator('iframe[title="JupyterLab"]');
  // A new notebook, from a new launcher: the one JupyterLab shows may hold another notebook.
  await frame.locator('[data-command="launcher:create"]').first().click({ timeout: 120_000 });
  await frame.locator('.jp-Launcher:not(.lm-mod-hidden) .jp-LauncherCard[data-category="Notebook"]').click();
  const notebook = frame.locator(".jp-NotebookPanel:not(.lm-mod-hidden)");
  const kernel = frame.locator('.jp-StatusBar-TextItem[title^="Change kernel"]');
  await expect(kernel).toHaveText("MetaLake | Idle", { timeout: 90_000 });
  /** Runs `code` in the notebook's last cell, and answers its output. */
  const run = async (code: string) => {
    await notebook.locator(".jp-Cell .cm-content").last().click();
    await page.keyboard.type(code);
    await page.keyboard.press("Shift+Enter");
    return notebook.locator(".jp-OutputArea-output").last();
  };

  // The selector lists the lake's refs as it is opened, the branch made just now among them.
  const ref = notebook.getByRole("combobox", { name: "Ref" });
  await expect(ref).toHaveValue("main");
  await ref.focus();
  await ref.selectOption(branch);
  await expect(await run("lake.ref")).toHaveText(`'${branch}'`, { timeout: 60_000 });
  // The cell says "written" once every statement ran; an error would be its output instead.
  const write = 'lake.sql("create schema nb; create table nb.t (k bigint); insert into nb.t values (1)"), "written"';
  await expect(await run(`(${write})[1]`)).toHaveText("'written'", { timeout: 60_000 });

  // A new kernel starts on `main`: the notebook's ref is applied to it again.
  await notebook
    .locator('[data-command="kernelmenu:restart"], [data-command="notebook:restart-kernel"]')
    .first()
    .click();
  await frame.locator(".jp-Dialog").getByRole("button", { name: "Restart", exact: true }).click();
  await expect(kernel).toHaveText("MetaLake | Idle", { timeout: 90_000 });
  await expect(await run("lake.ref")).toHaveText(`'${branch}'`, { timeout: 60_000 });

  // The Query page finds the table the notebook wrote on its branch, and not on `main`.
  await page.getByRole("navigation", { name: "Pages" }).getByRole("link", { name: "Query" }).click();
  const assets = page.getByRole("complementary", { name: "Assets" });
  await assets.getByRole("combobox", { name: "Branch" }).selectOption(branch);
  await expect(assets.getByRole("list", { name: "nb" })).toContainText("t");
  await assets.getByRole("combobox", { name: "Branch" }).selectOption("main");
  await expect(assets.getByRole("list", { name: "nb" })).not.toBeAttached();
});
