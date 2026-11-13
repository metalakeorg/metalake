import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The frame on a lake (ADR 0007, milestone 6): `metalake ui` runs on one, as the end-to-end job starts it.

test("the UI opens on Query, the sidebar's first entry, Notebook after it", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  const sidebar = page.getByRole("navigation", { name: "Pages" });
  await expect(sidebar.getByRole("link")).toHaveText(["Query", "Notebook"]);
  await expect(page.getByRole("main").getByRole("heading", { name: "Query" })).toBeVisible();
  await page.reload();
  await expect(page.getByRole("main").getByRole("heading", { name: "Query" })).toBeVisible();
});

// `METALAKE` is the command that reaches the lake the UI serves (`metalake --lake DIR`), as the end-to-end job sets it.
const lake = process.env.METALAKE;

test("a table made on the lake is listed at once, and a branch made is in the selector", async ({ page }) => {
  test.skip(!lake, "METALAKE names no lake");
  await page.goto(`/#token=${TOKEN}`);
  const assets = page.getByRole("complementary", { name: "Assets" });
  await expect(assets.getByRole("combobox", { name: "Branch" })).toHaveValue("main");
  const name = `live_${Date.now()}`;
  execSync(`${lake} sql "create schema main.${name}; create table main.${name}.t (k bigint)"`);
  // No reload: the lake's event makes the page read its catalog again.
  await expect(assets.getByRole("list", { name })).toContainText("t");
  execSync(`${lake} branch create ${name}`);
  await expect(assets.getByRole("option", { name })).toBeAttached({ timeout: 15_000 });
});

/** Writes `text` in the editor, over what it held, and runs it. */
async function runInEditor(page: import("@playwright/test").Page, text: string) {
  await page.getByRole("textbox", { name: "SQL" }).click();
  await page.keyboard.press("Control+a");
  await page.keyboard.type(text);
  await page.keyboard.press("Control+Enter");
}

test("the editor reads the lake, and a table it writes is in the tree at once", async ({ page }) => {
  test.skip(!lake, "METALAKE names no lake");
  await page.goto(`/#token=${TOKEN}`);
  await runInEditor(page, "select 41 + 1 as answer");
  const rows = page.getByRole("table", { name: "Rows" });
  await expect(rows.getByRole("columnheader", { name: "answer" })).toBeVisible();
  await expect(rows.getByRole("cell", { name: "42" })).toBeVisible();

  const name = `edited_${Date.now()}`;
  await runInEditor(
    page,
    `create schema ${name}; create table ${name}.t (k bigint); insert into ${name}.t values (1), (2)`,
  );
  await expect(page.getByRole("complementary", { name: "Assets" }).getByRole("list", { name })).toContainText("t");
  await runInEditor(page, `select count(*) as n from ${name}.t`);
  await expect(page.getByRole("table", { name: "Rows" }).getByRole("cell", { name: "2" })).toBeVisible();
});

test("an address that is no page says so", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  await page.goto("/nowhere/at/all");
  await expect(page.getByRole("alert")).toContainText("No such page");
});
