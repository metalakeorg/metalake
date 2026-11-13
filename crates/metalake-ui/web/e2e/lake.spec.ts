import { execSync } from "node:child_process";
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

// `METALAKE` is the command that reaches the lake the UI serves (`metalake --lake DIR`), as the end-to-end job sets it.
const lake = process.env.METALAKE;

test("a table made on the lake is listed at once, and a branch made is in the selector", async ({ page }) => {
  test.skip(!lake, "METALAKE names no lake");
  await page.goto(`/#token=${TOKEN}`);
  const assets = page.getByRole("complementary", { name: "Assets" });
  await expect(assets.getByRole("combobox", { name: "Branch" })).toHaveValue("main");
  const name = `live_${Date.now()}`;
  execSync(`${lake} sql "create schema if not exists main.live; create table main.live.${name} (k bigint)"`);
  // No reload: the lake's event makes the page read its catalog again.
  await expect(assets.getByRole("list", { name: "live" })).toContainText(name);
  execSync(`${lake} branch create ${name}`);
  await expect(assets.getByRole("option", { name })).toBeAttached({ timeout: 15_000 });
});

test("an address that is no page says so", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  await page.goto("/nowhere/at/all");
  await expect(page.getByRole("alert")).toContainText("No such page");
});
