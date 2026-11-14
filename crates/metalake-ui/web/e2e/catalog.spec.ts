import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The Catalog's views on a lake (ADR 0007): a ref's, a schema's and a table's, read from the lake and kept as it is.

/** The command that reaches the lake the UI serves (`metalake --lake DIR`), as the end-to-end job sets it. */
const lake = process.env.METALAKE;

test("a table's view: its columns, rows, sample and snapshots, and a commit seen without a reload", async ({
  page,
}) => {
  test.skip(!lake, "METALAKE names no lake");
  const name = `cat_${Date.now()}`;
  execSync(
    `${lake} sql "create schema ${name}; create table ${name}.t (k bigint not null, v varchar); insert into ${name}.t values (1, 'a'), (2, 'b')"`,
  );
  await page.goto(`/catalog#token=${TOKEN}`);

  // The ref's catalog at first, its new schema among its schemas.
  const catalog = page.getByRole("region", { name: "Branch main" });
  await expect(catalog.getByRole("table", { name: "Schemas" })).toContainText(name);

  // The schema, then its table, from the tree.
  const assets = page.getByRole("complementary", { name: "Assets" });
  await assets.getByRole("button", { name, exact: true }).click();
  await expect(
    page.getByRole("region", { name: `Schema ${name}` }).getByRole("table", { name: "Tables" }),
  ).toContainText("t");
  await assets.getByRole("list", { name }).getByRole("button", { name: "t", exact: true }).click();
  const table = page.getByRole("region", { name: `Table ${name}.t` });
  await expect(table.getByRole("table", { name: "Columns" })).toContainText("k");

  await table.getByRole("tab", { name: "Sample data" }).click();
  await expect(table.getByRole("table", { name: "Rows" })).toContainText("b");

  await table.getByRole("tab", { name: "History" }).click();
  const snapshots = table.getByRole("table", { name: "Snapshots" }).locator("tbody tr");
  await expect(snapshots).not.toHaveCount(0);
  const before = await snapshots.count();

  // A commit made elsewhere: the view reads the table again, as the lake's event says it changed.
  execSync(`${lake} sql "insert into ${name}.t values (3, 'c')"`);
  await expect(snapshots).toHaveCount(before + 1, { timeout: 15_000 });
  await table.getByRole("tab", { name: "Details" }).click();
  await expect(table.getByRole("tabpanel")).toContainText("3");
});
