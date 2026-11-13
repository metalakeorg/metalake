import { expect, test } from "@playwright/test";
import { TOKEN } from "../playwright.config";

// The Query page's shell: an editor that highlights SQL and runs on Ctrl-Enter, though running reaches no lake yet.

test("the editor takes SQL, highlights it, and runs it on Ctrl-Enter", async ({ page }) => {
  // What the browser refuses, a style the page's policy blocks among it, is said in the console.
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  await page.goto(`/#token=${TOKEN}`);
  const editor = page.getByRole("textbox", { name: "SQL" });
  await editor.click();
  await page.keyboard.type("select 1 as one -- a comment");
  await expect(page.locator(".tok-keyword").first()).toHaveText("select");
  await expect(page.locator(".tok-comment")).toHaveText("-- a comment");
  await page.keyboard.press("Control+Enter");
  await expect(page.getByRole("status")).toContainText("does not reach the lake yet");
  // Its gutter beside the text, as its own style sheet lays it out.
  const gutter = await page.locator(".cm-gutters").boundingBox();
  const content = await page.locator(".cm-content").boundingBox();
  expect(gutter && content && gutter.x + gutter.width <= content.x).toBe(true);
  expect(errors).toEqual([]);
});

test("Run with nothing written says so", async ({ page }) => {
  await page.goto(`/#token=${TOKEN}`);
  await page.getByRole("button", { name: "Run" }).click();
  await expect(page.getByRole("region", { name: "Results" })).toContainText("nothing to run");
});
