import { defineConfig, devices } from "@playwright/test";

// The end-to-end tests drive a browser against the UI as `metalake ui` serves it from the binary built from this
// checkout, never against Vite's dev server: the server shipped is the one tested (ADR 0007 §8). Where the command line
// is not built, `METALAKE_UI_COMMAND` names another that serves the same, such as metalake-ui's `serve` example.
const port = 4173;
const command = process.env.METALAKE_UI_COMMAND ?? "../../../target/debug/metalake ui --no-browser";

export default defineConfig({
  testDir: "e2e",
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: process.env.CI ? [["list"], ["github"]] : "list",
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `${command} --port ${port}`,
    url: `http://127.0.0.1:${port}/`,
    reuseExistingServer: false,
    stdout: "pipe",
    stderr: "pipe",
    timeout: 30_000,
  },
});
