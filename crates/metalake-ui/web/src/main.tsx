import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { adopt, Unauthorized } from "./auth";
import { Failure } from "./client";
import { router } from "./router";
import "./index.css";

// Before anything renders: the token leaves the address bar, and every request bears it.
adopt();

/** A request the API answered is not tried again: it would answer the same. One that did not reach it is, once. */
const queries = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (count, error) => !(error instanceof Failure || error instanceof Unauthorized) && count < 1,
      refetchOnWindowFocus: false,
    },
  },
});

const root = document.getElementById("root");
if (!root) {
  throw new Error("index.html has no #root");
}

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queries}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);
