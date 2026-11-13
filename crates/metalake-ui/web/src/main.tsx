import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { Frame } from "./Frame";
import "./index.css";

const root = document.getElementById("root");
if (!root) {
  throw new Error("index.html has no #root");
}

createRoot(root).render(
  <StrictMode>
    <Frame />
  </StrictMode>,
);
