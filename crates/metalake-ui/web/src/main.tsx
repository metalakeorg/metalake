import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { adopt } from "./auth";
import { Frame } from "./Frame";
import { useServer } from "./server";
import "./index.css";

// Before anything renders: the token leaves the address bar, and every request bears it.
adopt();

function App() {
  return <Frame server={useServer()} />;
}

const root = document.getElementById("root");
if (!root) {
  throw new Error("index.html has no #root");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
