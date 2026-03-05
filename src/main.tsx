import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import "react-autoql/dist/autoql.esm.css";
import "./styles/react-autoql-overrides.css";

// StrictMode is disabled because react-autoql Dashboard component
// performs direct DOM manipulation that conflicts with StrictMode's
// double-rendering in development. This only affects development mode;
// production builds ignore StrictMode anyway.
createRoot(document.getElementById("root")!).render(
  <App />
);
