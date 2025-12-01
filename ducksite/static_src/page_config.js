// ducksite/static_src/page_config.js
// Verbose logging for config parsing.

import { ID } from "./ducksite_contract.js";

export function readPageConfig() {
  const el = document.getElementById(ID.pageConfigJson);
  if (!el) {
    console.warn("[ducksite] page-config-json element not found.");
    return null;
  }
  try {
    const text = el.textContent || "{}";
    const cfg = JSON.parse(text);
    console.debug("[ducksite] readPageConfig:", cfg);
    return cfg;
  } catch (e) {
    console.error("[ducksite] Failed to parse page-config-json:", e);
    return null;
  }
}
