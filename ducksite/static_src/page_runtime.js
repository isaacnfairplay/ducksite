// ducksite/static_src/page_runtime.js
// Verbose logging of page lifecycle, with live re-render on input changes.

import { readPageConfig } from "./page_config.js";
import { initInputsFromUrl, createInputApi } from "./inputs.js";
import { initDuckDB } from "./duckdb_runtime.js";
import { renderAll } from "./render.js";
import { initSqlEditor } from "./sql_editor.js";
import { initFormsUI } from "./forms.js";

export async function initPage() {
  console.debug("[ducksite] initPage: start");

  const pageConfig = readPageConfig();
  if (!pageConfig) {
    console.warn("[ducksite] initPage: no page config; nothing to do.");
    return;
  }

  // 1) Initial inputs from URL and URL-sync API
  let inputs = initInputsFromUrl(pageConfig.inputs || {});
  createInputApi(inputs, pageConfig.inputs || {});
  console.debug("[ducksite] initPage: inputs after URL init", inputs);

  initFormsUI(inputs);

  // 2) Initialise DuckDB once
  const duckdbBundle = await initDuckDB();
  console.debug("[ducksite] initPage: duckdbBundle", duckdbBundle);

  let renderState = null;

  function computeChangedInputs(prev, next) {
    const keys = new Set([...Object.keys(prev || {}), ...Object.keys(next || {})]);
    const changed = new Set();
    for (const key of keys) {
      if (JSON.stringify(prev[key]) !== JSON.stringify(next[key])) {
        changed.add(key);
      }
    }
    return changed;
  }

  // Helper to (re)render the whole page for a given inputs object.
  async function rerender(currentInputs, changeInfo = null) {
    try {
      console.debug("[ducksite] initPage: rerender with inputs", currentInputs, changeInfo);
      renderState = await renderAll(pageConfig, currentInputs, duckdbBundle, renderState, changeInfo);
      console.debug("[ducksite] initPage: renderAll complete");
    } catch (err) {
      console.error("[ducksite] initPage: renderAll error", err);
      throw err;
    }
  }

  // 3) Initial render
  await rerender(inputs);

  // 4) Initialise the global SQL editor / hamburger menu.
  //
  // The editor:
  //   - uses the same DuckDB connection (initDuckDB reuses instances)
  //   - exposes global views compiled from all NamedQuery entries
  //   - can also prepare chart-local views like chart_data_<vizId>
  //
  // It always pulls the latest inputs via window.ducksiteGetInputs()
  // before materialising templated queries.
  initSqlEditor(pageConfig);

  // 5) Live updates: when inputs change (via dropdowns, etc.), rerun renderAll.
  //
  // inputs.js dispatches:
  //   new CustomEvent("ducksiteInputsChanged", { detail: { inputs } })
  //
  // We listen once here and re-use the existing DuckDB connection and page config.
  window.addEventListener("ducksiteInputsChanged", (ev) => {
    const detail = ev.detail || {};
    const nextInputs = detail.inputs || {};
    const changedInputs = computeChangedInputs(inputs, nextInputs);
    inputs = nextInputs;
    void rerender(inputs, { changedInputs });
  });
}
