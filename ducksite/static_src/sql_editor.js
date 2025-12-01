// ducksite/static_src/sql_editor.js
// Global SQL editor + hamburger menu.
//
// Features:
//   - Loads a manifest of globally compiled views: static/sql/_manifest.json
//   - Lets the user select which views to depend on (we CREATE VIEW <name> AS ...)
//   - Lets the user pick a chart and materialise `chart_data_<vizId>` as a view
//     so they can run queries like:
//         SELECT * FROM chart_data_filtered_chart WHERE cost > 10;
//
// The editor reuses the same DuckDB-WASM instance as the page runtime.

import { PATH } from "./ducksite_contract.js";
import { initDuckDB, executeQuery } from "./duckdb_runtime.js";
import {
  buildParamsFromInputs,
  substituteParams,
  rewriteParquetPathsHttp,
} from "./render.js";

let manifestPromise = null;

async function loadManifest() {
  if (manifestPromise) {
    return manifestPromise;
  }
  const url = `${PATH.sqlRoot}/_manifest.json`;
  console.debug("[ducksite] sql_editor: loading manifest", url);
  manifestPromise = (async () => {
    try {
      const resp = await fetch(url);
      if (!resp.ok) {
        console.warn("[ducksite] sql_editor: manifest fetch failed", resp.status);
        return { views: {} };
      }
      const data = await resp.json();
      console.debug("[ducksite] sql_editor: manifest data", data);
      return data || { views: {} };
    } catch (e) {
      console.error("[ducksite] sql_editor: manifest error", e);
      return { views: {} };
    }
  })();
  return manifestPromise;
}

function stripMetricsHeader(sqlText) {
  const lines = sqlText.split("\n");
  if (lines.length > 0 && lines[0].startsWith("-- METRICS:")) {
    return lines.slice(1).join("\n");
  }
  return sqlText;
}

function getCurrentInputs() {
  try {
    if (typeof window.ducksiteGetInputs === "function") {
      return window.ducksiteGetInputs() || {};
    }
  } catch (e) {
    console.warn("[ducksite] sql_editor: ducksiteGetInputs error", e);
  }
  return {};
}

function ensureNavRightContainer() {
  const nav = document.querySelector(".ducksite-nav");
  if (!nav) return null;

  let right = nav.querySelector(".ducksite-nav-right");
  if (!right) {
    right = document.createElement("div");
    right.className = "ducksite-nav-right";
    nav.appendChild(right);
  }
  return right;
}

function ensureOverlaySkeleton(pageConfig) {
  let overlay = document.querySelector(".ducksite-sql-overlay");
  if (overlay) {
    return overlay;
  }

  overlay = document.createElement("div");
  overlay.className = "ducksite-sql-overlay";

  const panel = document.createElement("div");
  panel.className = "ducksite-sql-panel";

  // Header
  const header = document.createElement("div");
  header.className = "ducksite-sql-header";

  const title = document.createElement("div");
  title.className = "ducksite-sql-title";
  title.textContent = "SQL editor (global views + chart data)";
  header.appendChild(title);

  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "ducksite-sql-close";
  closeBtn.textContent = "×";
  closeBtn.addEventListener("click", () => {
    overlay.style.display = "none";
  });
  header.appendChild(closeBtn);

  panel.appendChild(header);

  // Controls row: chart selector + views multiselect
  const controls = document.createElement("div");
  controls.className = "ducksite-sql-controls";

  const chartGroup = document.createElement("div");
  chartGroup.className = "ducksite-sql-group";
  const chartLabel = document.createElement("label");
  chartLabel.textContent = "Chart data view (optional)";
  const chartSelect = document.createElement("select");
  chartSelect.id = "ducksite-sql-chart-select";

  const noChartOpt = document.createElement("option");
  noChartOpt.value = "";
  noChartOpt.textContent = "(none)";
  chartSelect.appendChild(noChartOpt);

  const viz = pageConfig.visualizations || {};
  const vizEntries = Object.entries(viz);
  vizEntries.sort(([aId], [bId]) => aId.localeCompare(bId));
  for (const [vizId, spec] of vizEntries) {
    const opt = document.createElement("option");
    opt.value = vizId;
    const titleText = spec.title ? ` – ${spec.title}` : "";
    opt.textContent = `${vizId}${titleText}`;
    chartSelect.appendChild(opt);
  }

  chartGroup.appendChild(chartLabel);
  chartGroup.appendChild(chartSelect);
  controls.appendChild(chartGroup);

  const viewsGroup = document.createElement("div");
  viewsGroup.className = "ducksite-sql-group";
  const viewsLabel = document.createElement("label");
  viewsLabel.textContent = "Views to register (multi-select)";
  const viewsSelect = document.createElement("select");
  viewsSelect.id = "ducksite-sql-views-select";
  viewsSelect.multiple = true;
  viewsGroup.appendChild(viewsLabel);
  viewsGroup.appendChild(viewsSelect);
  controls.appendChild(viewsGroup);

  panel.appendChild(controls);

  // SQL textarea
  const textarea = document.createElement("textarea");
  textarea.id = "ducksite-sql-textarea";
  textarea.className = "ducksite-sql-textarea";
  textarea.placeholder = "SELECT 1;";
  textarea.value = "SELECT 1;";
  panel.appendChild(textarea);

  // Actions
  const actions = document.createElement("div");
  actions.className = "ducksite-sql-actions";

  const prepareChartBtn = document.createElement("button");
  prepareChartBtn.type = "button";
  prepareChartBtn.className = "ducksite-sql-prepare-chart";
  prepareChartBtn.textContent = "Prepare chart_data view";
  actions.appendChild(prepareChartBtn);

  const runBtn = document.createElement("button");
  runBtn.type = "button";
  runBtn.className = "ducksite-sql-run";
  runBtn.textContent = "Run SQL";
  actions.appendChild(runBtn);

  panel.appendChild(actions);

  // Results
  const results = document.createElement("div");
  results.id = "ducksite-sql-results";
  results.className = "ducksite-sql-results";
  panel.appendChild(results);

  overlay.appendChild(panel);
  document.body.appendChild(overlay);

  // Wire actions with closures over DOM refs
  prepareChartBtn.addEventListener("click", () => {
    void prepareChartDataView(pageConfig, chartSelect, textarea);
  });

  runBtn.addEventListener("click", () => {
    void runSqlWithContext(pageConfig, chartSelect, viewsSelect, textarea, results);
  });

  return overlay;
}

async function populateViewsSelect() {
  const manifest = await loadManifest();
  const views = manifest.views || {};
  const select = document.getElementById("ducksite-sql-views-select");
  if (!select) return;

  select.innerHTML = "";
  const names = Object.keys(views);
  names.sort((a, b) => a.localeCompare(b));

  for (const name of names) {
    const info = views[name] || {};
    const opt = document.createElement("option");
    opt.value = name;
    const kind = info.kind || "";
    const deps = Array.isArray(info.deps) ? info.deps : [];
    const depsLabel = deps.length ? ` [deps: ${deps.join(", ")}]` : "";
    opt.textContent = `${name} (${kind})${depsLabel}`;
    select.appendChild(opt);
  }
}

async function materialiseViewsForSql(pageConfig, selectedViews, chartVizIdForAlias) {
  const manifest = await loadManifest();
  const views = manifest.views || {};
  const { conn } = await initDuckDB();

  const inputDefs = pageConfig.inputs || {};
  const inputs = getCurrentInputs();
  const params = buildParamsFromInputs(inputDefs, inputs);

  // Helper to create or replace a view from a manifest entry.
  async function createViewFromManifest(name, targetNameOverride = null) {
    const info = views[name];
    if (!info || !info.sql_path) {
      console.warn("[ducksite] sql_editor: missing manifest for view", name);
      return;
    }
    const sqlUrl = info.sql_path;
    const raw = await (await fetch(sqlUrl)).text();
    const body = stripMetricsHeader(raw);
    const withParams = substituteParams(body, inputs, params);
    const finalSql = rewriteParquetPathsHttp(withParams).trim().replace(/^;+|;+$/g, "");;
    const target = targetNameOverride || name;
    const createSql = `CREATE OR REPLACE VIEW ${target} AS (${finalSql})`;
    console.debug(
      "[ducksite] sql_editor: CREATE VIEW for",
      target,
      "using base",
      name,
      "SQL (first 200 chars)",
      finalSql.slice(0, 200),
    );
    await executeQuery(conn, createSql);
  }

  // 1) Materialise selected global views (and their direct deps).
  for (const name of selectedViews) {
    if (!views[name]) continue;
    const info = views[name];
    const deps = Array.isArray(info.deps) ? info.deps : [];
    for (const dep of deps) {
      if (views[dep]) {
        await createViewFromManifest(dep);
      }
    }
    await createViewFromManifest(name);
  }

  // 2) Optional chart_data_<vizId> alias, backed by the viz's data_query.
  if (chartVizIdForAlias) {
    const viz = (pageConfig.visualizations || {})[chartVizIdForAlias];
    if (!viz) {
      console.warn("[ducksite] sql_editor: no viz spec for chart", chartVizIdForAlias);
      return;
    }
    const baseQueryId = viz.data_query || viz.dataQuery || chartVizIdForAlias;
    if (!views[baseQueryId]) {
      console.warn(
        "[ducksite] sql_editor: base query for chart not in manifest",
        chartVizIdForAlias,
        baseQueryId,
      );
      return;
    }
    const aliasName = `chart_data_${chartVizIdForAlias}`;
    await createViewFromManifest(baseQueryId, aliasName);
  }
}

async function prepareChartDataView(pageConfig, chartSelect, textarea) {
  const vizId = chartSelect.value || "";
  if (!vizId) {
    console.warn("[ducksite] sql_editor: no chart selected for chart_data alias");
    return;
  }
  await materialiseViewsForSql(pageConfig, [], vizId);

  // Pre-fill a helpful skeleton query.
  const aliasName = `chart_data_${vizId}`;
  const hint = `-- Example: inspect chart data for ${vizId}\nSELECT * FROM ${aliasName} WHERE 1 = 0;\n`;
  const existing = textarea.value || "";
  if (!existing.trim() || existing.trim() === "SELECT 1;") {
    textarea.value = hint;
  } else if (!existing.includes(aliasName)) {
    textarea.value = `${hint}\n${existing}`;
  }
}

function renderResultsTable(rows, container) {
  container.innerHTML = "";

  if (!rows || rows.length === 0) {
    const div = document.createElement("div");
    div.textContent = "No rows.";
    container.appendChild(div);
    return;
  }

  const table = document.createElement("table");
  table.className = "ducksite-sql-table";

  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  const cols = Object.keys(rows[0]);
  for (const c of cols) {
    const th = document.createElement("th");
    th.textContent = c;
    trHead.appendChild(th);
  }
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const c of cols) {
      const td = document.createElement("td");
      const v = row[c];
      td.textContent = v != null ? String(v) : "";
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);

  container.appendChild(table);
}

async function runSqlWithContext(
  pageConfig,
  chartSelect,
  viewsSelect,
  textarea,
  resultsContainer,
) {
  const sqlText = textarea.value || "";
  const errorDiv = document.createElement("div");
  errorDiv.className = "ducksite-sql-error";

  resultsContainer.innerHTML = "";
  resultsContainer.appendChild(errorDiv);

  if (!sqlText.trim()) {
    errorDiv.textContent = "SQL is empty.";
    return;
  }

  const selectedViews = Array.from(viewsSelect.selectedOptions || []).map(
    (opt) => opt.value,
  );
  const vizId = chartSelect.value || "";

  try {
    await materialiseViewsForSql(pageConfig, selectedViews, vizId);

    const { conn } = await initDuckDB();
    console.debug(
      "[ducksite] sql_editor: executing SQL (first 200 chars)",
      sqlText.slice(0, 200),
    );
    const rows = await executeQuery(conn, sqlText);
    errorDiv.textContent = "";
    renderResultsTable(rows, resultsContainer);
  } catch (e) {
    console.error("[ducksite] sql_editor: run error", e);
    errorDiv.textContent = String(e);
  }
}

export function initSqlEditor(pageConfig) {
  const right = ensureNavRightContainer();
  if (!right) {
    console.warn("[ducksite] sql_editor: nav not found; skipping editor init.");
    return;
  }

  let btn = right.querySelector(".ducksite-sql-menu-btn");
  if (!btn) {
    btn = document.createElement("button");
    btn.type = "button";
    btn.className = "ducksite-sql-menu-btn";
    btn.textContent = "≡ SQL";
    right.appendChild(btn);
  }

  btn.addEventListener("click", async () => {
    const overlay = ensureOverlaySkeleton(pageConfig);
    overlay.style.display = "flex";
    await populateViewsSelect();
  });
}
