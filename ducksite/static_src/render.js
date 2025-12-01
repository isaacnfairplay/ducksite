// ducksite/static_src/render.js
// Render charts and tables by executing compiled SQL.
//
// Parquet backend: **httpfs only**
//   - compiled SQL uses read_parquet(['data/...'])
//   - we rewrite those to absolute HTTP URLs based on window.location.origin
//   - DuckDB's httpfs extension handles range/caching against the HTTP server.

import { initDuckDB, executeQuery } from "./duckdb_runtime.js";
import { CLASS, DATA, PATH } from "./ducksite_contract.js";

/**
 * Compute /sql/... base path for the current page.
 */
function getPageSqlBasePath() {
  const path = window.location.pathname;
  const clean = path.endsWith("/") ? path.slice(0, -1) : path;
  const lastSlash = clean.lastIndexOf("/");
  const dir = lastSlash <= 0 ? "" : clean.slice(0, lastSlash);
  const relDir = dir.startsWith("/") ? dir.slice(1) : dir;

  const base = relDir ? `${PATH.sqlRoot}/${relDir}/` : `${PATH.sqlRoot}/`;
  console.debug("[ducksite] getPageSqlBasePath:", { path, clean, dir, relDir, base });
  return base;
}

/**
 * Build a params map derived from inputs + inputDefs.
 *
 * Two layers:
 *   1) Dropdown inputs with `expression_template` become Boolean predicate snippets
 *      under params[<input_name>], usable as:
 *          WHERE ${params.category_filter}
 *
 *   2) Any input (dropdown or text) may declare:
 *          param_name: <name>
 *          param_template: "sql_snippet_with_?"
 *
 *      In that case we compute:
 *          params[param_name] = param_template.replace("?", '<quoted value>')
 *
 *      and, for *simple prefix* templates, we also derive a plain-text value
 *      for use in query IDs (like picking a templated Parquet view):
 *
 *          param_template: "left(?, 1)"       -> inputs[param_name] = first char
 *          param_template: "substr(?, 1, N)" -> inputs[param_name] = first N chars
 *
 *      Example demo usage:
 *
 *          ```input barcode
 *          label: Barcode
 *          visual_mode: text
 *          url_key: barcode
 *          param_name: barcode_prefix
 *          param_template: "left(?, 1)"
 *          ```
 *
 *          -- later in an echart:
 *          data_query: "global:demo_${inputs.barcode_prefix}"
 */
function buildParamsFromInputs(inputDefs, inputs) {
  const params = {};
  if (!inputDefs) return params;

  // 1) Dropdown predicates (existing behaviour).
  for (const [name, def] of Object.entries(inputDefs)) {
    const visualMode = def.visual_mode || def["visual-mode"] || def.type;
    if (visualMode !== "dropdown") continue;

    const template = def.expression_template || def["expression-template"];
    if (!template) continue;

    const allLabel = def.all_label || def["all-label"] || "ALL";
    const allExpr = def.all_expression || def["all-expression"] || "TRUE";

    const rawValue = inputs[name] ?? def.default ?? allLabel;
    let predicate;

    if (
      rawValue === allLabel ||
      rawValue === "" ||
      rawValue === null ||
      rawValue === undefined
    ) {
      predicate = allExpr;
    } else {
      const v = String(rawValue);
      const escaped = v.replace(/'/g, "''");
      // Template is e.g. "category = ?" so we wrap the value safely here.
      predicate = template.replace("?", `'${escaped}'`);
    }

    // Store under the same name; SQL uses ${params.<name>}
    params[name] = predicate;
  }

  // 2) Generic derived params from any input via param_name / param_template.
  //
  // For example:
  //   param_name: barcode_prefix
  //   param_template: "left(?, 1)"
  //
  // With inputs.barcode = 'ABC123...', we get:
  //   params.barcode_prefix  = "left('ABC123...', 1)"
  //   inputs.barcode_prefix  = "A"
  //
  // which can be referenced as:
  //   - ${params.barcode_prefix} inside SQL
  //   - ${inputs.barcode_prefix} inside query IDs / data_query strings.
  for (const [name, def] of Object.entries(inputDefs)) {
    const targetName = def.param_name || def["param-name"];
    const template = def.param_template || def["param-template"];

    if (!targetName || !template) {
      continue;
    }

    const rawValue = inputs[name] ?? def.default ?? null;
    if (
      rawValue === null ||
      rawValue === undefined ||
      rawValue === ""
    ) {
      // Skip empty values; callers can treat missing params as "no-op".
      continue;
    }

    const v = String(rawValue);
    const escaped = v.replace(/'/g, "''");
    const snippet = template.replace("?", `'${escaped}'`);

    // Try to infer a simple prefix-based "derived ID" from the param_template
    // so we can also expose it in inputs[targetName] for query-id templates.
    //
    // Supported shapes (whitespace-insensitive, case-insensitive):
    //   left(?, N)
    //   substr(?, 1, N)
    //   substring(?, 1, N)
    let derivedId = null;
    const normalized = template.replace(/\s+/g, "").toLowerCase();

    if (normalized.startsWith("left(?,") && normalized.endsWith(")")) {
      const inner = normalized.slice("left(?,".length, -1);
      const n = parseInt(inner, 10);
      if (Number.isFinite(n) && n > 0) {
        derivedId = v.slice(0, n);
      }
    } else if (
      (normalized.startsWith("substr(?,1,") ||
        normalized.startsWith("substring(?,1,")) &&
      normalized.endsWith(")")
    ) {
      const base = normalized.startsWith("substr(?,1,")
        ? "substr(?,1,"
        : "substring(?,1,";
      const inner = normalized.slice(base.length, -1);
      const n = parseInt(inner, 10);
      if (Number.isFinite(n) && n > 0) {
        derivedId = v.slice(0, n);
      }
    }

    if (derivedId !== null) {
      // This lets data_query templates reference ${inputs.<param_name>}
      // directly to select templated views, e.g. "global:demo_${inputs.barcode_prefix}".
      inputs[targetName] = derivedId;
    }

    params[targetName] = snippet;
  }

  console.debug("[ducksite] buildParamsFromInputs: params", params, "inputs (possibly extended)", inputs);
  return params;
}

/**
 * Substitute input and parameter placeholders in SQL.
 *
 * - ${inputs.foo}  -> treated as a scalar; we quote it as a string literal.
 * - ${params.foo}  -> treated as a raw predicate or SQL snippet; we insert it
 *                     directly into the SQL without additional quoting.
 *
 *   If a params.* entry is missing/empty, we substitute **NULL** so that
 *   both boolean and scalar contexts stay type-correct:
 *
 *     WHERE ${params.something}           -> WHERE NULL
 *     category = ${params.barcode_prefix} -> category = NULL
 */
function substituteParams(sqlText, inputs, params) {
  const scopeKey = "(inputs|params)";
  const nameKey = "([A-Za-z0-9_]+)";
  const regex = new RegExp("\\$\\{" + scopeKey + "\\." + nameKey + "\\}", "g");
  const replaced = sqlText.replace(regex, (_, scope, key) => {
    const source = scope === "inputs" ? inputs : params;
    const val =
      source && Object.prototype.hasOwnProperty.call(source, key)
        ? source[key]
        : null;

    if (scope === "params") {
      if (val === null || val === undefined || String(val).trim() === "") {
        return "NULL";
      }
      return String(val);
    }

    if (val === null || val === undefined) {
      return "NULL";
    }
    const s = String(val);
    const escaped = s.replace(/'/g, "''");
    return "'" + escaped + "'";
  });
  console.debug(
    "[ducksite] substituteParams: before/after (first 200 chars)",
    sqlText.slice(0, 200),
    replaced.slice(0, 200),
  );
  return replaced;
}

/**
 * Rewrite read_parquet([...]) paths to absolute HTTP URLs based on current origin.
 * This is the only backend now; OPFS support has been removed.
 */
function rewriteParquetPathsHttp(sqlText) {
  const origin = window.location.origin.replace(/\/$/, "");
  const re = /read_parquet\(\s*\[(.*?)\]\s*\)/gis;

  const rewritten = sqlText.replace(re, (full, inner) => {
    console.debug("[ducksite] rewriteParquetPathsHttp: found read_parquet inner:", inner);
    const parts = inner.split(",");
    const rewrittenParts = parts.map((part) => {
      const trimmed = part.trim();
      const m = /^'([^']*)'$/.exec(trimmed);
      if (!m) {
        console.debug("[ducksite] rewriteParquetPathsHttp: skip non-literal:", part);
        return part;
      }
      let p = m[1];

      if (
        p.startsWith("http://") ||
        p.startsWith("https://") ||
        p.startsWith("//") ||
        p.startsWith("/")
      ) {
        console.debug("[ducksite] rewriteParquetPathsHttp: already absolute:", p);
        return `'${p}'`;
      }

      p = p.replace(/\\/g, "/");

      const url = `${origin}/${p.replace(/^\/+/, "")}`;
      console.debug("[ducksite] rewriteParquetPathsHttp: rewrote", p, "->", url);
      return `'${url}'`;
    });

    const out = `read_parquet([${rewrittenParts.join(", ")}])`;
    console.debug(
      "[ducksite] rewriteParquetPathsHttp: final read_parquet call",
      out.slice(0, 200),
    );
    return out;
  });

  console.debug(
    "[ducksite] rewriteParquetPathsHttp: before/after (first 200 chars)",
    sqlText.slice(0, 200),
    rewritten.slice(0, 200),
  );
  return rewritten;
}

async function loadSqlText(sqlUrl) {
  console.debug("[ducksite] loadSqlText:", sqlUrl);
  const resp = await fetch(sqlUrl);
  if (!resp.ok) {
    console.error("[ducksite] loadSqlText: fetch error", sqlUrl, resp.status);
    throw new Error(`Failed to fetch SQL from ${sqlUrl}: ${resp.status}`);
  }
  const text = await resp.text();
  console.debug(
    "[ducksite] loadSqlText: got SQL (first 200 chars)",
    text.slice(0, 200),
  );
  return text;
}

function ensureEcharts() {
  if (!window.echarts) {
    console.warn("[ducksite] ECharts is not available on window.echarts.");
    return null;
  }
  return window.echarts;
}

function getField(row, key) {
  if (!row || !key) return undefined;
  if (Object.prototype.hasOwnProperty.call(row, key)) {
    return row[key];
  }
  const lower = key.toLowerCase();
  for (const k of Object.keys(row)) {
    if (k.toLowerCase() === lower) {
      return row[k];
    }
  }
  return undefined;
}

/**
 * Helpers for chart building
 */

function toNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function uniqueValues(arr) {
  const seen = new Set();
  const out = [];
  for (const v of arr) {
    const key = v === null || v === undefined ? "__null__" : String(v);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(v);
    }
  }
  return out;
}

function minMaxFromArray(arr) {
  let min = Infinity;
  let max = -Infinity;
  for (const v of arr) {
    const n = Number(v);
    if (!Number.isFinite(n)) continue;
    if (n < min) min = n;
    if (n > max) max = n;
  }
  if (min === Infinity || max === -Infinity) {
    return { min: 0, max: 0 };
  }
  return { min, max };
}

/**
 * Serialize rows to CSV (header + rows).
 */
function rowsToCsv(rows) {
  if (!rows || rows.length === 0) {
    return "";
  }
  const cols = Object.keys(rows[0]);
  const escape = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[",\n]/.test(s)) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };
  const header = cols.map(escape).join(",");
  const lines = [header];
  for (const row of rows) {
    const line = cols.map((c) => escape(row[c])).join(",");
    lines.push(line);
  }
  return lines.join("\r\n");
}

/**
 * Trigger a CSV file download for the given rows.
 */
function downloadCsvForRows(id, rows) {
  const csv = rowsToCsv(rows);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  const safeId = id || "data";
  a.href = url;
  a.download = `${safeId}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

/**
 * Ensure a CSV download button exists inside the container for this id,
 * wired to the provided rows.
 */
function attachDownloadButton(container, id, rows) {
  if (!container) return;
  let btn = container.querySelector(".ducksite-download-btn");
  if (!btn) {
    btn = document.createElement("button");
    btn.type = "button";
    btn.className = "ducksite-download-btn";
    btn.textContent = "CSV";
    container.appendChild(btn);
  }
  btn.onclick = () => {
    if (!rows || rows.length === 0) {
      console.warn("[ducksite] downloadCsv: no rows for", id);
      return;
    }
    downloadCsvForRows(id, rows);
  };
}

/**
 * Build dropdown controls for any inputs that declare `visual_mode: dropdown`,
 * and text boxes for any inputs with `visual_mode: text`.
 *
 * Uses the provided runQuery helper to execute options_query for dropdowns.
 */
async function initInputsUI(inputDefs, inputs, runQuery) {
  const defs = inputDefs || {};
  const entries = Object.entries(defs);

  const controls = entries.filter(([_, def]) => {
    const vm = def.visual_mode || def["visual-mode"] || def.type;
    return vm === "dropdown" || vm === "text";
  });

  if (!controls.length) {
    return;
  }

  let bar = document.querySelector(".ducksite-input-bar");
  if (!bar) {
    bar = document.createElement("div");
    bar.className = "ducksite-input-bar";
    const nav = document.querySelector(".ducksite-nav");
    if (nav && nav.parentNode) {
      nav.parentNode.insertBefore(bar, nav.nextSibling);
    } else if (document.body.firstChild) {
      document.body.insertBefore(bar, document.body.firstChild);
    } else {
      document.body.appendChild(bar);
    }
  } else {
    bar.innerHTML = "";
  }

  for (const [name, def] of controls) {
    const visualMode = def.visual_mode || def["visual-mode"] || def.type;
    const labelText = def.label || name;

    const group = document.createElement("div");
    group.className = "ducksite-input-group";

    const label = document.createElement("label");
    const controlId = `ducksite-input-${name}`;
    label.htmlFor = controlId;
    label.textContent = labelText;

    group.appendChild(label);

    if (visualMode === "dropdown") {
      const select = document.createElement("select");
      select.id = controlId;

      const allLabel = def.all_label || def["all-label"] || "ALL";
      const allValue = def.all_value || def["all-value"] || allLabel;
      const defaultRaw = inputs[name] ?? def.default ?? allValue;

      const optAll = document.createElement("option");
      optAll.value = allValue;
      optAll.textContent = allLabel;
      select.appendChild(optAll);

      if (def.options_query) {
        try {
          const rows = await runQuery(def.options_query);
          if (rows && rows.length > 0) {
            const firstRow = rows[0];
            const cols = Object.keys(firstRow);
            const valueKey = def.value_column || def["value-column"] || cols[0];
            const labelKey =
              def.label_column || def["label-column"] || cols[1] || valueKey;

            for (const row of rows) {
              const v = row[valueKey];
              const l = row[labelKey];
              if (v === null || v === undefined) continue;
              const opt = document.createElement("option");
              opt.value = String(v);
              opt.textContent = l != null ? String(l) : String(v);
              select.appendChild(opt);
            }
          }
        } catch (e) {
          console.error("[ducksite] initInputsUI: options_query error for", name, e);
        }
      }

      select.value = defaultRaw ?? allValue;

      select.addEventListener("change", () => {
        const newVal = select.value;
        if (typeof window.ducksiteSetInput === "function") {
          window.ducksiteSetInput(name, newVal);
        } else {
          console.warn(
            "[ducksite] ducksiteSetInput is not defined; URL sync will not occur.",
          );
        }
      });

      group.appendChild(select);
    } else if (visualMode === "text") {
      const input = document.createElement("input");
      input.id = controlId;
      input.type = "text";
      const defaultRaw = inputs[name] ?? def.default ?? "";
      if (defaultRaw !== null && defaultRaw !== undefined) {
        input.value = String(defaultRaw);
      }
      if (def.placeholder) {
        input.placeholder = String(def.placeholder);
      }

      const handler = () => {
        const newVal = input.value;
        if (typeof window.ducksiteSetInput === "function") {
          window.ducksiteSetInput(name, newVal);
        } else {
          console.warn(
            "[ducksite] ducksiteSetInput is not defined; URL sync will not occur.",
          );
        }
      };

      // Use change to avoid spamming rerenders on every keystroke; can
      // switch to "input" if you want live updates per character.
      input.addEventListener("change", handler);

      group.appendChild(input);
    }

    bar.appendChild(group);
  }
}

/**
 * Very small, robust chart renderer:
 *
 * Simple DSL (no JSON in markdown) supports:
 *   - type: bar | line
 *   - type: scatter | effectScatter
 *   - type: pie
 *   - type: heatmap
 *   - type: gauge
 *   - type: funnel
 *   - type: pictorialBar
 *   - type: sankey
 *   - type: graph
 *   - type: boxplot
 *   - type: candlestick
 *   - type: radar
 *
 * Extra series types are supported via an "option-column" mode:
 *
 *   ```echart treemap_example
 *   data_query: treemap_option_query
 *   type: treemap
 *   option_column: option_json
 *   ```
 *
 * where `option_json` is a VARCHAR/TEXT column whose first row contains a
 * full ECharts option JSON string. This keeps JSON *out* of the DSL while
 * still allowing all ECharts series types.
 */
async function renderChart(container, vizSpec, rows, id) {
  const echarts = ensureEcharts();
  if (!echarts) return null;

  console.debug("[ducksite] renderChart:", { vizSpec, rowCount: rows.length });

  // Clear container and build chart root first
  container.innerHTML = "";
  const chartRoot = document.createElement("div");
  chartRoot.className = "ducksite-chart-root";
  container.appendChild(chartRoot);

  // Attach CSV button *after* chart root so it's on top in the stacking order
  attachDownloadButton(container, id, rows);

  const chart = echarts.init(chartRoot);

  const rawType = vizSpec.type || "bar";
  const type = String(rawType).trim();

  // --- Advanced "option_column" mode for arbitrary ECharts options ---
  //
  // DSL:
  //   option_column: option_json
  //
  // SQL:
  //   SELECT '<valid JSON>'::VARCHAR AS option_json;
  //
  // This keeps JSON out of markdown but lets SQL control *any* ECharts chart.
  const optionColumn =
    vizSpec.option_column || vizSpec.optionColumn || vizSpec.option;
  if (optionColumn) {
    let option = null;
    if (rows && rows.length > 0) {
      const raw = rows[0][optionColumn];
      if (raw !== null && raw !== undefined) {
        let text;
        if (typeof raw === "string") {
          text = raw;
        } else {
          try {
            text = JSON.stringify(raw);
          } catch (e) {
            console.error(
              "[ducksite] renderChart(option_column): JSON.stringify failed, value:",
              raw,
              e,
            );
            text = null;
          }
        }
        if (text) {
          try {
            option = JSON.parse(text);
          } catch (e) {
            console.error(
              "[ducksite] renderChart(option_column): JSON.parse failed, text:",
              text.slice(0, 200),
              e,
            );
          }
        }
      }
    }

    if (!option) {
      console.warn(
        "[ducksite] renderChart(option_column): no usable JSON option in column",
        optionColumn,
      );
      return chart;
    }

    // If the series doesn't specify type but the DSL does, honour the DSL type.
    if (Array.isArray(option.series) && option.series.length > 0) {
      const firstSeries = option.series[0] || {};
      if (!firstSeries.type && type) {
        firstSeries.type = type;
        option.series[0] = firstSeries;
      }
    }

    // If title.text is missing but DSL has title, inject it.
    if (vizSpec.title) {
      option.title = option.title || {};
      if (!option.title.text) {
        option.title.text = vizSpec.title;
      }
    }

    chart.setOption(option);
    return chart;
  }

  // --- Simple DSL-driven modes for common series types ---

  const xKey = vizSpec.x;
  const yKey = vizSpec.y;

  // Helper: basic cartesian bar/line (existing behaviour)
  function buildCartesianXYOption() {
    if (!xKey || !yKey) {
      console.warn("[ducksite] Viz spec missing x or y:", vizSpec);
      return {};
    }
    const categories = rows.map((r) => getField(r, xKey));
    const values = rows.map((r) => getField(r, yKey));

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: categories,
      },
      yAxis: {
        type: "value",
      },
      series: [
        {
          type,
          data: values,
        },
      ],
    };
  }

  function buildScatterOption(scatterType) {
    if (!xKey || !yKey) {
      console.warn("[ducksite] Scatter viz missing x or y:", vizSpec);
      return {};
    }
    const data = rows.map((r) => {
      const x = getField(r, xKey);
      const y = getField(r, yKey);
      return [toNumber(x, x), toNumber(y, y)];
    });

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "item" },
      xAxis: { type: "value" },
      yAxis: { type: "value" },
      series: [
        {
          type: scatterType,
          data,
        },
      ],
    };
  }

  function buildPieOption() {
    const nameKey = vizSpec.name || vizSpec.category || vizSpec.x || "category";
    const valueKey = vizSpec.value || vizSpec.y || "value";

    const data = rows.map((r) => ({
      name: getField(r, nameKey),
      value: getField(r, valueKey),
    }));

    const inner =
      vizSpec.inner_radius ||
      vizSpec.innerRadius ||
      (vizSpec.donut === "true" || vizSpec.ring === "true" ? "40%" : "0%");
    const outer = vizSpec.outer_radius || vizSpec.outerRadius || "70%";

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "item" },
      legend: { top: "5%", left: "center" },
      series: [
        {
          type: "pie",
          radius: [inner, outer],
          data,
        },
      ],
    };
  }

  function buildHeatmapOption() {
    const xK = xKey || vizSpec.x_key || vizSpec.xKey || "x";
    const yK = yKey || vizSpec.y_key || vizSpec.yKey || "y";
    const valueKey =
      vizSpec.value || vizSpec.val || vizSpec.z || "value";

    const rawX = rows.map((r) => getField(r, xK));
    const rawY = rows.map((r) => getField(r, yK));
    const rawV = rows.map((r) => getField(r, valueKey));
    const xs = uniqueValues(rawX);
    const ys = uniqueValues(rawY);
    const data = rows.map((r) => [
      getField(r, xK),
      getField(r, yK),
      toNumber(getField(r, valueKey), 0),
    ]);
    const { min, max } = minMaxFromArray(rawV);

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: {
        position: "top",
      },
      xAxis: {
        type: "category",
        data: xs,
        splitArea: { show: true },
      },
      yAxis: {
        type: "category",
        data: ys,
        splitArea: { show: true },
      },
      visualMap: {
        min,
        max,
        calculable: true,
        orient: "horizontal",
        left: "center",
        bottom: "5%",
      },
      series: [
        {
          type: "heatmap",
          data,
        },
      ],
    };
  }

  function buildGaugeOption() {
    const valueKey =
      vizSpec.value || vizSpec.y || "value";
    const nameKey = vizSpec.name || "value";

    let value = 0;
    if (rows && rows.length > 0) {
      value = toNumber(getField(rows[0], valueKey), 0);
    }

    const name =
      (rows && rows.length > 0 && getField(rows[0], nameKey)) ||
      vizSpec.title ||
      nameKey;

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      series: [
        {
          type: "gauge",
          data: [{ value, name }],
        },
      ],
    };
  }

  function buildFunnelOption() {
    const nameKey = vizSpec.name || vizSpec.category || vizSpec.x || "stage";
    const valueKey = vizSpec.value || vizSpec.y || "value";

    const data = rows.map((r) => ({
      name: getField(r, nameKey),
      value: toNumber(getField(r, valueKey), 0),
    }));

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "item" },
      series: [
        {
          type: "funnel",
          data,
        },
      ],
    };
  }

  function buildPictorialBarOption() {
    if (!xKey || !yKey) {
      console.warn("[ducksite] pictorialBar missing x or y:", vizSpec);
      return {};
    }
    const categories = rows.map((r) => getField(r, xKey));
    const values = rows.map((r) => getField(r, yKey));
    const symbol = vizSpec.symbol || "roundRect";

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: categories,
      },
      yAxis: {
        type: "value",
      },
      series: [
        {
          type: "pictorialBar",
          symbol,
          symbolRepeat: "fixed",
          symbolSize: [20, 8],
          data: values,
        },
      ],
    };
  }

  function buildSankeyOption() {
    const sourceKey =
      vizSpec.source || vizSpec.from || "source";
    const targetKey =
      vizSpec.target || vizSpec.to || "target";
    const valueKey =
      vizSpec.value || vizSpec.weight || "value";

    const nodesSet = new Set();
    const links = rows.map((r) => {
      const s = getField(r, sourceKey);
      const t = getField(r, targetKey);
      const v = toNumber(getField(r, valueKey), 0);
      if (s != null) nodesSet.add(String(s));
      if (t != null) nodesSet.add(String(t));
      return { source: s, target: t, value: v };
    });

    const nodes = Array.from(nodesSet).map((name) => ({ name }));

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "item", triggerOn: "mousemove" },
      series: [
        {
          type: "sankey",
          data: nodes,
          links,
          emphasis: { focus: "adjacency" },
        },
      ],
    };
  }

  function buildGraphOption() {
    const sourceKey =
      vizSpec.source || vizSpec.from || "source";
    const targetKey =
      vizSpec.target || vizSpec.to || "target";
    const valueKey =
      vizSpec.value || vizSpec.weight || "value";
    const layout = vizSpec.layout || "force";

    const nodesSet = new Set();
    const links = rows.map((r) => {
      const s = getField(r, sourceKey);
      const t = getField(r, targetKey);
      const v = toNumber(getField(r, valueKey), 0);
      if (s != null) nodesSet.add(String(s));
      if (t != null) nodesSet.add(String(t));
      return { source: s, target: t, value: v };
    });

    const nodes = Array.from(nodesSet).map((name) => ({ name }));

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: {},
      series: [
        {
          type: "graph",
          layout,
          data: nodes,
          links,
          roam: true,
          label: { show: true },
          force:
            layout === "force"
              ? {
                  repulsion: 100,
                  edgeLength: [30, 80],
                }
              : undefined,
        },
      ],
    };
  }

  function buildBoxplotOption() {
    const nameKey = vizSpec.name || vizSpec.category || vizSpec.x || "name";
    const lowKey = vizSpec.low || "low";
    const q1Key = vizSpec.q1 || "q1";
    const medianKey = vizSpec.median || "median";
    const q3Key = vizSpec.q3 || "q3";
    const highKey = vizSpec.high || "high";

    const categories = rows.map((r) => getField(r, nameKey));
    const values = rows.map((r) => [
      toNumber(getField(r, lowKey), 0),
      toNumber(getField(r, q1Key), 0),
      toNumber(getField(r, medianKey), 0),
      toNumber(getField(r, q3Key), 0),
      toNumber(getField(r, highKey), 0),
    ]);

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "item" },
      xAxis: {
        type: "category",
        data: categories,
      },
      yAxis: { type: "value" },
      series: [
        {
          type: "boxplot",
          data: values,
        },
      ],
    };
  }

  function buildCandlestickOption() {
    const nameKey = vizSpec.name || vizSpec.category || vizSpec.x || "name";
    const openKey = vizSpec.open || "open";
    const closeKey = vizSpec.close || "close";
    const lowKey = vizSpec.low || "low";
    const highKey = vizSpec.high || "high";

    const categories = rows.map((r) => getField(r, nameKey));
    const values = rows.map((r) => [
      toNumber(getField(r, openKey), 0),
      toNumber(getField(r, closeKey), 0),
      toNumber(getField(r, lowKey), 0),
      toNumber(getField(r, highKey), 0),
    ]);

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: categories,
      },
      yAxis: {
        type: "value",
        scale: true,
      },
      series: [
        {
          type: "candlestick",
          data: values,
        },
      ],
    };
  }

  function buildRadarOption() {
    const indicatorKey =
      vizSpec.indicator || vizSpec.axis || "indicator";
    const valueKey = vizSpec.value || vizSpec.y || "value";
    const maxKey = vizSpec.max || null;
    const seriesName = vizSpec.series_name || vizSpec.seriesName || "Series";

    const indicators = [];
    const values = [];
    const rawVals = [];

    for (const r of rows) {
      const name = getField(r, indicatorKey);
      const v = toNumber(getField(r, valueKey), 0);
      values.push(v);
      rawVals.push(v);
      let max = null;
      if (maxKey) {
        max = toNumber(getField(r, maxKey), null);
      }
      indicators.push(
        max != null
          ? { name, max }
          : { name },
      );
    }

    if (!maxKey) {
      const { max } = minMaxFromArray(rawVals);
      const padded = max === 0 ? 1 : max * 1.2;
      for (const ind of indicators) {
        ind.max = padded;
      }
    }

    return {
      title: vizSpec.title ? { text: vizSpec.title } : undefined,
      tooltip: {},
      radar: { indicator: indicators },
      series: [
        {
          type: "radar",
          data: [
            {
              name: seriesName,
              value: values,
            },
          ],
        },
      ],
    };
  }

  let option = {};

  switch (type) {
    case "bar":
    case "line":
      option = buildCartesianXYOption();
      break;

    case "scatter":
    case "effectScatter":
      option = buildScatterOption(type);
      break;

    case "pie":
      option = buildPieOption();
      break;

    case "heatmap":
      option = buildHeatmapOption();
      break;

    case "gauge":
      option = buildGaugeOption();
      break;

    case "funnel":
      option = buildFunnelOption();
      break;

    case "pictorialBar":
      option = buildPictorialBarOption();
      break;

    case "sankey":
      option = buildSankeyOption();
      break;

    case "graph":
      option = buildGraphOption();
      break;

    case "boxplot":
      option = buildBoxplotOption();
      break;

    case "candlestick":
      option = buildCandlestickOption();
      break;

    case "radar":
      option = buildRadarOption();
      break;

    default:
      console.warn(
        "[ducksite] renderChart: unrecognised type for simple DSL, falling back to cartesian:",
        type,
      );
      option = buildCartesianXYOption();
      break;
  }

  chart.setOption(option);
  return chart;
}

function renderTable(container, rows, id) {
  console.debug("[ducksite] renderTable: rowCount", rows ? rows.length : 0);
  container.innerHTML = "";

  if (!rows || rows.length === 0) {
    const empty = document.createElement("div");
    empty.textContent = "No data.";
    container.appendChild(empty);
    // Still attach a button; it will log and no-op if clicked.
    attachDownloadButton(container, id, rows);
    return;
  }

  const cols = Object.keys(rows[0]);
  const table = document.createElement("table");
  table.className = "ducksite-table";

  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
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
      td.textContent = row[c] != null ? String(row[c]) : "";
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);

  container.appendChild(table);

  // Append the button last so it sits above the table in the stacking order
  attachDownloadButton(container, id, rows);
}

/**
 * Substitute `${inputs.foo}` placeholders in a query-id / template string
 * *without* quoting, so we can safely form things like:
 *
 *   global:demo_${inputs.template_view}
 *
 * which should expand to:
 *
 *   global:demo_A
 *
 * The resulting string is treated as an identifier, not SQL.
 */
function substituteIdTemplate(template, inputs) {
  if (!template || template.indexOf("${inputs.") === -1) {
    return template;
  }
  return template.replace(/\$\{inputs\.([A-Za-z0-9_]+)\}/g, (_, key) => {
    if (!inputs || !Object.prototype.hasOwnProperty.call(inputs, key)) {
      return "";
    }
    const v = inputs[key];
    return v == null ? "" : String(v);
  });
}

export async function renderAll(pageConfig, inputs, duckdbBundle) {
  console.debug("[ducksite] renderAll: start", { pageConfig, inputs });

  const sqlBase = getPageSqlBasePath();
  const duckdb = duckdbBundle && duckdbBundle.conn ? duckdbBundle : await initDuckDB();
  const { conn } = duckdb;

  const inputDefs = pageConfig.inputs || {};
  const params = buildParamsFromInputs(inputDefs, inputs);

  const queryCache = new Map();
  const charts = [];

  async function runQuery(queryId) {
    // First, expand any `${inputs.*}` placeholders in the query identifier.
    // This lets us treat the ID itself as a simple template, e.g.:
    //
    //   "global:demo_${inputs.template_view}"
    //
    // which becomes "global:demo_A" when template_view == "demo_A".
    let effectiveId = substituteIdTemplate(queryId, inputs);

    if (!effectiveId) {
      console.warn("[ducksite] runQuery: empty effectiveId from", queryId);
      return [];
    }

    // Cache by the fully-resolved ID so changing inputs leads to distinct
    // cache entries when necessary.
    const cacheKey = effectiveId;
    if (queryCache.has(cacheKey)) {
      console.debug("[ducksite] runQuery: cache hit", effectiveId);
      return queryCache.get(cacheKey);
    }

    // Support a simple "global:" prefix to load compiled global views from:
    //   /sql/_global/<name>.sql
    //
    // This is used by the template demo to point charts at file-source
    // templated views like demo_A, demo_B, etc.
    let basePath = sqlBase;
    if (effectiveId.startsWith("global:")) {
      effectiveId = effectiveId.slice("global:".length);
      basePath = `${PATH.sqlRoot}/_global/`;
    }

    console.debug("[ducksite] runQuery: loading SQL for", {
      originalId: queryId,
      effectiveId,
      basePath,
    });

    const sqlUrl = `${basePath}${effectiveId}.sql`;
    const rawSql = await loadSqlText(sqlUrl);
    const withParams = substituteParams(rawSql, inputs, params);
    const finalSql = rewriteParquetPathsHttp(withParams);

    const rows = await executeQuery(conn, finalSql);
    queryCache.set(cacheKey, rows);
    return rows;
  }

  // Build/refresh dropdown controls + text inputs before rendering grids
  await initInputsUI(inputDefs, inputs, runQuery);

  const vizSpecs = pageConfig.visualizations || {};
  const grids = pageConfig.grids || [];

  for (const grid of grids) {
    console.debug("[ducksite] renderAll: processing grid", grid);
    for (const row of grid.rows || []) {
      for (const cell of row) {
        const cellId = cell.id;
        if (!cellId) continue;
        const vizSpec = vizSpecs[cellId];
        if (vizSpec) {
          const queryId = vizSpec.data_query || vizSpec.dataQuery || cellId;
          const selector = `.${CLASS.vizContainer}[${DATA.vizId}="${cellId}"]`;
          const container = document.querySelector(selector);
          if (!container) {
            console.warn("[ducksite] renderAll: viz container not found for", cellId);
            continue;
          }
          const rows = await runQuery(queryId);
          const chartInstance = await renderChart(container, vizSpec, rows, cellId);
          if (chartInstance) {
            charts.push(chartInstance);
          }
        } else {
          const queryId = cellId;
          const selector = `.${CLASS.tableContainer}[${DATA.tableId}="${cellId}"]`;
          const container = document.querySelector(selector);
          if (!container) {
            console.warn("[ducksite] renderAll: table container not found for", cellId);
            continue;
          }
          const rows = await runQuery(queryId);
          renderTable(container, rows, cellId);
        }
      }
    }
  }

  // Resize charts on window resize
  if (charts.length > 0) {
    const resizeHandler = () => {
      for (const chart of charts) {
        try {
          chart.resize();
        } catch (e) {
          console.warn("[ducksite] chart.resize() failed:", e);
        }
      }
    };

    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(resizeHandler);
    } else {
      resizeHandler();
    }

    window.addEventListener("resize", resizeHandler);
  }

  console.debug("[ducksite] renderAll: done");
}

// Export helpers so the SQL editor can materialise views with the same
// parameter semantics and httpfs path rewriting.
export { buildParamsFromInputs, substituteParams, rewriteParquetPathsHttp };
