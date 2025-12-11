// ducksite/static_src/inputs.js
// Verbose logging for URL <-> inputs mapping.

export function isMultiple(def = {}) {
  const raw = def.multiple ?? def.multi;
  if (typeof raw === "boolean") return raw;
  if (raw === undefined || raw === null) return false;
  return String(raw).toLowerCase() === "true";
}

export function normalizeMultiValues(raw) {
  if (Array.isArray(raw)) {
    return raw
      .filter((v) => v !== undefined && v !== null)
      .map((v) => String(v));
  }
  if (raw === null || raw === undefined || raw === "") return [];
  return String(raw)
    .split(",")
    .map((v) => v.trim())
    .filter((v) => v !== "");
}

export function initInputsFromUrl(inputDefs) {
  const inputs = {};
  const sp = new URLSearchParams(window.location.search);

  console.debug("[ducksite] initInputsFromUrl: defs", inputDefs);
  console.debug("[ducksite] initInputsFromUrl: URLSearchParams", Object.fromEntries(sp.entries()));

  for (const [name, def] of Object.entries(inputDefs)) {
    const key = def.url_key || def["url-key"] || name;
    const multiple = isMultiple(def);
    const raw = multiple ? sp.getAll(key) : sp.get(key);
    if (multiple) {
      const flattened = raw.flatMap((v) => normalizeMultiValues(v));
      const value = flattened.length ? flattened : normalizeMultiValues(def.default);
      inputs[name] = value;
    } else {
      let value = raw;
      if (raw === null || raw === "") {
        value = def.default ?? null;
      }
      inputs[name] = value;
    }
  }

  console.debug("[ducksite] initInputsFromUrl: initial inputs", inputs);
  return inputs;
}

export function createInputApi(inputs, inputDefs) {
  console.debug("[ducksite] createInputApi: starting with inputs", inputs);

  window.ducksiteGetInputs = () => ({ ...inputs });

  window.ducksiteSetInput = (name, value) => {
    console.debug("[ducksite] ducksiteSetInput:", name, "=", value);
    inputs[name] = value;
    syncInputsToUrl(inputs, inputDefs);
    window.dispatchEvent(
      new CustomEvent("ducksiteInputsChanged", { detail: { inputs } }),
    );
  };

  syncInputsToUrl(inputs, inputDefs);
}

function syncInputsToUrl(inputs, inputDefs) {
  const sp = new URLSearchParams();

  for (const [name, def] of Object.entries(inputDefs)) {
    const key = def.url_key || def["url-key"] || name;
    const value = inputs[name];
    const multiple = isMultiple(def);

    const empty =
      value === undefined ||
      value === null ||
      value === "" ||
      (Array.isArray(value) && value.length === 0);

    const sameAsDefault = value === def.default;

    if (!empty && !sameAsDefault) {
      if (multiple && Array.isArray(value)) {
        sp.set(key, value.join(","));
      } else {
        sp.set(key, String(value));
      }
    }
  }

  const base = window.location.pathname;
  const qs = sp.toString();
  const newUrl = qs ? `${base}?${qs}` : base;
  console.debug("[ducksite] syncInputsToUrl: new URL", newUrl, "inputs", inputs);
  window.history.replaceState(null, "", newUrl);
}
