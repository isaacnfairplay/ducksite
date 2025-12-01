// ducksite/static_src/inputs.js
// Verbose logging for URL <-> inputs mapping.

export function initInputsFromUrl(inputDefs) {
  const inputs = {};
  const sp = new URLSearchParams(window.location.search);

  console.debug("[ducksite] initInputsFromUrl: defs", inputDefs);
  console.debug("[ducksite] initInputsFromUrl: URLSearchParams", Object.fromEntries(sp.entries()));

  for (const [name, def] of Object.entries(inputDefs)) {
    const key = def.url_key || def["url-key"] || name;
    const raw = sp.get(key);
    let value = raw;
    if (raw === null || raw === "") {
      value = def.default ?? null;
    }
    inputs[name] = value;
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

    const empty =
      value === undefined ||
      value === null ||
      value === "" ||
      (Array.isArray(value) && value.length === 0);

    const sameAsDefault = value === def.default;

    if (!empty && !sameAsDefault) {
      sp.set(key, String(value));
    }
  }

  const base = window.location.pathname;
  const qs = sp.toString();
  const newUrl = qs ? `${base}?${qs}` : base;
  console.debug("[ducksite] syncInputsToUrl: new URL", newUrl, "inputs", inputs);
  window.history.replaceState(null, "", newUrl);
}
