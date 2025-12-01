// ducksite/static_src/main.js
// Loaded by <script type="module" src="/main.js">.

import { initPage } from "./page_runtime.js";

window.addEventListener("load", () => {
  console.debug("[ducksite] window load: starting initPage");
  initPage().catch((err) => {
    console.error("Error in initPage:", err);
  });
});
