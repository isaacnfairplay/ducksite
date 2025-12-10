# Evidence.dev feature research

## What Evidence offers today
- Business intelligence as code: SQL inside markdown pages drives data sources, components, templated pages, loops, and conditional rendering to build full BI sites. Their docs highlight a workflow where markdown pages run queries, render charts/components, and can be templated for reuse.
- Component stack: ECharts for charts, Leaflet for maps, and Shadcn-based UI components. The catalog includes lightweight data displays (Value, Big Value, Data Table) plus many chart variants (line, area, bar, stacked/100%, grouped, horizontal) with screenshots and deep linking for each variant.
- Authoring patterns: Evidence’s pages emphasize configuring data sources, embedding SQL blocks in markdown, and using templated pages/loops to generate many pages from one definition. That code-first approach keeps reports reproducible and reviewable in version control.

## Gaps and self-hosted opportunities for Ducksite
- Guided data entry: Ship an action-item block (action, owner, due date, status, tags) that writes to CSV in a repo-adjacent data folder by default, warns when pointed at tracked paths, and auto-surfaces ECharts summaries (status counts, overdue timelines). Make it easy to opt into project-wide or dashboard-local lists.
- Incident workflows: Provide an incident report block (id, summary, severity, root cause, actions needed, owner, due date, status) with a freeform analysis field. Keep it offline-friendly but allow optional hooks for local automation that can prefill analysis. Let action items link back to incidents for follow-up tracking.
- Relationship visuals: Add a relationship/flow block that consumes CSV or inline tables (`source`, `target`, `value`, optional aliases) and renders ECharts Sankey/graph outputs, with a simple tree/text fallback when ECharts isn’t available.
- Manager-ready outputs: Offer a management-summary block that pulls KPIs from linked data (incidents, action items), shows concise callouts/trends, and can export charts via headless rendering when supported. Provide CSV download fallbacks for air-gapped setups.
- Data hygiene defaults: Default generated CSVs to an app data directory outside the repo, create folders automatically, add `.gitignore` entries when users opt into tracked paths, and surface UI warnings about sensitive data in version control.
- File-redirect updates: For assets served through file redirects, monitor the source directory via watchdog when available (polling fallback) and trigger recompiles only on filename changes; for dated hierarchy sources, reprocess on directory additions/removals to keep compiled data in sync without relying on mtimes or content diffs.

## Chart perception and design cues for Ducksite
- Match chart types to cognitive goals: prefer bars for categorical comparisons, lines for trends, stacked bars only when parts-to-whole ordering is clear, and limit slices in pies/donuts to keep ranking legible for managers.
- Reduce overload by default: cap color palettes to ~6 categorical hues, encourage small-multiple faceting over single dense charts, and default to sorted axes for faster scanning.
- Highlight “what changed” first: provide built-in deltas and slope charts for before/after views; default ECharts to show labels on hovered points only to cut clutter.
- Keep annotations close to data: add lightweight callouts and reference bands (targets, SLAs) so reports communicate actionability without extra text blocks.
- Offer trustworthy fallbacks: where ECharts is unavailable, render plain tables and mermaid-style diagrams so self-hosted users still see relationships; keep these fallbacks visually simple to prioritize comprehension.

## Additional feature and method ideas for Ducksite
- Low-friction data freshness: add a lightweight file-change queue that batches rapid filesystem events from watchdog/polling before triggering rebuilds to avoid thrashing on shared folders or network mounts.
- Schema hints for CSV inputs: let blocks declare optional schemas (types, required columns, friendly labels) and validate on write/read with concise error surfaces; fail softly to keep dashboards usable during fixes.
- Reusable theming: expose a small set of themes tuned for management readability (high contrast, large labels) and allow per-dashboard overrides without complex configuration.
- Inline data explainers: provide collapsible “how to read this chart” notes next to complex visuals (Sankey/flow), nudging authors to supply context while keeping pages clean by default.
