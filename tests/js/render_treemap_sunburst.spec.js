import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

let executeQueryMock;
let setOptionSpy;
let lastQueryId;

beforeEach(async () => {
  vi.resetModules();

  const { window } = new JSDOM('<div class="viz" data-viz="treemap_demo"></div><div class="viz" data-viz="sunburst_demo"></div>', {
    url: 'https://example.com/gallery/index.html',
  });

  global.window = window;
  global.document = window.document;
  global.localStorage = window.localStorage;

  setOptionSpy = vi.fn();
  const resizeSpy = vi.fn();
  global.window.echarts = {
    init: vi.fn(() => ({ setOption: setOptionSpy, resize: resizeSpy })),
  };

  lastQueryId = null;

  executeQueryMock = vi.fn(async (conn, sql) => {
    if (lastQueryId === 'q_treemap') {
      return [
        { label: 'Alpha', size: 12 },
        { label: 'Beta', size: 5 },
      ];
    }
    if (lastQueryId === 'q_sunburst') {
      return [
        { segment: 'North', value: 7 },
        { segment: 'South', value: 3 },
      ];
    }
    return [];
  });

  global.fetch = vi.fn(async (url) => {
    const match = String(url).match(/\/([^/]+)\.sql$/);
    lastQueryId = match ? match[1] : null;
    return { ok: true, text: async () => 'select 1' };
  });

  vi.mock('../../ducksite/static_src/duckdb_runtime.js', () => ({
    initDuckDB: vi.fn().mockResolvedValue({ conn: {} }),
    executeQuery: (...args) => executeQueryMock(...args),
  }));
});

describe('treemap and sunburst charts', () => {
  it('renders treemap options from flat rows', async () => {
    document.body.innerHTML = '<div class="viz" data-viz="treemap_demo"></div>';

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        treemap_demo: {
          data_query: 'q_treemap',
          type: 'treemap',
          name: 'label',
          value: 'size',
          title: 'Treemap totals',
        },
      },
      grids: [
        { cols: 12, gap: 'md', rows: [[{ id: 'treemap_demo', span: 12 }]] },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(setOptionSpy).toHaveBeenCalledTimes(1);
    const option = setOptionSpy.mock.calls[0][0];
    const series = option.series && option.series[0];
    expect(series.type).toBe('treemap');
    expect(series.data).toEqual([
      { name: 'Alpha', value: 12 },
      { name: 'Beta', value: 5 },
    ]);
  });

  it('renders sunburst options from rows', async () => {
    document.body.innerHTML = '<div class="viz" data-viz="sunburst_demo"></div>';

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        sunburst_demo: {
          data_query: 'q_sunburst',
          type: 'sunburst',
          name: 'segment',
          value: 'value',
          title: 'Sunburst totals',
        },
      },
      grids: [
        { cols: 12, gap: 'md', rows: [[{ id: 'sunburst_demo', span: 12 }]] },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(setOptionSpy).toHaveBeenCalledTimes(1);
    const option = setOptionSpy.mock.calls[0][0];
    const series = option.series && option.series[0];
    expect(series.type).toBe('sunburst');
    expect(series.data).toEqual([
      { name: 'North', value: 7 },
      { name: 'South', value: 3 },
    ]);
  });
});
