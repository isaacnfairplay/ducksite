import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

let executeQueryMock;
let setOptionSpy;

beforeEach(async () => {
  vi.resetModules();

  const { window } = new JSDOM('<div id="root"></div>', {
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

  executeQueryMock = vi.fn(async (conn, sql) => {
    if (sql.includes('q_pie')) {
      return [
        { category: 'A', total_value: 10 },
        { category: 'B', total_value: 5 },
      ];
    }
    if (sql.includes('q_doughnut')) {
      return [
        { category: 'A', total_value: 7 },
        { category: 'B', total_value: 3 },
      ];
    }
    if (sql.includes('q_sankey')) {
      return [
        { src: 'A', dst: 'X', weight: 5 },
        { src: 'A', dst: 'Y', weight: 3 },
        { src: 'B', dst: 'X', weight: 4 },
        { src: 'B', dst: 'Z', weight: 2 },
      ];
    }
    return [];
  });

  global.fetch = vi.fn(async (url) => {
    let text = 'select 1';
    if (String(url).includes('q_pie')) {
      text = "select 'A' as category, 10 as total_value";
    }
    if (String(url).includes('q_doughnut')) {
      text = "select 'A' as category, 7 as total_value";
    }
    if (String(url).includes('q_sankey')) {
      text = "select 'A' as src, 'X' as dst, 5 as weight";
    }
    return { ok: true, text: async () => text };
  });

  vi.mock('../../ducksite/static_src/duckdb_runtime.js', () => ({
    initDuckDB: vi.fn().mockResolvedValue({ conn: {} }),
    executeQuery: (...args) => executeQueryMock(...args),
  }));
});

describe('layout for pie and sankey charts', () => {
  it('positions legend away from title for pie charts', async () => {
    document.body.innerHTML = '<div class="viz" data-viz="pie_demo"></div>';

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        pie_demo: {
          data_query: 'q_pie',
          type: 'pie',
          name: 'category',
          value: 'total_value',
          title: 'Pie: share by category',
        },
      },
      grids: [
        {
          cols: 12,
          gap: 'md',
          rows: [[{ id: 'pie_demo', span: 12 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(setOptionSpy).toHaveBeenCalledTimes(1);
    const option = setOptionSpy.mock.calls[0][0];
    expect(option.title).toBeDefined();
    expect(option.title.text).toBe('Pie: share by category');
    expect(option.legend).toBeDefined();

    const titleTop = option.title.top;
    const legendTop = option.legend.top;

    expect(titleTop).toBeDefined();
    expect(legendTop).toBeDefined();

    if (legendTop === 'bottom') {
      expect(legendTop).toBe('bottom');
    } else {
      expect(legendTop).not.toEqual(titleTop);
      if (typeof titleTop === 'number' && typeof legendTop === 'number') {
        expect(legendTop).toBeGreaterThan(titleTop + 8);
      }
    }
  });

  it('applies separated legend placement for doughnut charts', async () => {
    document.body.innerHTML = '<div class="viz" data-viz="doughnut_demo"></div>';

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        doughnut_demo: {
          data_query: 'q_doughnut',
          type: 'pie',
          name: 'category',
          value: 'total_value',
          title: 'Doughnut layout separation',
          inner_radius: '40%',
        },
      },
      grids: [
        {
          cols: 12,
          gap: 'md',
          rows: [[{ id: 'doughnut_demo', span: 12 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    const option = setOptionSpy.mock.calls[0][0];
    expect(option.title.top).toBeDefined();
    expect(option.legend.top).toBeDefined();
    expect(option.legend.top).not.toEqual(option.title.top);
    if (typeof option.legend.top === 'number' && typeof option.title.top === 'number') {
      expect(option.legend.top).toBeGreaterThan(option.title.top + 8);
    }
  });

  it('ensures sankey links are visible on dark theme', async () => {
    document.body.innerHTML = '<div class="viz" data-viz="sankey_demo"></div>';

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        sankey_demo: {
          data_query: 'q_sankey',
          type: 'sankey',
          source: 'src',
          target: 'dst',
          value: 'weight',
          title: 'Sankey: simple source→target flows',
        },
      },
      grids: [
        { cols: 12, gap: 'md', rows: [[{ id: 'sankey_demo', span: 12 }]] },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    const option = setOptionSpy.mock.calls[0][0];
    const series = option.series && option.series[0];
    expect(series.type).toBe('sankey');
    expect(series.lineStyle).toBeDefined();
    expect(typeof series.lineStyle.opacity).toBe('number');
    expect(series.lineStyle.opacity).toBeGreaterThanOrEqual(0.3);
    expect(series.lineStyle.opacity).toBeLessThanOrEqual(1);
    expect(series.lineStyle.color).toBeTruthy();
    expect(option.title.top).toBeGreaterThanOrEqual(10);
    expect(series.top === undefined || series.top === null ? 'na' : series.top).not.toBe(0);
  });

  it('avoids arbitrary percentage offset when sankey title is absent', async () => {
    document.body.innerHTML = '<div class="viz" data-viz="sankey_no_title"></div>';

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        sankey_no_title: {
          data_query: 'q_sankey',
          type: 'sankey',
          source: 'src',
          target: 'dst',
          value: 'weight',
        },
      },
      grids: [
        { cols: 12, gap: 'md', rows: [[{ id: 'sankey_no_title', span: 12 }]] },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    const option = setOptionSpy.mock.calls[0][0];
    const series = option.series && option.series[0];
    expect(option.title).toBeUndefined();
    expect(series.type).toBe('sankey');
    expect(series.top).toBe(0);
  });
});
