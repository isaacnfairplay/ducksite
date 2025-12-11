import { beforeEach, describe, expect, it, vi } from 'vitest';
import { JSDOM } from 'jsdom';

let fetchSpy;
let executeQueryMock;
let setOptionSpy;

beforeEach(() => {
  vi.resetModules();

  const { window } = new JSDOM('<div class="viz" data-viz="chart1"></div>', {
    url: 'https://example.com/scrap/investigations/',
  });

  global.window = window;
  global.document = window.document;
  global.localStorage = window.localStorage;

  setOptionSpy = vi.fn();
  const resizeSpy = vi.fn();
  const chartInstance = { setOption: setOptionSpy, resize: resizeSpy };
  global.window.echarts = { init: vi.fn(() => chartInstance) };

  fetchSpy = vi.fn(() => {
    throw new Error('fetch should not be called for missing queries');
  });
  global.fetch = fetchSpy;

  executeQueryMock = vi.fn();
  vi.mock('../../ducksite/static_src/duckdb_runtime.js', () => ({
    initDuckDB: vi.fn().mockResolvedValue({ conn: {} }),
    executeQuery: (...args) => executeQueryMock(...args),
  }));
});

describe('renderAll query lookup', () => {
  it('skips fetching missing page queries when a manifest is present', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      queries: { known_query: { sql_id: 'known_query' } },
      visualizations: {
        chart1: { data_query: 'missing_query', type: 'bar', x: 'category', y: 'value', title: 'T' },
      },
      grids: [
        {
          cols: 1,
          gap: 'md',
          rows: [[{ id: 'chart1', span: 1 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(executeQueryMock).not.toHaveBeenCalled();
    expect(setOptionSpy).toHaveBeenCalled();
  });

  it('ignores grid cells that correspond to inputs', async () => {
    fetchSpy.mockResolvedValue({ ok: true, status: 200, text: async () => 'select 1 as category, 2 as value' });
    executeQueryMock.mockResolvedValue([]);

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      inputs: {
        line_filter_prod: { visual_mode: 'dropdown' },
      },
      queries: { known_query: { sql_id: 'known_query' } },
      visualizations: {
        chart1: { data_query: 'known_query', type: 'bar', x: 'category', y: 'value', title: 'T' },
      },
      grids: [
        {
          cols: 2,
          gap: 'md',
          rows: [[{ id: 'line_filter_prod', span: 1 }, { id: 'chart1', span: 1 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(executeQueryMock).toHaveBeenCalledTimes(1);
    expect(setOptionSpy).toHaveBeenCalled();
  });
});
