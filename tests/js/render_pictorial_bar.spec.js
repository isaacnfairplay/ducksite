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

  executeQueryMock = vi.fn(async () => [
    { label: 'A', height: 5 },
    { label: 'B', height: 3 },
  ]);

  global.fetch = vi.fn(async () => ({ ok: true, text: async () => 'select 1' }));

  vi.mock('../../ducksite/static_src/duckdb_runtime.js', () => ({
    initDuckDB: vi.fn().mockResolvedValue({ conn: {} }),
    executeQuery: (...args) => executeQueryMock(...args),
  }));
});

describe('pictorial bar rendering', () => {
  it('uses data-driven symbol repeat mode', async () => {
    document.body.innerHTML = '<div class="viz" data-viz="pictorial_demo"></div>';

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        pictorial_demo: {
          data_query: 'q_pictorial',
          type: 'pictorialBar',
          x: 'label',
          y: 'height',
          title: 'Pictorial repeat',
        },
      },
      grids: [
        { cols: 12, gap: 'md', rows: [[{ id: 'pictorial_demo', span: 12 }]] },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(setOptionSpy).toHaveBeenCalledTimes(1);
    const option = setOptionSpy.mock.calls[0][0];
    const series = option.series && option.series[0];
    expect(series.type).toBe('pictorialBar');
    expect(series.symbolRepeat).toBe(true);
  });
});

