import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

let executeQueryMock;
let setOptionSpy;

beforeEach(async () => {
  vi.resetModules();

  const { window } = new JSDOM('<div class="viz" data-viz="radar_chart"></div>', {
    url: 'https://example.com/reports/radar.html',
  });

  global.window = window;
  global.document = window.document;
  global.localStorage = window.localStorage;

  setOptionSpy = vi.fn();
  const resizeSpy = vi.fn();
  global.window.echarts = {
    init: vi.fn(() => ({ setOption: setOptionSpy, resize: resizeSpy })),
  };

  global.fetch = vi.fn(async () => ({ ok: true, text: async () => 'select 1;' }));

  executeQueryMock = vi.fn(async () => [
    { metric: 'Speed', score: 80 },
    { metric: 'Power', score: 65 },
  ]);

  vi.mock('../../ducksite/static_src/duckdb_runtime.js', () => ({
    initDuckDB: vi.fn().mockResolvedValue({ conn: {} }),
    executeQuery: (...args) => executeQueryMock(...args),
  }));
});

describe('radar charts', () => {
  it('sets tooltip trigger to item', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        radar_chart: {
          data_query: 'radar_query',
          type: 'radar',
          indicator: 'metric',
          value: 'score',
        },
      },
      grids: [
        {
          cols: 1,
          gap: 'md',
          rows: [[{ id: 'radar_chart', span: 1 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(setOptionSpy).toHaveBeenCalledTimes(1);
    const option = setOptionSpy.mock.calls[0][0];
    expect(option.tooltip).toMatchObject({ trigger: 'item' });
  });

  it('offsets radar center when a title is present', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        radar_chart: {
          data_query: 'radar_query',
          type: 'radar',
          indicator: 'metric',
          value: 'score',
          title: 'Performance Overview',
        },
      },
      grids: [
        {
          cols: 1,
          gap: 'md',
          rows: [[{ id: 'radar_chart', span: 1 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(setOptionSpy).toHaveBeenCalledTimes(1);
    const option = setOptionSpy.mock.calls[0][0];
    expect(option.radar).toMatchObject({ center: ['50%', '55%'], radius: '70%' });
  });
});
