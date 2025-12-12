import { beforeEach, describe, expect, it, vi } from 'vitest';
import { JSDOM } from 'jsdom';

let executeQueryMock;
let setOptionSpy;
let chartOnSpy;

function setupDom(html = '') {
  const { window } = new JSDOM(html || '<div class="viz" data-viz="sales"></div>', {
    url: 'https://example.com/dashboard',
  });
  global.window = window;
  global.document = window.document;
  global.CustomEvent = window.CustomEvent;
  global.localStorage = window.localStorage;
}

beforeEach(() => {
  vi.resetModules();
  setupDom('<div class="viz" data-viz="sales"></div><div class="table" data-table="regions"></div>');

  setOptionSpy = vi.fn();
  chartOnSpy = vi.fn();
  const resizeSpy = vi.fn();
  const chartInstance = { setOption: setOptionSpy, resize: resizeSpy, on: chartOnSpy };
  global.window.echarts = { init: vi.fn(() => chartInstance) };

  executeQueryMock = vi.fn();
  vi.mock('../../ducksite/static_src/duckdb_runtime.js', () => ({
    initDuckDB: vi.fn().mockResolvedValue({ conn: {} }),
    executeQuery: (...args) => executeQueryMock(...args),
  }));

  global.fetch = vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    text: async () => 'select 1 as category, 2 as value',
  });
});

describe('click-driven input updates', () => {
  it('adds and removes values for multi-select inputs on chart clicks with modifiers', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    let inputsState = { region: ['emea'] };
    window.ducksiteGetInputs = () => ({ ...inputsState });
    window.ducksiteSetInput = (name, value) => {
      inputsState = { ...inputsState, [name]: value };
    };

    executeQueryMock.mockResolvedValue([
      { category: 'emea', value: 1 },
      { category: 'na', value: 2 },
    ]);

    const pageConfig = {
      inputs: { region: { visual_mode: 'dropdown', multiple: true } },
      visualizations: {
        sales: { data_query: 'chart_sql', type: 'bar', x: 'category', y: 'value', click_input: 'region' },
      },
      grids: [
        { cols: 1, gap: 'md', rows: [[{ id: 'sales', span: 1 }]] },
      ],
      queries: { chart_sql: { sql_id: 'chart_sql' } },
    };

    await renderAll(pageConfig, inputsState, { conn: {} });

    const clickHandler = chartOnSpy.mock.calls.find(([eventName]) => eventName === 'click')[1];
    expect(clickHandler).toBeInstanceOf(Function);

    clickHandler({
      name: 'na',
      dataIndex: 1,
      event: { event: { ctrlKey: true, metaKey: false } },
    });
    expect(inputsState.region).toEqual(['emea', 'na']);

    clickHandler({
      name: 'na',
      dataIndex: 1,
      event: { event: { ctrlKey: true, metaKey: false } },
    });
    expect(inputsState.region).toEqual(['emea']);
  });

  it('replaces selections on chart clicks without modifiers', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    let inputsState = { region: ['emea', 'na'] };
    window.ducksiteGetInputs = () => ({ ...inputsState });
    window.ducksiteSetInput = (name, value) => {
      inputsState = { ...inputsState, [name]: value };
    };

    executeQueryMock.mockResolvedValue([
      { category: 'latam', value: 3 },
    ]);

    const pageConfig = {
      inputs: { region: { visual_mode: 'dropdown', multiple: true } },
      visualizations: {
        sales: { data_query: 'chart_sql', type: 'bar', x: 'category', y: 'value', click_input: 'region' },
      },
      grids: [
        { cols: 1, gap: 'md', rows: [[{ id: 'sales', span: 1 }]] },
      ],
      queries: { chart_sql: { sql_id: 'chart_sql' } },
    };

    await renderAll(pageConfig, inputsState, { conn: {} });

    const clickHandler = chartOnSpy.mock.calls.find(([eventName]) => eventName === 'click')[1];
    clickHandler({
      name: 'latam',
      dataIndex: 0,
      event: { event: { ctrlKey: false, metaKey: false } },
    });

    expect(inputsState.region).toEqual(['latam']);
  });

  it('clicks on table rows reuse the declared column to toggle membership', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    let inputsState = { region: ['emea'] };
    window.ducksiteGetInputs = () => ({ ...inputsState });
    const setter = vi.fn((name, value) => {
      inputsState = { ...inputsState, [name]: value };
    });
    window.ducksiteSetInput = setter;

    executeQueryMock.mockResolvedValue([
      { region: 'emea', total: 10 },
      { region: 'na', total: 20 },
    ]);

    const pageConfig = {
      inputs: { region: { visual_mode: 'dropdown', multiple: true } },
      tables: {
        regions: { query: 'table_sql', click_input: 'region', click_value: 'region' },
      },
      grids: [
        { cols: 1, gap: 'md', rows: [[{ id: 'regions', span: 1 }]] },
      ],
      queries: { table_sql: { sql_id: 'table_sql' } },
    };

    await renderAll(pageConfig, inputsState, { conn: {} });

    expect(inputsState.region).toEqual(['emea']);

    const cell = document.querySelector('.table tbody tr:last-child td:first-child');
    cell.dispatchEvent(new window.MouseEvent('click', { bubbles: true, ctrlKey: true }));

    expect(setter).toHaveBeenCalledWith('region', ['emea', 'na']);
    expect(inputsState.region).toEqual(['emea', 'na']);
  });

  it('routes clicks with action="anti" to the anti input for charts', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    let inputsState = { region: ['emea'], region_exclude: [] };
    window.ducksiteGetInputs = () => ({ ...inputsState });
    window.ducksiteSetInput = (name, value) => {
      inputsState = { ...inputsState, [name]: value };
    };

    executeQueryMock.mockResolvedValue([
      { category: 'emea', value: 1, action: 'select' },
      { category: 'na', value: 2, action: 'anti' },
    ]);

    const pageConfig = {
      inputs: {
        region: { visual_mode: 'dropdown', multiple: true },
        region_exclude: { visual_mode: 'dropdown', multiple: true },
      },
      visualizations: {
        sales: {
          data_query: 'chart_sql',
          type: 'bar',
          x: 'category',
          y: 'value',
          click_input: 'region',
          click_anti_input: 'region_exclude',
          click_action: 'action',
        },
      },
      grids: [
        { cols: 1, gap: 'md', rows: [[{ id: 'sales', span: 1 }]] },
      ],
      queries: { chart_sql: { sql_id: 'chart_sql' } },
    };

    await renderAll(pageConfig, inputsState, { conn: {} });

    const clickHandler = chartOnSpy.mock.calls.find(([eventName]) => eventName === 'click')[1];
    clickHandler({
      name: 'na',
      dataIndex: 1,
      event: { event: { ctrlKey: false, metaKey: false } },
    });

    expect(inputsState.region).toEqual(['emea']);
    expect(inputsState.region_exclude).toEqual(['na']);
  });

  it('routes table clicks with action="anti" to the anti input and value mapping', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    let inputsState = { region: ['emea'], region_exclude: [] };
    window.ducksiteGetInputs = () => ({ ...inputsState });
    const setter = vi.fn((name, value) => {
      inputsState = { ...inputsState, [name]: value };
    });
    window.ducksiteSetInput = setter;

    executeQueryMock.mockResolvedValue([
      { region: 'emea', total: 10, action: 'select' },
      { region: 'na', total: 20, action: 'anti' },
    ]);

    const pageConfig = {
      inputs: {
        region: { visual_mode: 'dropdown', multiple: true },
        region_exclude: { visual_mode: 'dropdown', multiple: true },
      },
      tables: {
        regions: {
          query: 'table_sql',
          click_input: 'region',
          click_anti_input: 'region_exclude',
          click_action: 'action',
          click_value: 'region',
        },
      },
      grids: [
        { cols: 1, gap: 'md', rows: [[{ id: 'regions', span: 1 }]] },
      ],
      queries: { table_sql: { sql_id: 'table_sql' } },
    };

    await renderAll(pageConfig, inputsState, { conn: {} });

    const cell = document.querySelector('.table tbody tr:last-child td:first-child');
    cell.dispatchEvent(new window.MouseEvent('click', { bubbles: true }));

    expect(setter).toHaveBeenCalledWith('region_exclude', ['na']);
    expect(inputsState.region_exclude).toEqual(['na']);
    expect(inputsState.region).toEqual(['emea']);
  });
});
