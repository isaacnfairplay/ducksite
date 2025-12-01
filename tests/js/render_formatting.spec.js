import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

let executeQueryMock;
let setOptionSpy;

beforeEach(async () => {
  vi.resetModules();

  const { window } = new JSDOM(
    '<div class="viz" data-viz="chart1"></div>' +
      '<div class="table" data-table="table1"></div>' +
      '<div class="table" data-table="demo_table"></div>',
    { url: 'https://example.com/reports/index.html' },
  );
  global.window = window;
  global.document = window.document;
  global.localStorage = window.localStorage;

  setOptionSpy = vi.fn();
  const resizeSpy = vi.fn();
  global.window.echarts = {
    init: vi.fn(() => ({ setOption: setOptionSpy, resize: resizeSpy })),
  };

  global.fetch = vi.fn(async (url) => {
    const isChart = String(url).includes('q_chart');
    const text = isChart ? 'select 1 as chart_select;' : 'select 1 as table_select;';
    return { ok: true, text: async () => text };
  });

  executeQueryMock = vi.fn(async (_conn, sql) => {
    if (sql.includes('chart_select')) {
      return [{ category: 'A', value: 5 }];
    }
    if (sql.includes('table_select')) {
      return [{ value: 'X' }];
    }
    return [];
  });

  vi.mock('../../ducksite/static_src/duckdb_runtime.js', () => ({
    initDuckDB: vi.fn().mockResolvedValue({ conn: {} }),
    executeQuery: (...args) => executeQueryMock(...args),
  }));
});

describe('renderAll without formatting', () => {
  it('renders charts and tables without relying on formatting columns', async () => {
    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        chart1: { data_query: 'q_chart', type: 'bar', x: 'category', y: 'value', title: 'T' },
      },
      grids: [
        {
          cols: 2,
          gap: 'md',
          rows: [[{ id: 'chart1', span: 1 }, { id: 'table1', span: 1 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    expect(executeQueryMock).toHaveBeenCalled();
    const executedSql = executeQueryMock.mock.calls.map((c) => c[1]).join('\n');
    expect(executedSql).toContain('chart_select');
    expect(executedSql).toContain('table_select');

    expect(setOptionSpy).toHaveBeenCalledTimes(1);
    const option = setOptionSpy.mock.calls[0][0];
    expect(option.series[0].data).toEqual([5]);

    const cell = document.querySelector('td');
    expect(cell.textContent).toBe('X');
  });
});

describe('renderAll with formatting metadata', () => {
  it('passes through formatting values for charts', async () => {
    executeQueryMock.mockImplementation(async () => [
      {
        category: 'A',
        total_value: 10,
        __fmt_chart_total_value_color: '#22c55e',
        __fmt_chart_total_value_highlight: false,
      },
      {
        category: 'B',
        total_value: 25,
        __fmt_chart_total_value_color: '#f97373',
        __fmt_chart_total_value_highlight: true,
      },
    ]);

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {
        chart1: {
          data_query: 'q_chart',
          type: 'bar',
          x: 'category',
          y: 'total_value',
          format: {
            total_value: {
              color_expr: "CASE WHEN total_value >= 20 THEN '#f97373' ELSE '#22c55e' END",
              highlight_expr: 'total_value >= 20',
            },
          },
        },
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

    const option = setOptionSpy.mock.calls[0][0];
    const data = option.series[0].data;
    expect(data[0]).toMatchObject({ value: 10, itemStyle: { color: '#22c55e' } });
    expect(data[1]).toMatchObject({ value: 25, itemStyle: { color: '#f97373' } });
    expect(data[1].emphasis || data[1].emphasis?.itemStyle).toBeDefined();
  });

  it('applies formatting metadata to tables', async () => {
    executeQueryMock.mockImplementation(async () => [
      {
        category: 'A',
        value: 5,
        other_metric: 1,
        __fmt_tbl_value_bg: '#eee',
        __fmt_tbl_value_fg: '#111',
        __fmt_tbl_value_hl: false,
        __fmt_tbl_category_bg: '#1d283a',
        __fmt_tbl_category_fg: '#38bdf8',
        __fmt_tbl_category_hl: true,
      },
    ]);

    const { renderAll } = await import('../../ducksite/static_src/render.js');

    const pageConfig = {
      visualizations: {},
      tables: {
        demo_table: {
          query: 'demo_rows',
          format: {
            value: {
              bg_color_expr: "CASE WHEN value >= 20 THEN '#f97373' ELSE NULL END",
              fg_color_expr: "CASE WHEN other_metric > 0 THEN '#0b1120' END",
              highlight_expr: 'other_metric > 10',
            },
            category: {
              bg_color_expr: "CASE WHEN category = 'A' THEN '#1d283a' END",
              fg_color_expr: "CASE WHEN category = 'A' THEN '#38bdf8' END",
            },
          },
        },
      },
      grids: [
        {
          cols: 1,
          gap: 'md',
          rows: [[{ id: 'demo_table', span: 1 }]],
        },
      ],
    };

    await renderAll(pageConfig, {}, { conn: {} });

    const cells = Array.from(document.querySelectorAll('td'));
    expect(cells[0].style.backgroundColor.toLowerCase()).toMatch(/1d283a|29, 40, 58/);
    expect(cells[0].style.color.toLowerCase()).toMatch(/38bdf8|56, 189, 248/);
    expect(cells[0].classList.contains('ducksite-cell-highlight')).toBe(true);
    expect(cells[1].style.backgroundColor.toLowerCase()).toMatch(/#eee|238, 238, 238/);
    expect(cells[1].style.color.toLowerCase()).toMatch(/#111|17, 17, 17/);
  });
});
