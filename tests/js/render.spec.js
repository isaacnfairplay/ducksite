import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';

let buildParamsFromInputs;
let substituteParams;
let rewriteParquetPathsHttp;
let normalizeQueryId;
let getPageSqlBasePath;

beforeAll(async () => {
  const mod = await import('../../ducksite/static_src/render.js');
  ({
    buildParamsFromInputs,
    substituteParams,
    rewriteParquetPathsHttp,
    normalizeQueryId,
    getPageSqlBasePath,
  } = mod);
});

const { window } = new JSDOM('', { url: 'https://example.com/reports/index.html' });
global.window = window;
global.document = window.document;

describe('buildParamsFromInputs', () => {
  it('builds dropdown predicate and param template derived inputs', () => {
    const inputDefs = {
      category: {
        visual_mode: 'dropdown',
        expression_template: 'category = ?',
        all_label: 'ALL',
        all_expression: 'TRUE'
      },
      barcode: {
        visual_mode: 'text',
        param_name: 'barcode_prefix',
        param_template: 'left(?, 2)'
      }
    };
    const inputs = { category: 'Widgets', barcode: 'AB123' };
    const params = buildParamsFromInputs(inputDefs, inputs);
    expect(params.category).toBe("category = 'Widgets'");
    expect(params.barcode_prefix).toBe("left('AB123', 2)");
    expect(inputs.barcode_prefix).toBe('AB');
  });

  it('handles missing dropdown value as ALL expression', () => {
    const inputDefs = { status: { visual_mode: 'dropdown', expression_template: 'status = ?', all_label: 'ALL', all_expression: 'TRUE' } };
    const inputs = {};
    const params = buildParamsFromInputs(inputDefs, inputs);
    expect(params.status).toBe('TRUE');
  });
});

describe('substituteParams', () => {
  it('quotes inputs and leaves params raw', () => {
    const sql = 'select * from t where c = ${inputs.x} and ${params.pred}';
    const out = substituteParams(sql, { x: "O'Reilly" }, { pred: 'flag = TRUE' });
    expect(out).toContain("c = 'O''Reilly'");
    expect(out).toContain('flag = TRUE');
  });

  it('replaces missing values with NULL', () => {
    const sql = 'select ${inputs.maybe} as a, ${params.none} as b';
    const out = substituteParams(sql, {}, {});
    expect(out).toContain('NULL as a');
    expect(out).toContain('NULL as b');
  });
});

describe('rewriteParquetPathsHttp', () => {
  it('makes relative parquet paths absolute', () => {
    const sql = "select * from read_parquet(['data/sample.parquet'])";
    const out = rewriteParquetPathsHttp(sql);
    expect(out).toContain("read_parquet(['https://example.com/data/sample.parquet']");
  });

  it('keeps absolute and root paths unchanged', () => {
    const sql = "select * from read_parquet(['https://host/x.parquet', '/root/a.parquet', '//cdn/b.parquet'])";
    const out = rewriteParquetPathsHttp(sql);
    expect(out).toContain("'https://host/x.parquet'");
    expect(out).toContain("'/root/a.parquet'");
    expect(out).toContain("'//cdn/b.parquet'");
  });

  it('rewrites read_csv_auto relative paths to absolute HTTP URLs', () => {
    const sql = "select * from read_csv_auto('forms/feedback.csv', HEADER=TRUE)";
    const out = rewriteParquetPathsHttp(sql);
    expect(out).toContain(
      "read_csv_auto('https://example.com/forms/feedback.csv', HEADER=TRUE",
    );
  });

  it('does not change absolute or root CSV paths in read_csv_auto', () => {
    const sql =
      "select * from read_csv_auto('https://host/x.csv')" +
      " union all select * from read_csv_auto('/root/a.csv')" +
      " union all select * from read_csv_auto('//cdn/b.csv')";
    const out = rewriteParquetPathsHttp(sql);
    expect(out).toContain("read_csv_auto('https://host/x.csv'");
    expect(out).toContain("read_csv_auto('/root/a.csv'");
    expect(out).toContain("read_csv_auto('//cdn/b.csv'");
  });
});

describe('normalizeQueryId', () => {
  it('falls back demo_ to demo for global barcode demo when prefix empty', async () => {
    const { id, valid, basePath } = normalizeQueryId(
      'global:demo_${inputs.barcode_prefix}',
      {},
    );
    expect(valid).toBe(true);
    expect(id).toBe('demo');
    expect(basePath).toBe('/sql/_global/');
  });

  it('marks invalid ids like ".." as invalid', async () => {
    const { valid, id } = normalizeQueryId('..', {});
    expect(valid).toBe(false);
    expect(id).toBe('..');
  });

  it('passes through normal ids unchanged', async () => {
    const { valid, id, basePath } = normalizeQueryId('gallery_q1_totals', {});
    expect(valid).toBe(true);
    expect(id).toBe('gallery_q1_totals');
    expect(basePath).toBe('/sql/reports/');
  });
});

describe('getPageSqlBasePath', () => {
  it('keeps full directory depth for trailing slash URLs', () => {
    const original = window.location.href;
    window.history.pushState(
      {},
      '',
      'https://example.com/scrap/tests/cost_associations/',
    );

    expect(getPageSqlBasePath()).toBe('/sql/scrap/tests/cost_associations/');

    window.history.pushState({}, '', original);
  });
});
