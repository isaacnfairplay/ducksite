import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { JSDOM } from 'jsdom';
import { writeFile, rm } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const contractPath = path.resolve(__dirname, '../../ducksite/static_src/ducksite_contract.js');

let buildParamsFromInputs;
let substituteParams;
let rewriteParquetPathsHttp;

beforeAll(async () => {
  await writeFile(contractPath, `export const CLASS = { vizContainer: 'viz', tableContainer: 'table' };
export const DATA = { vizId: 'data-viz', tableId: 'data-table' };
export const PATH = { sqlRoot: '/sql' };
`, 'utf-8');

  const mod = await import('../../ducksite/static_src/render.js');
  ({ buildParamsFromInputs, substituteParams, rewriteParquetPathsHttp } = mod);
});

afterAll(async () => {
  await rm(contractPath, { force: true });
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
});
