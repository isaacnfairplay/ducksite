import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import { writeFile, rm } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const contractPath = path.resolve(__dirname, '../../ducksite/static_src/ducksite_contract.js');

beforeEach(() => {
  return writeFile(contractPath, "export const ID = { pageConfigJson: 'page-config-json' };\n", 'utf-8');
});

beforeEach(() => {
  const { window } = new JSDOM('<div class="ducksite-input-bar"></div>', { url: 'https://example.com' });
  global.window = window;
  global.document = window.document;
  global.localStorage = window.localStorage;
  global.alert = vi.fn();
});

afterEach(() => rm(contractPath, { force: true }));

it('disables submit when auth required and no email', async () => {
  const { initFormsUI } = await import('../../ducksite/static_src/forms.js');
  window.localStorage.clear();
  window.ducksiteGetInputs = () => ({ foo: 'bar' });
  const cfg = { forms: [{ id: 'f1', label: 'F1', inputs: ['foo'], target_csv: 'x', sql_relation_query: 'select 1', auth_required: true }] };
  const el = document.createElement('script');
  el.id = 'page-config-json';
  el.type = 'application/json';
  el.textContent = JSON.stringify(cfg);
  document.body.appendChild(el);
  initFormsUI({});
  const btn = document.querySelector('button');
  await btn.onclick();
  expect(global.alert).toHaveBeenCalled();
});

it('sends fetch with inputs and identity', async () => {
  const { initFormsUI } = await import('../../ducksite/static_src/forms.js');
  window.localStorage.clear();
  window.localStorage.setItem('ducksite_user_email', 'u@example.com');
  window.ducksiteGetInputs = () => ({ foo: 'bar' });
  const cfg = { forms: [{ id: 'f2', label: 'F2', inputs: ['foo'], target_csv: 'x', sql_relation_query: 'select 1' }] };
  const el = document.createElement('script');
  el.id = 'page-config-json';
  el.type = 'application/json';
  el.textContent = JSON.stringify(cfg);
  document.body.appendChild(el);
  const fetchSpy = vi.fn().mockResolvedValue({ json: () => Promise.resolve({ status: 'ok' }) });
  global.fetch = fetchSpy;
  initFormsUI({});
  const btn = document.querySelector('button');
  await btn.onclick();
  expect(fetchSpy).toHaveBeenCalled();
  const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
  expect(body.inputs._user_email).toBe('u@example.com');
});
