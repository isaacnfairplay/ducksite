import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

vi.mock('../../ducksite/static_src/ducksite_contract.js', () => ({
  ID: { pageConfigJson: 'page-config-json' },
}));

beforeEach(() => {
  const { window } = new JSDOM('<div class="ducksite-input-bar"></div>', { url: 'https://example.com' });
  global.window = window;
  global.document = window.document;
  global.localStorage = window.localStorage;
  global.alert = window.alert = vi.fn();
  global.confirm = window.confirm = vi.fn();
  global.prompt = window.prompt = vi.fn();
});

afterEach(() => vi.restoreAllMocks());

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
  const btn = Array.from(document.querySelectorAll('button')).find((b) => b.textContent === 'Submit');
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
  const btn = Array.from(document.querySelectorAll('button')).find((b) => b.textContent === 'Submit');
  await btn.onclick();
  expect(fetchSpy).toHaveBeenCalled();
  const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
  expect(body.inputs._user_email).toBe('u@example.com');
});

it('prompts to confirm first password and marks password as set on success', async () => {
  const { initFormsUI } = await import('../../ducksite/static_src/forms.js');
  window.localStorage.clear();
  window.ducksiteGetInputs = () => ({ foo: 'bar' });

  const cfg = {
    forms: [{
      id: 'f1',
      label: 'F1',
      inputs: ['foo'],
      target_csv: 'x',
      sql_relation_query: 'select 1',
      auth_required: true,
    }],
  };
  const el = document.createElement('script');
  el.id = 'page-config-json';
  el.type = 'application/json';
  el.textContent = JSON.stringify(cfg);
  document.body.appendChild(el);

  const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);
  const fetchSpy = vi.fn().mockResolvedValue({
    json: () => Promise.resolve({ status: 'ok', auth_status: 'set' }),
  });
  global.fetch = fetchSpy;

  initFormsUI({});
  const [emailInput, passwordInput] = document.querySelectorAll('input');
  emailInput.value = 'u@example.com';
  passwordInput.value = 'secret123';
  window.localStorage.setItem('ducksite_user_email', 'u@example.com');
  window.localStorage.setItem('ducksite_user_password', 'secret123');

  const btn = Array.from(document.querySelectorAll('button')).find((b) => b.textContent === 'Submit');
  await btn.onclick();

  expect(confirmSpy).toHaveBeenCalled();
  expect(fetchSpy).toHaveBeenCalled();
  const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
  expect(body.inputs._user_password).toBe('secret123');
});

it('alerts on unauthorized error', async () => {
  const { initFormsUI } = await import('../../ducksite/static_src/forms.js');
  window.localStorage.clear();
  window.ducksiteGetInputs = () => ({ foo: 'bar' });

  const cfg = {
    forms: [{
      id: 'f2',
      label: 'F2',
      inputs: ['foo'],
      target_csv: 'x',
      sql_relation_query: 'select 1',
      auth_required: true,
    }],
  };
  const el = document.createElement('script');
  el.id = 'page-config-json';
  el.type = 'application/json';
  el.textContent = JSON.stringify(cfg);
  document.body.appendChild(el);

  const fetchSpy = vi.fn().mockResolvedValue({
    json: () => Promise.resolve({ error: 'unauthorized' }),
  });
  global.fetch = fetchSpy;
  const alertSpy = vi.spyOn(global, 'alert');
  vi.spyOn(window, 'confirm').mockReturnValue(true);

  initFormsUI({});
  const [emailInput, passwordInput] = document.querySelectorAll('input');
  emailInput.value = 'u@example.com';
  passwordInput.value = 'wrong';
  window.localStorage.setItem('ducksite_user_email', 'u@example.com');
  window.localStorage.setItem('ducksite_user_password', 'wrong');

  const btn = Array.from(document.querySelectorAll('button')).find((b) => b.textContent === 'Submit');
  await btn.onclick();

  expect(alertSpy).toHaveBeenCalled();
  expect(alertSpy.mock.calls[0][0].toLowerCase()).toContain('unauthorized');
});

it('sends update password request and handles success', async () => {
  const { initFormsUI } = await import('../../ducksite/static_src/forms.js');
  window.localStorage.clear();
  window.ducksiteGetInputs = () => ({ foo: 'bar' });

  const cfg = {
    forms: [{
      id: 'f3',
      label: 'F3',
      inputs: ['foo'],
      target_csv: 'x',
      sql_relation_query: 'select 1',
      auth_required: true,
    }],
  };
  const el = document.createElement('script');
  el.id = 'page-config-json';
  el.type = 'application/json';
  el.textContent = JSON.stringify(cfg);
  document.body.appendChild(el);

  const prompts = ['u@example.com', 'oldpass', 'newpass123', 'newpass123'];
  const promptSpy = vi.spyOn(window, 'prompt').mockImplementation(() => prompts.shift());
  const fetchSpy = vi.fn().mockResolvedValue({ json: () => Promise.resolve({ status: 'ok' }) });
  global.fetch = fetchSpy;
  const alertSpy = vi.spyOn(global, 'alert');

  initFormsUI({});
  const updateBtn = Array.from(document.querySelectorAll('button')).find((b) => b.textContent === 'Change password');
  await updateBtn.onclick();

  expect(promptSpy).toHaveBeenCalled();
  const call = fetchSpy.mock.calls.find((c) => c[0] === '/api/auth/update_password');
  expect(call).toBeTruthy();
  const body = JSON.parse(call[1].body);
  expect(body).toEqual({ email: 'u@example.com', old_password: 'oldpass', new_password: 'newpass123' });
  expect(alertSpy).toHaveBeenCalledWith('Password updated.');
});
