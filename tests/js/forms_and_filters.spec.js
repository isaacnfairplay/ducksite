import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

beforeEach(() => {
  vi.restoreAllMocks();
});

it('keeps form submit buttons when filters render', async () => {
  const { window } = new JSDOM('<div class="ducksite-nav"></div>', {
    url: 'https://example.com/forms/index.html',
  });
  global.window = window;
  global.document = window.document;
  global.localStorage = window.localStorage;
  global.alert = vi.fn();

  const pageConfig = {
    forms: [
      {
        id: 'feedback_form',
        label: 'Feedback',
        inputs: ['feedback_text'],
        target_csv: 'forms/feedback.csv',
        sql_relation_query: 'select 1 as x',
      },
    ],
    inputs: {
      category_filter: {
        visual_mode: 'dropdown',
        expression_template: 'category = ?',
        all_label: 'ALL',
        all_expression: 'TRUE',
      },
    },
    visualizations: {},
    grids: [],
  };

  const el = document.createElement('script');
  el.id = 'page-config-json';
  el.type = 'application/json';
  el.textContent = JSON.stringify(pageConfig);
  document.body.appendChild(el);

  const { initFormsUI } = await import('../../ducksite/static_src/forms.js');
  const { initInputsUI } = await import('../../ducksite/static_src/render.js');

  initFormsUI({});
  const runQuery = vi.fn().mockResolvedValue([]);
  await initInputsUI(pageConfig.inputs, {}, runQuery);

  const bar = document.querySelector('.ducksite-input-bar');
  expect(bar).not.toBeNull();

  const submitButtons = Array.from(bar.querySelectorAll('button')).filter((b) =>
    (b.textContent || '').includes('Submit'),
  );
  expect(submitButtons.length).toBeGreaterThan(0);

  const filterControls = bar.querySelectorAll('select, input[type="text"]');
  expect(filterControls.length).toBeGreaterThan(0);
});

