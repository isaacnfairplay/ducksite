import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';

import { initInputsFromUrl, createInputApi } from '../../ducksite/static_src/inputs.js';

describe('multi-select input plumbing', () => {
  beforeEach(() => {
    delete global.window;
    delete global.document;
    delete global.CustomEvent;
  });

  it('parses comma-separated URL params into arrays when multiple is enabled', () => {
    const { window } = new JSDOM('', { url: 'https://example.com/?region=emea,na' });
    global.window = window;
    global.document = window.document;
    global.CustomEvent = window.CustomEvent;

    const defs = {
      region: { visual_mode: 'dropdown', multiple: true },
    };

    const inputs = initInputsFromUrl(defs);
    expect(inputs.region).toEqual(['emea', 'na']);
  });

  it('syncs array selections back to the URL', () => {
    const { window } = new JSDOM('', { url: 'https://example.com/' });
    global.window = window;
    global.document = window.document;
    global.CustomEvent = window.CustomEvent;

    const defs = {
      region: { visual_mode: 'dropdown', multiple: true },
    };

    const inputs = initInputsFromUrl(defs);
    createInputApi(inputs, defs);

    window.ducksiteSetInput('region', ['emea', 'na']);
    expect(window.location.search).toBe('?region=emea%2Cna');
  });
});
