const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function workbench() {
  const elements = [];
  const requests = [];
  const toasts = [];
  let reviewed = new Set();

  function makeElement(id = '') {
    let html = '';
    const element = {
      _id: id, value: '', disabled: false, hidden: false, scrollTop: 0, writes: 0,
      dataset: {}, listeners: {}, attrs: {}, classList: { toggle() {}, remove() {}, add() {} },
      addEventListener(name, callback) { this.listeners[name] = callback; },
      setAttribute(name, value) { this.attrs[name] = value; },
      querySelector() { return null; },
      querySelectorAll(selector) {
        if (selector === '[data-decision="correct"]') return elements.filter(item => item.dataset.decision === 'correct');
        if (selector === '[data-split-sequence]') return elements.filter(item => item.dataset.splitSequence);
        return [];
      },
      click() { if (this.listeners.click) return this.listeners.click({ target: this }); },
    };
    Object.defineProperty(element, 'id', {
      get() { return this._id; },
      set(value) { this._id = value; },
    });
    Object.defineProperty(element, 'innerHTML', {
      get() { return html; },
      set(value) {
        html = value;
        this.writes++;
        if (this.id === 'suggestion-detail') {
          const intent=value.match(/<select id="suggestion-intent">([\s\S]*?)<\/select>/);
          const selected=intent&&intent[1].match(/<option value="([^"]+)" selected/);
          if(selected)getElementById('suggestion-intent').value=selected[1];
          const tags = value.match(/<button\b[^>]*>/g) || [];
          for (const tag of tags) {
            const attrs = Object.fromEntries([...tag.matchAll(/([\w-]+)="([^"]*)"/g)].map(m => [m[1], m[2]]));
            const button = makeElement(attrs.id || '');
            if (attrs['data-decision']) button.dataset.decision = attrs['data-decision'];
            if (attrs['data-split-sequence']) button.dataset.splitSequence = attrs['data-split-sequence'];
          }
        }
      },
    });
    elements.push(element);
    return element;
  }

  const byId = new Map();
  function getElementById(id) {
    const found = [...elements].reverse().find(item => item.id === id);
    if (found) return found;
    const created = makeElement(id);
    byId.set(id, created);
    return created;
  }

  const first = suggestion('s1', 'case1', '问题一');
  const second = suggestion('s2', 'case2', '问题二');
  const context = {
    URL, URLSearchParams, AbortController, console,
    localStorage: { getItem() { return null; } }, toasts,
    window: { setTimeout() { return 1; }, clearTimeout() {} },
    document: { getElementById, querySelector() { return null; }, querySelectorAll() { return []; } },
    fetch: async (url, options = {}) => {
      const request = { url, options };
      requests.push(request);
      if (options.method === 'POST') {
        const payload = JSON.parse(options.body || '{}');
        reviewed.add(payload.suggestion_id);
        return { ok: true, json: async () => ({ ok: true, review: { decision: payload.decision } }) };
      }
      return { ok: true, json: async () => ({ items: [first, second].filter(item => !reviewed.has(item.id)), has_more: false, next_before: null }) };
    },
  };
  function bootstrap() {
    toast = function (title, detail) { toasts.push({ title, detail }); };
    workflowJobMonitoring = true;
    globalThis.audit = {
      load: loadSuggestions,
      accept: function () { return document.getElementById('suggestion-accept').click(); },
      saveCorrected: saveSuggestionReview,
      selected: function () { return selectedSuggestionId; },
      items: function () { return suggestionItems.map(item => item.id); },
    };
  }
  const source = fs.readFileSync(path.join(__dirname, 'app.js'), 'utf8');
  const cut = source.lastIndexOf('\n  bindEvents();');
  assert.ok(cut > 0);
  vm.createContext(context);
  vm.runInContext(source.slice(0, cut) + '(' + bootstrap.toString() + ')();\n})();', context);
  return { ...context.audit, context, requests, elements, toasts, getElementById };
}

function suggestion(id, caseId, content) {
  return {
    id, case_id: caseId, case_state: 'open', state: 'pending', created_at: '2026-10-26T00:00:00Z',
    source_account_id: 'account1', group_id: 'group1', sender_id: 'user1', intent: 'qa', assignee: 'group_human', message_count: 1,
    payload: { intent: 'qa', assignee: 'group_human', response_needed: true, reply_draft: '', messages: [{ sequence: 1, content }], message_sequences: [1] },
  };
}

test('correct suggestion is saved locally, removed from todo, and the next item is selected', async () => {
  const app = workbench();
  await app.load(true);
  assert.equal(app.selected(), 's1');
  app.accept();
  for (let attempt = 0; attempt < 20 && app.selected() !== 's2'; attempt++) {
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.deepEqual(app.requests.filter(request => request.options.method === 'POST').map(request => JSON.parse(request.options.body)), [
    { suggestion_id: 's1', decision: 'correct', corrections: {intent:'qa',faq_verdicts:{},faq_coverage:'not_applicable',faq_comparison_status:'unknown',gold_reply_sequences:[]}, reason: '' },
  ]);
  assert.deepEqual(app.items(), ['s2']);
  assert.equal(app.selected(), 's2');
  assert.ok(app.requests.some(request => new URL(request.url, 'http://localhost').searchParams.get('status') === 'todo'));
});

test('corrected suggestion requires a reason and persists only after the reason is supplied', async () => {
  const app = workbench();
  await app.load(true);
  await app.saveCorrected('corrected');
  assert.equal(app.requests.filter(request => request.options.method === 'POST').length, 0);
  assert.ok(app.toasts.some(item => item.title === '需要填写修正原因'));
  app.getElementById('suggestion-reason').value = '承办角色判断错了';
  app.getElementById('suggestion-intent').value = 'service';
  app.getElementById('suggestion-assignee').value = 'aftercare';
  app.getElementById('suggestion-response').value = 'true';
  await app.saveCorrected('corrected');
  assert.equal(app.requests.filter(request => request.options.method === 'POST').length, 1);
  const saved = JSON.parse(app.requests.find(request => request.options.method === 'POST').options.body);
  assert.equal(saved.decision, 'corrected');
  assert.equal(saved.reason, '承办角色判断错了');
  assert.equal(saved.corrections.intent, 'service');
  assert.equal(saved.corrections.faq_coverage, 'not_applicable');
});
