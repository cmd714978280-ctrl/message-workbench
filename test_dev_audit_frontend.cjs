const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

// Run the real audit navigation code without starting polling or changing live data.
function workbench() {
  const nodes = new Map();
  const timers = new Map();
  const requests = [];
  const rendered = [];
  let timerId = 0;
  function element(id) {
    if (!nodes.has(id)) {
      let html = '';
      const node = { value: '', disabled: false, scrollTop: 350, writes: 0,
        classList: { toggle() {}, remove() {}, add() {} }, setAttribute() {},
        querySelectorAll() { return []; }, querySelector() { return null; }, addEventListener() {} };
      Object.defineProperty(node, 'innerHTML', {
        get() { return html; }, set(value) { html = value; node.writes++; node.scrollTop = 0; }
      });
      nodes.set(id, node);
    }
    return nodes.get(id);
  }
  const context = {
    URL, URLSearchParams, AbortController, console, rendered,
    localStorage: { getItem() { return null; } },
    window: { setTimeout(fn, delay) { const id = ++timerId; timers.set(id, { fn, delay }); return id; },
      clearTimeout(id) { timers.delete(id); } },
    document: { getElementById: element, addEventListener() {}, querySelector() { return element('detail-body'); },
      querySelectorAll(selector) { return selector === '#dev-actual-labels input:checked' ? [{ value: 'qa' }] : []; } },
    fetch: async (url, options) => {
      const request = { url, options, sequence: Number(new URL(url, 'http://localhost').searchParams.get('sequence')) };
      requests.push(request);
      return context.respond(request);
    },
    respond: async request => ({ ok: true, json: async () => request.options?.method === 'POST' ?
      { ok: true, audit: { revision: 2, actual_primary: 'qa' } } : payload(request.sequence) })
  };
  function bootstrap() {
    loadDevelopmentAuditSummary = async function () {};
    toast = function () {};
    renderDevelopmentAuditDetail = function () {
      globalThis.rendered.push(devAuditDetail);
      updateDevelopmentAuditControls();
    };
    activeView = 'dev-audit';
    devAuditVersion = 15;
    devAuditLoaded = true;
    devAuditItems = [1, 2, 3].map(function (sequence) {
      return { sequence: sequence, message: { content: 'sample' }, analysis: { primary: 'qa' }, audit: null };
    });
    devAuditTotal = 3;
    document.getElementById('dev-audit-status').value = 'todo';
    document.getElementById('dev-actual-primary').value = 'qa';
    document.getElementById('dev-audit-correction').hidden = true;
    globalThis.audit = {
      load: loadDevelopmentAuditDetail, request: requestDevelopmentAuditDetail,
      cached: cachedDevelopmentAuditDetail, invalidate: invalidateDevelopmentAuditDetails,
      save: saveDevelopmentAudit,
      snapshot: function () { return { selected: devAuditSelectedSequence, detail: devAuditDetail,
        loading: devAuditDetailLoading, saving: devAuditSaving, cacheSize: devAuditDetailCache.size }; }
    };
  }
  const source = fs.readFileSync(path.join(__dirname, 'app.js'), 'utf8');
  const cut = source.lastIndexOf('\n  bindEvents();');
  assert.ok(cut > 0);
  vm.createContext(context);
  vm.runInContext(source.slice(0, cut) + '(' + bootstrap.toString() + ')();\n})();', context);
  return { ...context.audit, context, requests, rendered, element,
    runTimers(delay) { for (const [id, timer] of [...timers]) if (timer.delay === delay) { timers.delete(id); timer.fn(); } } };
}

function payload(sequence, extra = {}) {
  return { version: 15, message: { sequence }, analysis: { primary: 'qa' }, audit: null,
    predicted_faq_id: '', ...extra };
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

test('switching keeps the list DOM and scroll; revisiting and re-clicking avoid duplicate loads', async () => {
  const app = workbench();
  await app.load(1);
  await app.load(2);
  await app.load(1);
  await app.load(1);
  assert.equal(app.requests.length, 2);
  assert.equal(app.rendered.length, 3);
  assert.equal(app.element('dev-audit-list').writes, 0);
  assert.equal(app.element('dev-audit-list').scrollTop, 350);
  assert.equal(app.element('dev-audit-detail').writes, 0, 'fast requests must not clear the pane to a spinner');
});

test('rapid back-and-forth cancels obsolete requests and displays only the newest response', async () => {
  const app = workbench();
  const replies = [deferred(), deferred(), deferred()];
  app.context.respond = request => replies[app.requests.indexOf(request)].promise;
  const first = app.load(1);
  const second = app.load(2);
  const third = app.load(1);
  assert.equal(app.requests.length, 3, 'an aborted request must not be reused on return');
  assert.equal(app.requests[0].options.signal.aborted, true);
  assert.equal(app.requests[1].options.signal.aborted, true);
  replies[2].resolve({ ok: true, json: async () => payload(1, { marker: 'new' }) });
  await third;
  replies[1].resolve({ ok: true, json: async () => payload(2) });
  replies[0].resolve({ ok: true, json: async () => payload(1, { marker: 'old' }) });
  await Promise.all([first, second]);
  assert.equal(app.snapshot().detail.marker, 'new');
  assert.equal(app.cached(1).marker, 'new');
  assert.equal(app.rendered.length, 1);
});

test('clicking the next row reuses its in-flight prefetch', async () => {
  const app = workbench();
  await app.load(1);
  const next = deferred();
  app.context.respond = () => next.promise;
  app.runTimers(250);
  assert.equal(app.requests.length, 2);
  const selecting = app.load(2);
  assert.equal(app.requests.length, 2);
  next.resolve({ ok: true, json: async () => payload(2) });
  await selecting;
  assert.equal(app.snapshot().detail.message.sequence, 2);
  assert.equal(app.snapshot().loading, false);
});

test('configuration or source invalidation forces a fresh detail even on the selected row', async () => {
  const app = workbench();
  await app.load(1);
  app.invalidate();
  await app.load(1);
  assert.equal(app.requests.length, 2);
  assert.equal(app.snapshot().cacheSize, 1);
});

test('saving a reviewed sample also updates the cached human conclusion', async () => {
  const app = workbench();
  app.element('dev-audit-status').value = 'audited';
  await app.load(1);
  await app.save();
  assert.equal(app.cached(1).audit.revision, 2);
  await app.load(2);
  await app.load(1);
  assert.equal(app.snapshot().detail.audit.revision, 2);
  assert.equal(app.requests.filter(request => request.sequence === 1).length, 1);
});

test('a failed switch cannot submit the previous message form', async () => {
  const app = workbench();
  await app.load(1);
  app.context.respond = async () => { throw new Error('network unavailable'); };
  await app.load(2);
  assert.equal(app.snapshot().detail, null);
  assert.equal(app.element('dev-audit-save').disabled, true);
  await app.save();
  assert.equal(app.requests.filter(request => request.options?.method === 'POST').length, 0);
  await app.load(1);
  assert.equal(app.snapshot().detail.message.sequence, 1);
  assert.equal(app.element('dev-audit-save').disabled, false);
});
