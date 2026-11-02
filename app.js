(function () {
  'use strict';

  var STORAGE_KEY = 'message-workbench-live-v1';
  var LEGACY_STORAGE_KEY = 'message-workbench-demo-v1';
  var groupNames = ['新开群回复话术', '新开群回复', '养群回复', '接粉', '其他'];
  var seed = {
    faqs: [],
    settings: { aftercareName: '售后客服', aftercareUrl: '', aftercareScope: '', aftercareTemplate: '这边帮您登记一下，请联系售后客服继续处理：{{售后链接}}', previousActivityTemplate: '请联系售后客服咨询之前活动的商品链接：{{售后链接}}', galleryType: 'bitable', galleryUrl: '', orderTableUrl: '', demandTableUrl: '', todayProductCopy: '', soldLastWeekProducts: '', soldNextWeekProducts: '', thanksTemplate: '谢谢支持～', allowedShareDomains: '', feishuAppId: '', rules: { feedback: true, warning: true, aftercare: true, qa: true, closing: true, burst: true, burstCount: 8, burstSeconds: 30, conversationSeconds: 1800, qaConfidence: 82, allowlist: '' } },
    audit: []
  };

  function readState() {
    try {
      var current = localStorage.getItem(STORAGE_KEY);
      var migratingLegacy = !current;
      var saved = JSON.parse(current || localStorage.getItem(LEGACY_STORAGE_KEY) || '{}');
      var faqs = Array.isArray(saved.faqs) ? saved.faqs.filter(function (faq) {
        return !migratingLegacy || faq.source === '本地修改' || faq.source === '本地新增';
      }) : seed.faqs.slice();
      return {
        messages: [],
        faqs: faqs,
        settings: Object.assign({}, seed.settings, saved.settings || {}, { rules: Object.assign({}, seed.settings.rules, (saved.settings || {}).rules || {}) }),
        audit: Array.isArray(saved.audit) ? saved.audit : [],
        decisions: saved.decisions && typeof saved.decisions === 'object' ? saved.decisions : {},
        live: true
      };
    } catch (error) {
      return { messages: [], faqs: seed.faqs.slice(), settings: Object.assign({}, seed.settings, { rules: Object.assign({}, seed.settings.rules) }), audit: [], decisions: {}, live: true };
    }
  }

  var state = readState();
  var activeView = 'inbox';
  var activeFilter = 'all';
  var selectedMessageId = state.messages.find(function (message) { return message.status === 'pending'; })?.id || state.messages[0]?.id;
  var activeFaqGroup = '全部';
  var editingFaqId = null;
  var liveStatus = { connected: false, phase: '连接本机服务中', error: '', received_count: 0, latest_sequence: 0 };
  var liveStatusReady = false;
  var liveCursor = 0;
  var isFirstLiveSync = true;
  var liveInstance = '';
  var hasOlder = false;
  var syncInFlight = false;
  var isSyncChanging = false;
  var isHistoryLoading = false;
  var replies = [];
  var replyBefore = 0;
  var replyHasOlder = false;
  var selectedReplySequence = 0;
  var repliesLoading = false;
  var replyKnownCount = null;
  var queueItems = [];
  var queueLoaded = false;
  var queueBefore = 0;
  var queueHasOlder = false;
  var queueTotal = 0;
  var queueRequestId = 0;
  var lastQueueAnalysisRefreshAt = 0;
  var filterTimer = null;
  var serverConfigReady = false;
  var configSaveTimer = null;
  var devAuditItems = [];
  var workflowData = null;
  var workflowLoading = false;
  var suggestionStatus = 'todo';
  var suggestionItems = [];
  var selectedSuggestionId = '';
  var suggestionLoading = false;
  var suggestionCursor = null;
  var suggestionHasMore = false;
  var faqCandidateItems = [];
  var faqCandidateStatus = 'pending';
  var faqCandidateScope = 'all';
  var faqCandidateSource = null;
  var workflowJobMonitoring = false;
  var devAuditLoaded = false;
  var devAuditBefore = 0;
  var devAuditHasMore = false;
  var devAuditTotal = 0;
  var devAuditRequestId = 0;
  var devAuditDetailRequestId = 0;
  var devAuditSelectedSequence = 0;
  var devAuditDetail = null;
  var devAuditVersion = 0;
  var devAuditLoading = false;
  var devAuditSaving = false;
  var devAuditListKey = '';
  var devAuditDetailLoading = false;
  var devAuditDetailCache = new Map();
  var devAuditDetailPending = new Map();
  var devAuditDetailCacheEpoch = 0;
  var devAuditDetailEpoch = -1;
  var devAuditPrefetchTimer = null;
  var devAuditFilterTimer = null;
  var devAuditLabels = { service: '售后问题', feedback: '反馈图候选', warning: '疑似推广', mini: '小程序', burst: '疑似刷屏', order_image: '订单截图', requirement: '产品需求', product_link: '当天商品链接', thanks: '强提醒感谢', qa: '日常问题', closing: '无需回复', ordinary: '普通交流', unknown: '待判断' };
  var suggestionAssigneeLabels = { aftercare: '售后客服', group_human: '群内人工', reviewer: '待人工核验' };
  function suggestionAssigneeLabel(value) { return suggestionAssigneeLabels[value] || value || '待分配'; }
  function suggestionIntentLabel(value) { return devAuditLabels[value] || value || '待判断'; }
  var suggestionReviewLabels = { correct: '已完成例外核对', corrected: '已记录 FAQ / 分类问题', needs_human: '已转人工核对', not_applicable: '无需 FAQ 审核', auto_matched: '系统对照完成', auto_partial: '系统标记答案需补全', auto_conflict: '系统标记 FAQ 冲突', auto_irrelevant: '系统标记 FAQ 无关', auto_no_hit: '系统记录 FAQ 缺口', auto_mixed: '系统已按消息分别对照' };
  function suggestionReviewLabel(value) { return suggestionReviewLabels[value] || '已记录审核意见'; }

  function persist(saveConfig) {
    var decisions = Object.assign({}, state.decisions || {});
    state.messages.forEach(function (message) {
      if (message.status !== 'pending') decisions[message.id] = message.status;
    });
    state.decisions = decisions;
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ faqs: state.faqs, settings: state.settings, audit: state.audit, decisions: decisions }));
    if (serverConfigReady && saveConfig !== false) {
      window.clearTimeout(configSaveTimer);
      configSaveTimer = window.setTimeout(saveServerConfig, 350);
    }
  }

  async function saveServerConfig() {
    try {
      var response = await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ settings: state.settings, faqs: state.faqs }) });
      if (!response.ok) throw new Error('本机配置保存失败');
      var saved = await response.json();
      invalidateDevelopmentAuditDetails();
      return saved;
    } catch (error) { toast('配置未保存到服务端', error.message || '稍后重试'); }
  }

  async function initializeWorkspace() {
    try {
      var savedBrowserState = localStorage.getItem(STORAGE_KEY) || localStorage.getItem(LEGACY_STORAGE_KEY);
      if (savedBrowserState && !localStorage.getItem('message-workbench-phase2-backup')) {
        try {
          var backupResponse = await fetch('/api/client-state-backup', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: savedBrowserState });
          if (!backupResponse.ok) throw new Error('旧版浏览器数据备份失败');
          var backup = await backupResponse.json();
          localStorage.setItem('message-workbench-phase2-backup', backup.digest);
        } catch (backupError) { toast('旧版数据尚未备份', backupError.message || '请保留当前浏览器数据'); }
      }
      var response = await fetch('/api/config', { cache: 'no-store' });
      if (!response.ok) throw new Error('读取服务端配置失败');
      var config = await response.json();
      if (!config.initialized) {
        response = await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ settings: state.settings, faqs: state.faqs, only_if_empty: true }) });
        if (!response.ok) throw new Error('迁移浏览器配置失败');
        config = await response.json();
      }
      state.settings = Object.assign({}, seed.settings, config.settings || {}, { rules: Object.assign({}, seed.settings.rules, (config.settings || {}).rules || {}) });
      state.faqs = Array.isArray(config.faqs) ? config.faqs : [];
      serverConfigReady = true;
      renderSettings(); renderRules(); renderFaq();
      loadIntegrations(); loadFaqSourceStatus();
      loadReviewHistory();
    } catch (error) { toast('本机配置暂不可用', error.message || '仍可查看已保存消息'); }
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, function (char) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char];
    });
  }

  function toast(title, detail) {
    var region = document.getElementById('toast-region');
    var item = document.createElement('div');
    item.className = 'toast';
    item.innerHTML = '<strong>' + escapeHtml(title) + '</strong>' + (detail ? '<small>' + escapeHtml(detail) + '</small>' : '');
    region.appendChild(item);
    window.setTimeout(function () { item.remove(); }, 3600);
  }

  function updateFilterButtonCount() {
    var count = ['filter-kind', 'filter-group', 'filter-start', 'filter-end', 'filter-status', 'filter-work-status'].reduce(function (total, id) {
      return total + (document.getElementById(id).value ? 1 : 0);
    }, 0);
    var badge = document.getElementById('filter-active-count');
    badge.textContent = String(count);
    badge.hidden = count === 0;
  }

  function countPending() {
    return state.messages.filter(function (message) { return message.status === 'pending'; }).length;
  }

  function updateCounters() {
    var analysisStats = liveStatus.analysis || {};
    var workStats = liveStatus.work_items || {};
    if (!liveStatusReady) {
      document.getElementById('metric-pending').textContent = '—';
      document.getElementById('nav-pending').textContent = '—';
      document.getElementById('filter-all-count').textContent = '—';
      document.getElementById('metric-total').textContent = '—';
      document.getElementById('metric-classified').textContent = '—';
      document.getElementById('metric-attention').textContent = '—';
      return;
    }
    var pending = liveStatus.pending_review != null
      ? Number(liveStatus.pending_review)
      : workStats.statuses ? Number(workStats.statuses.pending || 0) : countPending();
    document.getElementById('metric-pending').textContent = String(pending);
    document.getElementById('nav-pending').textContent = String(pending);
    document.getElementById('filter-all-count').textContent = String(Number(liveStatus.message_count || queueTotal || state.messages.length));
    var urgentCount = document.getElementById('filter-urgent-count');
    if (urgentCount) urgentCount.textContent = String(['service', 'warning', 'mini', 'burst', 'order_image', 'requirement', 'product_link', 'thanks'].reduce(function (sum, kind) {
      return sum + Number((analysisStats.by_category || {})[kind] || 0);
    }, 0));
    var conflictCount = document.getElementById('filter-conflict-count');
    if (conflictCount) conflictCount.textContent = String(Number(workStats.ai_disagreements || 0));
    var todayCount = Number(liveStatus.today_message_count || 0);
    var mediaCounts = liveStatus.media || {};
    var mediaTotal = Object.keys(mediaCounts).reduce(function (sum, key) { return sum + Number(mediaCounts[key] || 0); }, 0);
    var attention = state.messages.filter(function (message) { return message.status === 'pending' && (message.priority === '优先' || ['warning', 'mini', 'burst', 'order_image', 'requirement', 'product_link', 'thanks'].indexOf(message.kind) !== -1); }).length;
    var total = document.getElementById('metric-total');
    var coverage = document.getElementById('metric-classified');
    var attentionMetric = document.getElementById('metric-attention');
    if (total) total.textContent = String(todayCount);
    if (coverage) coverage.innerHTML = (mediaTotal ? Math.round(Number(mediaCounts.stored || 0) / mediaTotal * 100) : 0) + '<span class="metric-unit">%</span>';
    if (attentionMetric) attentionMetric.textContent = String(analysisStats.attention_pending == null ? attention : analysisStats.attention_pending);
  }

  function updateConnectionUi() {
    var paused = liveStatus.sync_enabled === false;
    var ocrState = document.getElementById('ocr-state');
    if (ocrState) ocrState.textContent = liveStatus.ocr_state || '等待本机识别';
    var visionState = document.getElementById('vision-state');
    if (visionState) visionState.textContent = liveStatus.vision_state || '等待图片视觉初筛';
    var sync = document.querySelector('.sync-label');
    if (sync) {
      sync.innerHTML = '<span class="status-dot ' + (liveStatus.connected && !paused ? 'green' : 'amber') + '"></span>' + escapeHtml(paused ? '消息同步已暂停' : (liveStatus.connected ? '云中客实时连接正常' : (liveStatus.error ? '云中客：' + liveStatus.error : liveStatus.phase || '连接中')));
    }
    var privacy = document.querySelector('.privacy-banner span:not(.privacy-banner-icon)');
    var ruleLabel = liveStatus.rule_id ? '规则 ' + String(liveStatus.rule_id) : '当前配置的规则';
    var ruleSmall = document.querySelector('.rule-label small');
    if (ruleSmall) ruleSmall.textContent = '规则 ID · ' + String(liveStatus.rule_id || '读取配置中');
    var liveRule = document.getElementById('live-rule-label');
    if (liveRule) liveRule.textContent = liveStatus.rule_id ? 'RULE ' + String(liveStatus.rule_id) : 'RULE ID · 配置加载中';
    var cmsRule = document.getElementById('cms-rule-id');
    if (cmsRule) cmsRule.textContent = String(liveStatus.rule_id || '读取配置中');
    var footerRule = document.getElementById('footer-rule-id');
    if (footerRule) footerRule.textContent = liveStatus.rule_id ? '规则 ' + String(liveStatus.rule_id) : '规则号读取配置中';
    var receiveState = paused ? '消息同步已暂停。' : liveStatus.connected ? '已连接' + ruleLabel + ' 的实时群消息。' : '消息接收当前未连接：' + (liveStatus.error || liveStatus.phase || '等待连接') + '。';
    if (privacy) privacy.textContent = receiveState + '源消息保存在本机数据库；人工审核只记录结论，不会触发群消息发送或飞书上传。';
    var cmsState = document.querySelector('.cms-card .connection-state');
    if (cmsState) cmsState.innerHTML = '<i class="status-dot ' + (liveStatus.connected && !paused ? 'green' : 'amber') + '"></i>' + escapeHtml(paused ? '消息同步已暂停' : (liveStatus.connected ? '实时已连接' : (liveStatus.error ? '连接异常' : '正在连接')));
    var realTimeText = document.querySelector('.cms-card .source-detail div:nth-child(4) strong');
    if (realTimeText) realTimeText.textContent = paused
      ? '同步已暂停'
      : liveStatus.connected
      ? '已连接 · ' + Number(liveStatus.message_count || 0) + ' 条已存' + (liveStatus.history_limited ? '（历史回溯受限）' : '')
      : String(liveStatus.phase || '').indexOf('受阻') >= 0
      ? '接收受阻，修复权限后重试'
      : (liveStatus.error ? '连接异常，服务将重试' : '连接中');
    var robotText = document.querySelector('.cms-card .source-detail div:nth-child(3) strong');
    if (robotText && liveStatus.bot_count != null) robotText.textContent = liveStatus.bot_count + ' 个关联机器人';
    var lastSync = document.getElementById('last-sync-text');
    if (lastSync) lastSync.textContent = paused ? '消息同步已暂停 · 已存 ' + Number(liveStatus.message_count || 0) + ' 条' : (liveStatus.last_message_at ? '最近消息 ' + liveStatus.last_message_at.slice(11, 16) : (liveStatus.connected ? '实时连接正常' : (liveStatus.phase || '等待连接')));
    var storeText = document.getElementById('source-store-status');
    if (storeText) storeText.textContent = '本机数据库 ' + Number(liveStatus.message_count || 0) + ' 条 · 图片 ' + Number((liveStatus.media || {}).stored || 0) + ' / ' + Object.keys(liveStatus.media || {}).reduce(function (sum, key) { return sum + Number(liveStatus.media[key] || 0); }, 0) + ' 已归档';
    var coverageText = document.getElementById('source-coverage-status');
    var retryAt = liveStatus.history_retry_after ? Date.parse(liveStatus.history_retry_after) : 0;
    var cooling = retryAt > Date.now();
    if (coverageText) coverageText.textContent = cooling
      ? '来源限流，' + liveStatus.history_retry_after.slice(11, 16) + ' 后自动续读 · 今日未完成 ' + Number(liveStatus.today_unfinished_history_scans || 0) + ' 个机器人扫描'
      : (liveStatus.coverage_gap || liveStatus.history_note || '历史覆盖尚未核验');
    var received = (liveStatus.reception_metrics || {}).by_first_source || {};
    if (coverageText && received.websocket) coverageText.textContent += ' · 近24小时实时首收 ' + received.websocket.count + ' 条，本地入库延迟95%在 ' + received.websocket.p95_seconds + ' 秒内；历史首收 ' + Number((received.history || {}).count || 0) + ' 条（不等于漏消息）';
    var more = document.getElementById('load-more');
    if (more) more.hidden = !hasOlder;
    var toggle = document.getElementById('toggle-sync');
    if (toggle) {
      toggle.textContent = paused ? '恢复同步' : '暂停同步';
      toggle.disabled = isSyncChanging;
    }
    var refresh = document.getElementById('refresh-label');
    var refreshButton = document.getElementById('refresh-demo');
    if (refresh) refresh.textContent = paused ? (Number(liveStatus.message_count || 0) ? '更新当日历史' : '读取当日历史') : '立即同步';
    if (refreshButton) refreshButton.disabled = isHistoryLoading || (paused && cooling);
    var rangeButton = document.getElementById('read-range-history');
    if (rangeButton) rangeButton.disabled = isHistoryLoading || !paused || cooling;
  }

  function classifyBurstMessages() {
    // The server uses source IDs and timestamps; a partly loaded browser page cannot count bursts reliably.
  }

  function acceptLiveMessage(raw) {
    if (!raw || !raw.id) return false;
    var existing = state.messages.find(function (item) { return item.id === raw.id; });
    if (existing) {
      Object.assign(existing, raw, { initials: String(raw.sender || '群').slice(0, 1), date: raw.date || raw.time || '' });
      return false;
    }
    var message = Object.assign({}, raw, {
      initials: String(raw.sender || '群').slice(0, 1),
      date: raw.date || raw.time || '',
      status: raw.status || 'pending'
    });
    state.messages.push(message);
    return true;
  }

  async function syncLive() {
    if (syncInFlight) return;
    syncInFlight = true;
    try {
      var statusResponse = await fetch('/api/status', { cache: 'no-store' });
      if (!statusResponse.ok) throw new Error('本机消息服务暂不可用');
      var previousAnalyzed = liveStatusReady ? Number((liveStatus.analysis || {}).analyzed || 0) : null;
      liveStatus = await statusResponse.json();
      liveStatusReady = true;
      var analysisProgressed = previousAnalyzed !== null &&
        Number((liveStatus.analysis || {}).analyzed || 0) > previousAnalyzed &&
        Number((liveStatus.analysis || {}).unprocessed || 0) > 0;
      var instanceChanged = !!liveInstance && liveInstance !== liveStatus.started_at;
      if (instanceChanged) {
        state.messages = [];
        liveCursor = 0;
        isFirstLiveSync = true;
      }
      liveInstance = liveStatus.started_at || '';
      var added = 0;
      if (isFirstLiveSync) {
        state.messages = [];
        liveCursor = Number(liveStatus.latest_sequence || 0);
        isFirstLiveSync = false;
      } else {
        for (var pageNumber = 0; pageNumber < 20; pageNumber += 1) {
          var messageResponse = await fetch('/api/messages?after=' + encodeURIComponent(liveCursor) + '&limit=200', { cache: 'no-store' });
          if (!messageResponse.ok) throw new Error('读取新增消息失败');
          var payload = await messageResponse.json();
          if (payload.reset_required) {
            isFirstLiveSync = true;
            throw new Error('消息游标已重置，请等待重新加载');
          }
          (payload.messages || []).forEach(function (message) { if (acceptLiveMessage(message)) added += 1; });
          liveCursor = Number(payload.next);
          if (!payload.has_more) break;
        }
        var selected = selectedMessage();
        if (selected && (selected.media || []).some(function (item) { return item.state === 'pending'; })) {
          var mediaResponse = await fetch('/api/messages?before=' + encodeURIComponent(Number(selected.sequence) + 1) + '&limit=1', { cache: 'no-store' });
          if (mediaResponse.ok) {
            var mediaPayload = await mediaResponse.json();
            (mediaPayload.messages || []).forEach(acceptLiveMessage);
          }
        }
      }
      state.messages.sort(function (a, b) { return Date.parse(b.timestamp) - Date.parse(a.timestamp); });
      classifyBurstMessages();
      updateConnectionUi();
      updateCounters();
      if (activeView === 'replies' && replyKnownCount !== null && Number(liveStatus.outbound_count || 0) !== replyKnownCount) loadReplies(true);
      var refreshForAnalysis = analysisProgressed && queueLoaded && Date.now() - lastQueueAnalysisRefreshAt >= 8000;
      if (added || instanceChanged || refreshForAnalysis || (queueLoaded && selectedMessage() && (selectedMessage().media || []).some(function (item) { return item.state === 'pending'; }))) {
        invalidateDevelopmentAuditDetails();
        if (refreshForAnalysis) lastQueueAnalysisRefreshAt = Date.now();
        loadQueue(true, queueLoaded && !instanceChanged);
      }
      if (activeView === 'inbox' && added && !queueLoaded) renderQueue();
      else if (activeView === 'inbox' && selectedMessageId) {
        var selected = selectedMessage();
        if (selected && (selected.media || []).some(function (item) { return item.state === 'pending'; })) renderDetail();
      }
      var refreshed = document.getElementById('refresh-demo');
      if (refreshed && refreshed.dataset.refreshRequested === 'true') {
        var syncPaused = liveStatus.sync_enabled === false;
        toast(syncPaused ? '消息同步已暂停' : '同步完成', syncPaused ? '当前仅查看本机已缓存的消息。' : liveStatus.connected ? (added ? '新增 ' + added + ' 条消息。' : '连接正常，当前没有新消息。') : '实时连接尚未建立，服务会自动重试。');
        refreshed.dataset.refreshRequested = 'false';
      }
    } catch (error) {
      liveStatus = { connected: false, phase: '本机消息服务不可用', error: error.message || '连接失败' };
      updateConnectionUi();
      if (isFirstLiveSync) {
        state.messages = [];
        renderQueue();
      }
    } finally {
      syncInFlight = false;
    }
  }

  async function loadOlderMessages() {
    loadQueue(false);
  }

  async function loadQueue(reset, preserveLoaded) {
    if (!reset && !queueHasOlder) return;
    var requestId = ++queueRequestId;
    var previousItems = reset && preserveLoaded && queueLoaded ? queueItems.slice() : [];
    var previousBefore = queueBefore;
    var previousHasOlder = queueHasOlder;
    var params = new URLSearchParams({ limit: '100' });
    if (!reset) params.set('before', String(queueBefore));
    if (activeFilter === 'urgent') params.set('urgent', '1');
    if (activeFilter === 'conflict') params.set('conflict', '1');
    var selectedKind = document.getElementById('filter-kind').value;
    if (selectedKind) params.set('kind', selectedKind);
    var query = document.getElementById('message-search').value.trim();
    var group = document.getElementById('filter-group').value.trim();
    var start = document.getElementById('filter-start').value;
    var end = document.getElementById('filter-end').value;
    var status = document.getElementById('filter-status').value;
    var workStatus = document.getElementById('filter-work-status').value;
    if (query) params.set('q', query);
    if (group) params.set('group', group);
    if (start) params.set('start', start + 'T00:00:00+08:00');
    if (end) params.set('end', end + 'T23:59:59+08:00');
    if (status) params.set('status', status);
    if (workStatus) params.set('work_status', workStatus);
    try {
      var response = await fetch('/api/review-queue?' + params.toString(), { cache: 'no-store' });
      if (!response.ok) throw new Error('审核队列读取失败');
      var payload = await response.json();
      if (requestId !== queueRequestId) return;
      var items = (payload.messages || []).map(function (raw) { return Object.assign({}, raw, { initials: String(raw.sender || '群').slice(0, 1), date: raw.date || raw.time || '' }); });
      if (reset && previousItems.length) {
        var previousBySequence = new Map(previousItems.map(function (item) { return [Number(item.sequence), item]; }));
        var freshSequences = new Set(items.map(function (item) { return Number(item.sequence); }));
        var refreshedHead = items.map(function (item) {
          var previous = previousBySequence.get(Number(item.sequence));
          return previous ? Object.assign({}, previous, item) : item;
        });
        queueItems = refreshedHead.concat(previousItems.filter(function (item) { return !freshSequences.has(Number(item.sequence)); }));
        queueItems.sort(function (a, b) { return Number(b.sequence || 0) - Number(a.sequence || 0); });
        queueBefore = previousBefore;
        queueHasOlder = previousHasOlder || !!payload.has_more;
      } else {
        queueItems = reset ? items : queueItems.concat(items);
        queueBefore = Number(payload.next_before || 0);
        queueHasOlder = !!payload.has_more;
      }
      queueLoaded = true;
      queueTotal = Number(payload.total || 0);
      hasOlder = queueHasOlder;
      renderQueue(); updateConnectionUi();
    } catch (error) { toast('审核队列读取失败', error.message || '请稍后重试'); }
  }

  async function toggleSync() {
    var enabled = liveStatus.sync_enabled === false;
    isSyncChanging = true;
    updateConnectionUi();
    try {
      var response = await fetch('/api/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: enabled })
      });
      var result = await response.json();
      if (!response.ok || result.sync_enabled !== enabled) throw new Error(result.error || '更新同步状态失败');
      liveStatus.sync_enabled = enabled;
      if (!enabled) liveStatus.connected = false;
      await syncLive();
      toast(enabled ? '消息同步已恢复' : '消息同步已暂停', enabled ? '工作台正在重新连接云中客。' : '工作台仍可查看本机已缓存的消息。');
    } catch (error) {
      toast('同步状态未更新', error.message || '请确认本机消息服务正常运行。');
    } finally {
      isSyncChanging = false;
      updateConnectionUi();
    }
  }

  async function loadHistoryOnce(useRange) {
    isHistoryLoading = true;
    updateConnectionUi();
    try {
      var range = useRange ? { start_date: document.getElementById('history-start-date').value, end_date: document.getElementById('history-end-date').value } : {};
      if (useRange && (!range.start_date || !range.end_date)) throw new Error('请先选择开始和结束日期');
      var response = await fetch('/api/history', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(range) });
      var result = await response.json();
      if (!response.ok || !result.ok) throw new Error(result.error || '读取当日历史失败');
      await syncLive();
      toast(result.state === 'complete' ? '历史读取完成' : '历史读取存在未核实范围', '扫描 ' + Number(result.pages || 0) + ' 页，新增 ' + Number(result.added || 0) + ' 条；' + (result.errors || []).slice(0, 1).join('；'));
    } catch (error) {
      toast('历史读取失败', error.message || '请稍后重试。');
    } finally {
      isHistoryLoading = false;
      updateConnectionUi();
    }
  }

  function setView(view) {
    activeView = view;
    document.querySelectorAll('.view').forEach(function (element) { element.classList.toggle('active', element.id === 'view-' + view); });
    document.querySelectorAll('.nav-item').forEach(function (element) { element.classList.toggle('active', element.dataset.view === view); });
    var titles = { inbox: '审核队列', replies: '已发消息', rules: '处理规则', faq: 'FAQ 管理', 'faq-candidates': 'FAQ 沉淀', copy: '售后话术', settings: '连接设置', history: '处理记录', 'dev-audit': '分类审核', 'business-data': '业务资料', 'suggestion-audit': '对话分拣校准' };
    titles.processing='信息处理';titles.exports='飞书入库';
    document.getElementById('breadcrumb-current').textContent = titles[view] || '工作台';
    var viewUrl = new URL(window.location.href);
    viewUrl.searchParams.set('view', view);
    window.history.replaceState(null, '', viewUrl);
    if (view === 'inbox') { renderQueue(); if (!queueLoaded) loadQueue(true); }
    if (view === 'replies') loadReplies(true);
    if (view === 'faq') renderFaq();
    if (view === 'faq-candidates') loadFaqCandidates();
    if (view === 'rules') { renderRules(); loadRuleVersions(); loadEvaluation(); }
    if (view === 'history') loadReviewHistory();
    if (view === 'dev-audit') {
      loadDevelopmentAuditSummary();
      if (!devAuditLoaded) loadDevelopmentAudits(true);
      else if (devAuditSelectedSequence && devAuditDetailEpoch !== devAuditDetailCacheEpoch) loadDevelopmentAuditDetail(devAuditSelectedSequence);
    }
    if (view === 'business-data') loadWorkflowData();
    if (view === 'processing' && window.processingCore) window.processingCore.load();
    if (view === 'exports' && window.feishuExportUI) window.feishuExportUI.load();
    if (view === 'suggestion-audit') { loadSuggestions(true); monitorWorkflowJob(false); }
  }

  async function workflowPost(path, body) {
    var response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
    var payload = await response.json();
    if (!response.ok) throw new Error(payload.error || '请求失败');
    return payload;
  }

  async function loadWorkflowData() {
    if (workflowLoading) return;
    workflowLoading = true;
    var host = document.getElementById('workflow-source-list');
    if (host && !workflowData) host.innerHTML = '<div class="work-item-load-state">正在读取本机资料配置…</div>';
    try {
      var response = await fetch('/api/workflow/data', { cache: 'no-store' });
      var data = await response.json();
      if (!response.ok) throw new Error(data.error || '读取失败');
      workflowData = data;
      renderWorkflowData();
    } catch (error) {
      if (host) host.innerHTML = '<div class="queue-empty">业务资料暂不可用：' + escapeHtml(error.message || '读取失败') + '</div>';
    } finally { workflowLoading = false; }
  }

  function workflowSourceCard(source, accounts) {
    if (!source.enabled) return '<div class="workflow-source-card"><div><strong>'+escapeHtml(source.title)+' · 已停用</strong><small>当前文案来自排品计划中按消息日期查找的日期子表。旧测试来源及历史快照保留，不参与当前连接检查。</small><small>'+escapeHtml(source.url)+'</small></div></div>';
    var statusNames={not_configured:'未配置',permission_denied:'节点无读取权限',previewed_partial:'局部预览待确认',previewed:'已预览',needs_confirmation:'待确认',confirmed_schedule:'已确认排期范围',failed:'读取失败',not_synced:'尚未读取'};
    var permission=source.state==='permission_denied'?'；请检查该知识库节点是否向当前飞书应用开放读取权限':'';
    var periodic=source.sync_enabled?'已开启，只读；间隔 '+Number(source.sync_interval_minutes||360)+' 分钟'+(source.next_sync_at?'；下次检查 '+escapeHtml(source.next_sync_at):''):'已关闭，仅手动读取';
    var accountOptions=(accounts||[]).map(function(account){return '<option value="'+escapeHtml(account.id)+'">'+escapeHtml(account.display_name)+' · '+escapeHtml({paused:'暂停',enabled:'启用',error:'连接异常'}[account.state]||'待配置')+'</option>';}).join('');
    var intervals=[60,180,360,720,1440];
    var intervalOptions=intervals.map(function(value){return '<option value="'+value+'">'+(value<1440?value+' 分钟':Math.round(value/60)+' 小时')+'</option>';}).join('');
    return '<div class="workflow-source-card"><div><strong>'+escapeHtml(source.title)+'</strong><small>'+escapeHtml(source.url)+'</small><small>状态：'+escapeHtml(statusNames[source.state]||source.state)+(source.error_code?' · 错误 '+escapeHtml(source.error_code):'')+escapeHtml(permission)+'</small><small>周期同步：'+escapeHtml(periodic)+'</small><small>最近成功：'+escapeHtml(source.last_success_at||'尚无成功读取')+'</small>'+(source.error_summary?'<small>'+escapeHtml(source.error_summary)+'</small>':'')+'</div><div><label class="field"><span>源链接</span><input data-source-url="'+escapeHtml(source.id)+'" value="'+escapeHtml(source.url)+'"></label>'+(source.source_type==='sheet'?'<label class="field"><span>工作表 ID</span><input data-source-sheet="'+escapeHtml(source.id)+'" value="'+escapeHtml(source.sheet_id||'')+'"></label>':'')+'<label class="field"><span>资料账号范围</span><select data-source-account="'+escapeHtml(source.id)+'">'+accountOptions+'</select></label><label class="field"><span>只读同步间隔</span><select data-source-interval="'+escapeHtml(source.id)+'">'+intervalOptions+'</select></label><label class="field"><span>周期只读同步</span><input type="checkbox" data-source-periodic="'+escapeHtml(source.id)+'" '+(source.sync_enabled?'checked':'')+'>启用（默认关闭）</label></div><div class="workflow-source-actions"><button class="button button-secondary" data-source-save="'+escapeHtml(source.id)+'">保存资料源与周期设置</button><button class="button button-secondary" data-source-read="'+escapeHtml(source.id)+'" data-action="test">测试连接</button><button class="button button-secondary" data-source-read="'+escapeHtml(source.id)+'" data-action="preview">只读预览</button><button class="button button-secondary" data-source-read="'+escapeHtml(source.id)+'" data-action="sync">手动读取完整范围</button></div></div>';
  }

  function renderWorkflowData() {
    if (!workflowData) return;
    document.getElementById('workflow-schema-state').textContent = '迁移 v' + Number(workflowData.schema_version || 0) + ' · AI 调用 ' + Object.values(workflowData.ai_usage || {}).reduce(function (sum, item) { return sum + Number(item.calls || 0); }, 0) + ' 次';
    var sourceHost = document.getElementById('workflow-source-list');
    sourceHost.innerHTML = (workflowData.sources || []).map(function (source) { return workflowSourceCard(source,workflowData.accounts); }).join('') || '<div class="queue-empty">没有配置资料源。</div>';
    (workflowData.sources||[]).forEach(function(source){var select=sourceHost.querySelector('[data-source-account="'+source.id+'"]');if(select)select.value=source.source_account_id||liveStatus.source_account_id||'';var interval=sourceHost.querySelector('[data-source-interval="'+source.id+'"]');if(interval)interval.value=String(source.sync_interval_minutes||360);});
    var profileSelect = document.getElementById('group-profile');
    var currentProfile = profileSelect.value;
    profileSelect.innerHTML = '<option value="">暂不指定</option>' + (workflowData.service_profiles || []).map(function (profile) { return '<option value="' + escapeHtml(profile.id) + '">' + escapeHtml(profile.name) + (profile.url ? '' : '（链接待补）') + '</option>'; }).join('');
    if (currentProfile) profileSelect.value = currentProfile;
    var profileEdit=document.getElementById('service-existing');var currentProfileEdit=profileEdit.value;
    profileEdit.innerHTML='<option value="">新建客服配置</option>'+(workflowData.service_profiles||[]).map(function(profile){return '<option value="'+escapeHtml(profile.id)+'">'+escapeHtml(profile.name)+' · v'+Number(profile.version||1)+'</option>';}).join('');
    if(currentProfileEdit)profileEdit.value=currentProfileEdit;
    var productEdit=document.getElementById('product-catalog-existing');var currentProductEdit=productEdit.value;
    productEdit.innerHTML='<option value="">新建商品</option>'+(workflowData.products||[]).map(function(product){return '<option value="'+escapeHtml(product.id)+'">'+escapeHtml(product.name)+' · '+escapeHtml(product.state)+'</option>';}).join('');
    if(currentProductEdit)productEdit.value=currentProductEdit;
    var linkProduct=document.getElementById('product-link-product');var currentLinkProduct=linkProduct.value;
    linkProduct.innerHTML='<option value="">手动录入商品名</option>'+(workflowData.products||[]).map(function(product){return '<option value="'+escapeHtml(product.id)+'">'+escapeHtml(product.name)+' · '+escapeHtml(product.state)+'</option>';}).join('');
    if(currentLinkProduct)linkProduct.value=currentLinkProduct;
    var selectedLinkProduct=(workflowData.products||[]).find(function(product){return product.id===linkProduct.value;});
    var linkProductName=document.getElementById('product-link-name');
    if(selectedLinkProduct)linkProductName.value=selectedLinkProduct.name;
    linkProductName.readOnly=Boolean(selectedLinkProduct);
    document.getElementById('product-catalog-list').innerHTML=(workflowData.products||[]).map(function(product){return '<div class="workflow-record-card"><div><strong>'+escapeHtml(product.name)+'</strong><small>'+escapeHtml(product.state)+' · 别名：'+escapeHtml((product.aliases||[]).join('、')||'无')+'</small><small>ID：'+escapeHtml(product.id)+'</small></div></div>';}).join('')||'<div class="workflow-note">商品目录为空。仅录入人工确认的名称和别名，不代表实际在售。</div>';
    document.getElementById('business-groups-list').innerHTML = (workflowData.groups || []).map(function (group) {
      return '<div class="workflow-record-card"><div><strong>' + escapeHtml(group.name) + '</strong><small>群 ID：' + escapeHtml(group.group_id) + ' · 账号：' + escapeHtml(group.source_account_id) + '</small><small>客服：' + escapeHtml(group.service_name || '未配置') + (group.service_url ? ' · ' + escapeHtml(group.service_url) : ' · 链接待补') + '</small></div></div>';
    }).join('') || '<div class="workflow-note">尚未建立群映射。群 ID 可从现有消息详情复制；未配置前，建议会明确显示待补客服。</div>';
    document.getElementById('product-links-list').innerHTML = (workflowData.product_links || []).slice(0, 20).map(function (item) {
      return '<div class="workflow-record-card"><div><strong>' + escapeHtml(item.product_name) + '</strong><small>' + escapeHtml(item.business_date || '日期待补') + ' · ' + escapeHtml(item.state) + ' · 账号：' + escapeHtml(item.source_account_id || '未绑定来源账号') + ' · 群：' + escapeHtml((item.group_scope || []).join('、') || '未配置') + '</small><small>' + escapeHtml(item.copy || item.url || '购买链接待补') + '</small></div></div>';
    }).join('') || '<div class="workflow-note">当前没有独立商品链接记录。可在上方新增草稿，后续确认日期与群范围后才可发布到建议中。</div>';
    document.getElementById('schedule-candidates-list').innerHTML = (workflowData.schedule_candidates || []).slice(0, 40).map(function (item) {
      return '<div class="workflow-record-card"><div><strong>' + escapeHtml(item.item) + '</strong><small>' + escapeHtml(item.business_date || '日期待确认') + ' · ' + escapeHtml(item.cell_ref) + ' · ' + escapeHtml(item.state) + (item.is_sales_fact ? ' · 已售事实' : ' · 排期候选，不等于已售') + '</small></div></div>';
    }).join('') || '<div class="workflow-note">尚无排品候选。请先对排品资料源执行只读预览。</div>';
    document.getElementById('manual-material-list').innerHTML = (workflowData.materials || []).slice(0, 15).map(function (item) {
      return '<div class="workflow-record-card"><div><strong>' + escapeHtml(item.title || item.material_type) + '</strong><small>' + escapeHtml(item.business_date || '适用日期待确认') + ' · ' + escapeHtml(item.state) + ' · 账号 ' + escapeHtml(item.source_account_id || '未绑定来源账号') + ' · 来源 ' + escapeHtml(item.source_id || '手动导入') + '</small><small>' + escapeHtml((item.body || '').slice(0, 220)) + '</small></div><div class="workflow-source-actions"><input type="date" data-material-date="' + escapeHtml(item.id) + '" value="' + escapeHtml(item.business_date || '') + '"><input data-material-groups="' + escapeHtml(item.id) + '" value="' + escapeHtml((item.group_scope || []).join(', ')) + '" placeholder="群 ID 列表"><button class="button button-secondary" data-material-confirm="' + escapeHtml(item.id) + '" data-account="' + escapeHtml(item.source_account_id || '') + '" data-state="' + (item.state === 'confirmed' ? 'inactive' : 'confirmed') + '">' + (item.state === 'confirmed' ? '停用' : '确认生效') + '</button></div></div>';
    }).join('') || '<div class="workflow-note">无手动导入资料。</div>';
    var accountSelect = document.getElementById('group-account');var currentAccount=accountSelect.value;
    accountSelect.innerHTML = (workflowData.accounts || []).map(function (account) { return '<option value="' + escapeHtml(account.id) + '">' + escapeHtml(account.display_name) + ' · ' + escapeHtml(account.state) + '</option>'; }).join('') || '<option value="' + escapeHtml(liveStatus.source_account_id || '') + '">云中客主账号（尚未就绪）</option>';
    if(currentAccount)accountSelect.value=currentAccount;
    document.getElementById('observed-groups').innerHTML = (workflowData.observed_groups || []).map(function(group){return '<option value="'+escapeHtml(group.group_id)+'">'+escapeHtml(group.name)+' · '+escapeHtml(group.source_account_id)+'</option>';}).join('');
  }

  async function runSourceRead(sourceId, action) {
    var resultNode = document.getElementById('workflow-operation-result');
    resultNode.textContent = '正在通过飞书官方只读接口读取；不会写入飞书…';
    try {
      var sourceAccountSelect=document.querySelector('[data-source-account="'+sourceId+'"]');
      var result = await workflowPost('/api/workflow/sync', { source_id: sourceId, action: action,
        source_account_id: sourceAccountSelect&&sourceAccountSelect.value });
      if (!result.ok) resultNode.textContent = '读取未完成：' + (result.error || '未知错误') + (result.error_code ? '（' + result.error_code + '）' : '');
      else {
        var preview = result.preview || {};
        var schedule = preview.schedule_preview;
        var dayLookup=preview.daily_page_lookup||{};
        var daySummary=schedule&&dayLookup.requested_dates&&dayLookup.requested_dates.length?'；按消息时间读取日期子表 '+(dayLookup.loaded_dates||[]).length+'/'+dayLookup.requested_dates.length+'（'+dayLookup.requested_dates.join('、')+'）':'';
        resultNode.textContent = '读取成功，状态：' + result.state + '；范围：' + (result.coverage && result.coverage.row_count != null ? result.coverage.row_count + ' 行' : '文案正文') + (preview.title ? '；工作表/文档：' + preview.title : '') + daySummary + (schedule ? '；排期候选 ' + schedule.candidates.length + ' 项，按消息时间定位日期；排期不代表已售事实。' : '') + (preview.content ? '；文案预览：' + preview.content.slice(0, 300) : '');
      }
      await loadWorkflowData();
    } catch (error) { resultNode.textContent = '只读读取失败：' + (error.message || '请稍后重试') + '；已有成功快照不会被清空。'; await loadWorkflowData(); }
  }

  async function loadSuggestions(resetSelection) {
    if (suggestionLoading) return;
    suggestionLoading = true;
    var list = document.getElementById('suggestion-list');
    var loadMore = document.getElementById('load-more-suggestions');
    list.setAttribute('aria-busy', 'true');
    try {
      if(resetSelection){suggestionCursor=null;suggestionHasMore=false;}
      var previousScroll=list.scrollTop;
      var query=new URLSearchParams({status:suggestionStatus,limit:'100'});
      if(!resetSelection&&suggestionCursor){query.set('before_created',suggestionCursor.created_at);query.set('before_id',suggestionCursor.id);}
      var response = await fetch('/api/suggestions?' + query.toString(), { cache: 'no-store' });
      var data = await response.json(); if (!response.ok) throw new Error(data.error || '建议读取失败');
      var newItems=data.items||[];
      if(resetSelection)suggestionItems=newItems;
      else {var known=new Set(suggestionItems.map(function(item){return item.id;}));suggestionItems=suggestionItems.concat(newItems.filter(function(item){return !known.has(item.id);}));}
      suggestionCursor=data.next_before||null;suggestionHasMore=Boolean(data.has_more);
      document.getElementById('suggestion-count').textContent = suggestionItems.length + (suggestionHasMore?'+':'') + ' 条 · ' + ({todo:'待人工核对',reviewed:'系统已整理',all:'全部'})[suggestionStatus];
      document.getElementById('suggestion-page-count').textContent='已加载 '+suggestionItems.length+(suggestionHasMore?' 条以上':' 条');
      if(loadMore)loadMore.hidden=!suggestionHasMore;
      list.innerHTML = suggestionItems.map(function (item) {
        var p=item.payload||{}; var preview=(p.messages||[]).map(function(m){return m.content||'';}).filter(Boolean).join(' · ');
        var intent=p.intent||item.intent||'unknown';
        var replyCount=(p.gold_reply_sequences||[]).length;
        var faqComparison=p.faq_comparison||{};
        var faqStatusLabel=faqComparison.label||'待核对 FAQ';
        var messageCount=Number(item.message_count||0)+' 条客户消息'+(replyCount?' · '+replyCount+' 条真实回复':'');
        return '<button type="button" class="message-row suggestion-item ' + (item.id===selectedSuggestionId?'selected':'') + '" data-suggestion-id="' + escapeHtml(item.id) + '"><span class="row-type-icon type-' + escapeHtml(intent) + '">' + messageIcon(intent) + '</span><span class="row-main"><span class="row-heading"><strong>' + escapeHtml(item.group_name||p.group_name||'群名称待补') + '</strong><span class="row-time">' + escapeHtml(suggestionAssigneeLabel(item.assignee)) + '</span></span><span class="row-meta">客户：' + escapeHtml(item.sender_name||p.sender_name||'客户昵称待补') + ' · ' + escapeHtml(suggestionIntentLabel(intent)) + ' · ' + escapeHtml(messageCount) + '</span><span class="row-preview">' + escapeHtml(preview.slice(0,180)||'无文字内容') + '</span><span class="row-bottom"><span class="tag tag-' + escapeHtml(item.state||'pending') + '">' + escapeHtml(item.state==='reviewed'?suggestionReviewLabel((item.review||{}).decision):'待处理') + '</span><span class="faq-match-badge faq-match-' + escapeHtml(faqComparison.status||'unknown') + '">' + escapeHtml(faqStatusLabel) + '</span></span></span></button>';
      }).join('') || '<div class="queue-empty">' + (suggestionStatus==='todo'?'本轮没有待人工核对项。系统已整理的结果在“系统已整理”中；FAQ 候选在“FAQ 沉淀”中确认。':'没有匹配记录。') + '</div>';
      if (resetSelection || !suggestionItems.some(function(item){return item.id===selectedSuggestionId;})) selectedSuggestionId=suggestionItems[0]&&suggestionItems[0].id||'';
      if(!resetSelection)list.scrollTop=previousScroll;
      if(resetSelection || !selectedSuggestionId)renderSuggestionDetail();
    } catch (error) { list.innerHTML='<div class="queue-empty">建议读取失败：'+escapeHtml(error.message||'请稍后重试')+'</div>'; }
    finally { suggestionLoading=false; list.setAttribute('aria-busy','false'); }
  }

  async function loadFaqCandidates() {
    var list=document.getElementById('faq-candidate-list');
    if(!list)return;
    list.setAttribute('aria-busy','true');
    try {
      var query=new URLSearchParams({status:faqCandidateStatus,scope:faqCandidateScope,limit:'300'});
      var response=await fetch('/api/faq-candidates?'+query.toString(),{cache:'no-store'});
      var data=await response.json();if(!response.ok)throw new Error(data.error||'候选读取失败');
      faqCandidateItems=data.items||[];
      document.getElementById('faq-candidate-count').textContent=faqCandidateItems.length+' 条 · '+({pending:'待确认',approved:'已加入 FAQ',rejected:'暂不采用',all:'全部'})[faqCandidateStatus];
      renderFaqCandidates();
    } catch(error) { list.innerHTML='<div class="queue-empty">候选读取失败：'+escapeHtml(error.message||'请稍后重试')+'</div>'; }
    finally { list.setAttribute('aria-busy','false'); }
  }

  function renderFaqCandidates() {
    var list=document.getElementById('faq-candidate-list');if(!list)return;
    var statusLabels={pending:'待确认',approved:'已加入 FAQ',rejected:'暂不采用',merged:'已合并到同范围候选'};
    list.innerHTML=faqCandidateItems.length?faqCandidateItems.map(function(item){
      var evidence=item.evidence||[];var scopeLabel=item.scope==='product'?'当前产品':'通用';
      var productUnresolved=item.scope==='product'&&item.product_name==='待识别商品';
      var validity=item.scope==='product'?'有效期：'+(item.valid_from||'未设开始')+' 至 '+(item.valid_until||'未设失效'):'长期内容';
      var candidateExpired=item.scope==='product'&&item.valid_until&&item.valid_until<new Date().toLocaleDateString('sv-SE');
      var source=evidence.slice(0,3).map(function(ref){return (ref.group_name||'群待补')+' · '+(ref.sender_name||'客户待补');}).join('；');
      var actions=item.status==='pending'?'<button class="button button-quiet" data-faq-candidate-edit="'+escapeHtml(item.id)+'">编辑</button><button class="button button-secondary" data-faq-candidate-reject="'+escapeHtml(item.id)+'">暂不采用</button><button class="button button-primary" data-faq-candidate-approve="'+escapeHtml(item.id)+'">'+(candidateExpired?'确认并加入历史 FAQ':'确认并加入 FAQ')+'</button>':item.status==='rejected'?'<button class="button button-quiet" data-faq-candidate-restore="'+escapeHtml(item.id)+'">恢复待确认</button>':'';
      return '<article class="workflow-record-card faq-candidate-card"><div class="faq-candidate-copy"><div class="faq-candidate-title"><strong>'+escapeHtml(item.question)+'</strong><span class="faq-scope-pill faq-scope-'+escapeHtml(item.scope)+'">'+scopeLabel+'</span><span class="faq-candidate-state faq-candidate-state-'+escapeHtml(item.status)+'">'+(statusLabels[item.status]||item.status)+'</span>'+(candidateExpired?'<span class="faq-candidate-state faq-candidate-state-expired">已过期</span>':'')+'</div><p>'+escapeHtml(item.answer)+'</p><small>'+escapeHtml(item.scope==='product'?(productUnresolved?'商品尚未识别':'商品：'+item.product_name)+' · '+validity:validity)+'</small><small>'+escapeHtml(productUnresolved?'先确认商品名和适用日期，再确认加入。':candidateExpired?'确认后可用于对应日期的历史回放；有效期以外不会参与当前答疑。':'')+'</small><small>问法：'+escapeHtml(item.keywords||'未设置')+' · 来源案例 '+evidence.length+' 个</small><small>'+escapeHtml(source||'来源对话待补')+'</small></div><div class="workflow-source-actions">'+actions+'</div></article>';
    }).join(''):'<div class="queue-empty">当前没有待确认候选。整理对话时，问答明确、可复用且未命中或需要补全的真实回复会自动归入这里。</div>';
  }

  function setFaqCandidateScopeUI() {
    var product=document.getElementById('faq-candidate-scope').value==='product';
    document.getElementById('faq-candidate-product-field').hidden=!product;
    document.getElementById('faq-candidate-dates').hidden=!product;
    document.getElementById('faq-candidate-valid-until').required=false;
    document.getElementById('faq-candidate-product').required=product;
  }

  function openFaqCandidateFromSuggestion(item,p,replies) {
    var comparison=p.faq_comparison||{};var replySequences=(comparison.reply_sequences||[]).map(Number);
    var usableReplies=(replies||[]).filter(function(reply){return !replySequences.length||replySequences.indexOf(Number(reply.sequence))>=0;});
    if(!usableReplies.length){toast('没有可沉淀的真实回复','只有明确关联且包含实际答复的群消息可以作为 FAQ 来源。');return;}
    faqCandidateSource={suggestionId:item.id,replySequences:usableReplies.map(function(reply){return Number(reply.sequence);})};
    var question=(p.messages||[]).filter(function(message){return message.relation!=='acknowledgment'&&String(message.content||'').trim();}).map(function(message){return String(message.content||'').trim();}).slice(0,3).join('\n');
    var answer=Array.from(new Set(usableReplies.map(function(reply){return String(reply.content||'').trim();}).filter(Boolean))).join('\n');
    var products=(p.business_context||{}).matched_products||[];
    document.getElementById('faq-candidate-scope').value='product';
    document.getElementById('faq-candidate-product').value=products.length===1?products[0]:'待识别商品';
    var firstDate=String(((p.messages||[]).find(function(message){return message.timestamp;})||{}).timestamp||'').match(/^\d{4}-\d{2}-\d{2}/);
    document.getElementById('faq-candidate-valid-from').value=firstDate?firstDate[0]:'';
    document.getElementById('faq-candidate-valid-until').value=firstDate?firstDate[0]:'';
    document.getElementById('faq-candidate-question').value=question;
    document.getElementById('faq-candidate-answer').value=answer;
    document.getElementById('faq-candidate-keywords').value='';
    document.getElementById('faq-candidate-groups').value=((p.business_context||{}).group_scope||[p.group_id]).filter(Boolean).join(',');
    document.getElementById('faq-candidate-dialog-title').textContent='整理 FAQ 候选';
    document.getElementById('faq-candidate-save').textContent='保存为待确认候选';
    setFaqCandidateScopeUI();document.getElementById('faq-candidate-dialog').showModal();
  }

  function editFaqCandidate(candidateId) {
    var item=faqCandidateItems.find(function(candidate){return candidate.id===candidateId;});if(!item)return;
    var firstEvidence=(item.evidence&&item.evidence[0])||{};
    faqCandidateSource={candidateId:item.id,suggestionId:firstEvidence.suggestion_id,replySequences:firstEvidence.reply_sequences||[]};
    document.getElementById('faq-candidate-scope').value=item.scope;
    document.getElementById('faq-candidate-product').value=item.product_name||'';
    document.getElementById('faq-candidate-valid-from').value=item.valid_from||'';
    document.getElementById('faq-candidate-valid-until').value=item.valid_until||'';
    document.getElementById('faq-candidate-question').value=item.question||'';
    document.getElementById('faq-candidate-answer').value=item.answer||'';
    document.getElementById('faq-candidate-keywords').value=item.keywords||'';
    document.getElementById('faq-candidate-groups').value=(item.group_scope||[]).join(',');
    document.getElementById('faq-candidate-dialog-title').textContent='编辑 FAQ 候选';
    document.getElementById('faq-candidate-save').textContent='保存修改';
    setFaqCandidateScopeUI();document.getElementById('faq-candidate-dialog').showModal();
  }

  async function saveFaqCandidate(event) {
    if(!event.submitter||event.submitter.id!=='faq-candidate-save')return;
    event.preventDefault();if(!faqCandidateSource)return;
    var payload={id:faqCandidateSource.candidateId,scope:document.getElementById('faq-candidate-scope').value,
      product_name:document.getElementById('faq-candidate-product').value,valid_from:document.getElementById('faq-candidate-valid-from').value,
      valid_until:document.getElementById('faq-candidate-valid-until').value,question:document.getElementById('faq-candidate-question').value,
      answer:document.getElementById('faq-candidate-answer').value,keywords:document.getElementById('faq-candidate-keywords').value,
      group_scope:document.getElementById('faq-candidate-groups').value.split(',').map(function(x){return x.trim();}).filter(Boolean),
      source_suggestion_id:faqCandidateSource.suggestionId,source_reply_sequences:Array.from(new Set(faqCandidateSource.replySequences||[]))};
    try{await workflowPost('/api/faq-candidates',payload);document.getElementById('faq-candidate-dialog').close();faqCandidateSource=null;
      toast('已保存 FAQ 候选','候选待确认，不会立即用于答疑。');await loadFaqCandidates();}
    catch(error){toast('FAQ 候选未保存',error.message||'请检查内容和来源回复');}
  }

  async function setFaqCandidateStatus(candidateId,status) {
    try{await workflowPost('/api/faq-candidate/status',{id:candidateId,status:status});toast(status==='rejected'?'已暂不采用':'已恢复待确认','候选仍保留，可在对应状态中查看。');await loadFaqCandidates();}
    catch(error){toast('候选状态未更新',error.message||'请重试');}
  }

  async function approveFaqCandidate(candidateId) {
    try{var result=await workflowPost('/api/faq-candidate/approve',{id:candidateId});
      toast(result.duplicate_existing?'已关联到现有 FAQ':'已加入本机可用 FAQ',result.duplicate_existing?'没有重复新增条目。':'只影响本机后续建议；不会回写云中客或飞书。');
      var config=await fetch('/api/config',{cache:'no-store'});if(config.ok){var saved=await config.json();state.faqs=saved.faqs||[];state.settings=Object.assign({},state.settings,saved.settings||{});}
      renderFaq();await loadFaqCandidates();}
    catch(error){toast('FAQ 尚未启用',error.message||'请先完善候选内容');}
  }

  function renderSuggestionMedia(media) {
    var mediaId=Number(media&&media.id||0);if(!Number.isInteger(mediaId)||mediaId<=0)return '';
    var state=String(media.state||'pending');var mime=String(media.mime||'').toLowerCase();
    if(state!=='stored')return '<span class="suggestion-media-missing">图片暂不可预览 · '+escapeHtml(state)+'</span>';
    var url='/api/media/'+mediaId;
    if(!mime||mime.indexOf('image/')===0)return '<a class="suggestion-media" href="'+url+'" target="_blank" rel="noopener noreferrer"><img src="'+url+'" alt="消息图片 '+mediaId+'" loading="lazy"></a>';
    return '<a class="suggestion-attachment-link" href="'+url+'" target="_blank" rel="noopener noreferrer">查看附件 · '+escapeHtml(mime)+'</a>';
  }

  function businessContextMarkup(context) {
    if(!context||!context.business_date)return '';
    var matchLabels={matched_from_conversation:'根据对话上下文匹配',matched_from_reviewed_product_context:'沿用已审核的商品范围',single_scheduled_product:'当天只有一个排品，按唯一商品匹配',ambiguous_conversation:'上下文涉及多个商品，保留人工核对',product_not_resolved:'尚不能从上下文确定商品',no_products_scheduled:'当天月历没有识别到商品',not_checked:'尚未匹配商品'};
    var products=(context.planned_products||[]).map(function(item){return item.item||'';}).filter(Boolean);
    var matched=(context.matched_products||[]).filter(Boolean);
    var excerpt=String(context.daily_copy_excerpt||'').trim();
    var dailyStatus=context.daily_plan_status==='ready'?'当天子表已读取':context.daily_plan_status==='not_loaded'?'当天子表尚未读取':context.daily_plan_status==='daily_node_not_found'?'未找到对应日期子表':context.daily_plan_status==='daily_node_ambiguous'?'日期子表不唯一':'子表状态：'+String(context.daily_plan_status||'未知');
    return '<section class="business-context-card"><div class="business-context-heading"><strong>按消息日期匹配排期</strong><span>'+escapeHtml(context.business_date)+' · '+escapeHtml(matchLabels[context.product_match_status]||context.product_match_status||'待匹配')+'</span></div>'+
      '<p>日期依据：消息发生时间（上海时区） · '+escapeHtml(context.schedule_title||'飞书排品计划')+' · '+escapeHtml(dailyStatus)+'</p>'+
      '<div class="business-context-products"><span>当天排品</span><strong>'+escapeHtml(products.join('、')||'月历暂无匹配商品')+'</strong></div>'+
      '<div class="business-context-products"><span>对话匹配商品</span><strong>'+escapeHtml(matched.join('、')||'未唯一确定；商品 FAQ 不按日期直接套用')+'</strong></div>'+
      (context.daily_plan_title?'<small class="business-context-source">当日子表：'+escapeHtml(context.daily_plan_title)+'</small>':'')+
      (excerpt?'<pre class="business-context-excerpt">'+escapeHtml(excerpt)+'</pre>':'')+
      '<small class="business-context-note">'+escapeHtml(context.note||'排期用于确定当日商品和查找资料，不代表商品已售或允许自动发送。')+'</small></section>';
  }

  function renderSuggestionDetail() {
    var host=document.getElementById('suggestion-detail');
    var item=suggestionItems.find(function(x){return x.id===selectedSuggestionId;});
    if (!item) { host.innerHTML='<div class="detail-placeholder"><div><div class="empty-symbol">✓</div><strong>选择一段客户对话</strong><span>本轮用已有人工回复作标准答案，核对 FAQ 命中和分类。</span></div></div>'; return; }
    var p=item.payload||{}; var intent=p.intent||item.intent||'unknown';
    var review=item.review||null;var corrections=review&&review.corrections||{};
    var editIntent=corrections.intent||intent;
    var options=Object.keys(devAuditLabels).map(function(key){return '<option value="'+escapeHtml(key)+'" '+(key===editIntent?'selected':'')+'>'+escapeHtml(devAuditLabels[key])+'</option>';}).join('');
    var verdictLabels={match:'FAQ 能支持真实回复',partial:'相关但答案不完整',valid:'相关且有效',outdated:'与真实回复冲突',irrelevant:'与问题无关',uncertain:'无法判断'};
    var reviewVerdicts=corrections.faq_verdicts||{};
    var savedGold=(corrections.gold_reply_sequences||p.gold_reply_sequences||[]).map(Number);
    var reviewSummary=review?'<div class="workflow-record-card"><div><strong>最近审核：'+escapeHtml(suggestionReviewLabel(review.decision))+'</strong><small>审核版本 '+Number(review.revision||1)+' · '+escapeHtml(review.reviewed_at||'')+'</small><small>确认分类：'+escapeHtml(devAuditLabels[corrections.intent||intent]||corrections.intent||intent)+' · FAQ 判断：'+escapeHtml(Object.keys(reviewVerdicts).map(function(id){return verdictLabels[reviewVerdicts[id]]||'待核对';}).join('、')||corrections.faq_coverage||'无')+' · 标准回复 '+savedGold.length+' 条</small><small>原因：'+escapeHtml(review.reason||'未填写')+'</small></div></div>':'';
    var replyContext=p.reply_context||{};var replyMessages=replyContext.messages||[];var timeline=[];
    (p.messages||[]).forEach(function(m){timeline.push({kind:'case',sequence:Number(m.sequence),timestamp:m.timestamp||'',content:m.content||'[无文字内容]',sender_name:m.sender_name||p.sender_name||'',relation:m.relation||'issue',media:m.media||[],direction:'inbound'});});
    replyMessages.forEach(function(m){timeline.push({kind:'context',sequence:Number(m.sequence),timestamp:m.timestamp||'',content:m.content||'[无文字内容]',sender_name:m.sender_name||'',relation:m.relation||'',reply_link_type:m.reply_link_type||'context',media:m.media||[],direction:m.direction||'inbound'});});
    timeline.sort(function(a,b){var at=Date.parse(a.timestamp)||0,bt=Date.parse(b.timestamp)||0;return at===bt?a.sequence-b.sequence:at-bt;});
    var timelineMarkup=timeline.map(function(m){
      var isCase=m.kind==='case';var direction=m.direction==='outbound'?'群内已发消息':'客户消息';
      var relation=isCase?'本案例 · '+(m.relation==='acknowledgment'?'同会话后续':'问题消息'):m.reply_link_type==='direct_reply'||m.reply_link_type==='quoted'?'明确引用关联 · 回复对照':m.reply_link_type==='recipient_inferred'?'按唯一收件人推定关联 · 回复对照':m.reply_link_type==='recipient_ambiguous'?'发给该客户但对应问题不明确':m.relation==='same_customer_context'?'同一客户的其他消息':'群内消息（仅作上下文）';
      var media=(m.media||[]).map(renderSuggestionMedia).join('');
      return '<div class="conversation-context-item '+(m.direction==='outbound'?'outbound':'inbound')+(isCase?' case-message':'')+'"><small>'+escapeHtml(direction+(m.sender_name?' · '+m.sender_name:'')+' · '+relation+' · '+(m.timestamp||''))+' · 消息 #'+m.sequence+'</small><div>'+escapeHtml(m.content)+'</div>'+(media?'<div class="suggestion-media-grid">'+media+'</div>':'')+'</div>';
    }).join('');
    if(replyContext.has_more)timelineMarkup+='<small class="conversation-context-more">附近上下文较多，当前显示最近 40 条。</small>';
    var conversationContext='<section class="conversation-context"><h3>客户对话和真实群回复（按时间）</h3><p>明确引用到客户消息的群回复作为直接对照；没有引用编号时，仅在收件人唯一对应一个问题时标为推定关联。@提及和附近群消息只作上下文。</p>'+(timelineMarkup||'<div class="conversation-context-empty">当前记录没有可显示的消息上下文。</div>')+'</section>';
    var confirmedReplySet=new Set((p.gold_reply_sequences||[]).map(Number));
    replyMessages.forEach(function(m){if(m.direction==='outbound'&&(m.relation==='explicit_reply'||m.reply_to_case===true))confirmedReplySet.add(Number(m.sequence));});
    var confirmedReplies=replyMessages.filter(function(m){return m.direction==='outbound'&&confirmedReplySet.has(Number(m.sequence));});
    var actualReplySection=confirmedReplies.length?'<section class="suggestion-calibration-section"><h3>2. 真实回复与 FAQ 对照</h3><p>回复内容来自群内真实发送记录。明确引用的作为直接对照；按唯一收件人推定的会单独标记，便于识别关联不确定性。</p><ul class="suggestion-reply-list">'+confirmedReplies.map(function(m){var replyBasis=m.reply_link_type==='recipient_inferred'?'按收件人和唯一问题推定关联':m.reply_link_type==='quoted'?'引用回复':m.reply_link_type==='direct_reply'?'消息编号明确关联':'关联依据待确认';return '<li class="suggestion-reply-candidate"><div><strong>'+replyBasis+' · FAQ 对照回复</strong><small>'+(m.timestamp||'')+' · 消息 #'+Number(m.sequence||0)+'</small><p>'+escapeHtml(m.content||'[无文字内容]')+'</p></div></li>';}).join('')+'</ul></section>':'<section class="suggestion-calibration-section"><h3>2. 真实回复与 FAQ 对照</h3><p>当前没有找到可直接关联的人工回复。附近群发言只作上下文；本条仍可核对分类和 FAQ。</p></section>';
    var faqRefs=p.knowledge_refs||[];var savedVerdicts=corrections.faq_verdicts||{};var hasGoldReply=confirmedReplies.length>0;
    var faqComparison=p.faq_comparison||{status:'unknown',label:'待生成自动对照'};
    var faqTurnComparisons=p.faq_turn_comparisons||faqComparison.turn_comparisons||[];
    var faqTurnMarkup=faqTurnComparisons.filter(function(x){return x.status!=='not_applicable'||(x.reply_sequences||[]).length;}).map(function(x){
      return '<li class="faq-turn-comparison"><strong>消息 #'+Number(x.message_sequence||0)+' · '+escapeHtml(suggestionIntentLabel(x.intent))+'</strong><span>'+escapeHtml(x.label||'待对照')+(x.reply_sequences&&x.reply_sequences.length?' · 真实回复 #'+x.reply_sequences.map(Number).join('、'):' · 暂无明确关联回复')+'</span></li>';
    }).join('');
    var canCreateFaqCandidate=hasGoldReply&&((faqComparison.reply_sequences||[]).length>0||faqComparison.status==='no_hit'||faqComparison.status==='mixed'||faqComparison.status==='uncertain'||faqComparison.status==='partial'||faqComparison.status==='outdated');
    var faqChoices=hasGoldReply?[['unreviewed','尚未核对'],['match','能支持真实回复'],['partial','相关但答案不完整'],['outdated','内容过期或与回复冲突'],['irrelevant','与问题无关'],['uncertain','无法判断']]:[['unreviewed','尚未核对'],['valid','相关且仍有效'],['outdated','内容过期'],['irrelevant','与问题无关'],['uncertain','无法判断']];
    var faq=faqRefs.map(function(x,index){var faqId=String(x.id||('ref-'+index));var selected=savedVerdicts[faqId]||'unreviewed';
      var faqOptions=faqChoices.map(function(choice){return '<option value="'+choice[0]+'" '+(selected===choice[0]?'selected':'')+'>'+choice[1]+'</option>';}).join('');
      var automatic=(faqComparison.verdicts||{})[faqId]||{};var finalVerdict=savedVerdicts[faqId]||automatic.verdict||'';
      var verdictControl=finalVerdict&&finalVerdict!=='uncertain'?'<span class="faq-static-verdict">'+escapeHtml(verdictLabels[finalVerdict]||'已自动对照')+'</span>':
        (item.state==='pending'&&faqComparison.status==='uncertain'?'<label class="field"><span>系统无法确定，请只核对这一项：</span><select class="suggestion-faq-verdict" data-faq-id="'+escapeHtml(faqId)+'">'+faqOptions+'</select></label>':'<span class="faq-static-verdict">'+escapeHtml(verdictLabels[finalVerdict]||'已对照')+'</span>');
      return '<li class="suggestion-faq-review"><div class="suggestion-faq-copy"><strong>'+escapeHtml(x.question||x.id||'未知 FAQ')+'</strong><small>命中提示 '+Math.round(Number(x.score||0)*100)+'% · 仅用于找候选，不代表答案正确</small><p>'+escapeHtml(x.answer||'未保存答案')+'</p><small class="faq-verdict-reason">'+escapeHtml(automatic.reason||'')+'</small></div>'+verdictControl+'</li>';
    }).join('');
    var noFaqHit=!faqRefs.length&&faqComparison.status==='no_hit';
    if(!faq)faq='<li class="suggestion-faq-empty">'+(noFaqHit?'本条有明确真实回复，但没有命中已启用 FAQ；可考虑沉淀候选。真实群回复仍作为正确答案保留。':faqComparison.status==='not_applicable'?'没有明确关联的实质性人工回复，本条无需 FAQ 核对。':'本条没有可用 FAQ 命中。')+'</li>';
    var faqComparisonBanner='<div class="faq-comparison-banner faq-comparison-'+escapeHtml(faqComparison.status||'unknown')+'"><strong>'+escapeHtml(faqComparison.label||'FAQ 对照状态待生成')+'</strong><span>'+escapeHtml(faqComparison.note||'由本地规则对照，不调用大模型。')+'</span></div>';
    var faqCandidateAction=faqComparison.candidate_id?'<span class="faq-static-verdict">已整理到 FAQ 沉淀 · 待确认后才会启用</span>':canCreateFaqCandidate?'<button class="button button-secondary" id="open-faq-candidate">整理为 FAQ 候选</button>':'';
    var images=(p.image_actions||[]).map(function(x){return '<li>媒体 #'+Number(x.media_id||0)+' · '+escapeHtml(x.usage||'用途待判断')+' · '+escapeHtml(x.state||'')+'</li>';}).join('')||'<li>没有图片附加动作</li>';
    var blocked=(p.blockers||p.missing_information||[]).map(function(x){return '<li>'+escapeHtml(x)+'</li>';}).join('')||'<li>无明确阻断项；仍需人工检查来源事实</li>';
    host.innerHTML='<div class="suggestion-detail-body"><div class="detail-head"><div><div class="detail-kind"><span class="tag tag-'+escapeHtml(intent)+'">'+escapeHtml(suggestionIntentLabel(intent))+'</span><span class="detail-title">'+escapeHtml(p.group_name||item.group_name||'群名称待补')+'</span></div><div class="detail-id">群：'+escapeHtml(p.group_name||item.group_name||'群名称待补')+' · 客户：'+escapeHtml(p.sender_name||item.sender_name||'客户昵称待补')+' · '+Number((p.message_sequences||[]).length)+' 条客户消息'+((p.gold_reply_sequences||[]).length?' · '+(p.gold_reply_sequences||[]).length+' 条真实回复':'')+'</div></div><span class="detail-priority">'+escapeHtml(item.state==='reviewed'?'已审核':'待处理')+'</span></div>'+reviewSummary+conversationContext+businessContextMarkup(p.business_context)+'<section class="suggestion-calibration-section"><h3>1. 核对分类（发现分类错误时再修正）</h3><p>分类规则仍在“分类审核”中单独校准。本页不要求重复确认正确分类。</p><label class="field"><span>对话类别</span><select id="suggestion-intent">'+options+'</select></label></section>'+actualReplySection+'<section class="suggestion-calibration-section"><h3>2. FAQ 自动对照</h3>'+faqComparisonBanner+(faqTurnMarkup?'<ul class="faq-turn-comparison-list">'+faqTurnMarkup+'</ul>':'')+'<ul class="suggestion-faq-list">'+faq+'</ul>'+faqCandidateAction+'</section><details class="dev-audit-fold"><summary>其他机器信息</summary><p>处理动作：'+escapeHtml((p.actions||[]).join('；')||'未生成')+'</p><p>回复需要：'+(p.response_needed===true?'需要':p.response_needed===false?'无需':'待确认')+' · 承办：'+escapeHtml(suggestionAssigneeLabel(p.assignee))+'</p><strong>图片动作</strong><ul>'+images+'</ul><strong>待补信息</strong><ul>'+blocked+'</ul></details><section class="suggestion-calibration-section"><h3>3. 保存例外核对结果</h3><label class="field"><span>原因或补充说明（分类无误、FAQ判断清楚时可留空）</span><textarea id="suggestion-reason" rows="2" placeholder="仅在需要说明 FAQ 冲突或分类修正时填写。"></textarea></label><div class="suggestion-review-actions"><button class="button button-secondary" id="suggestion-correct" type="button">记录 FAQ / 分类问题并审核下一条</button><button class="button button-primary" id="suggestion-accept" type="button">已核对，保存并审核下一条</button></div></section></div>';
    var accept=document.getElementById('suggestion-accept'); if(accept) accept.addEventListener('click',function(){saveSuggestionReview('correct');});
    var correct=document.getElementById('suggestion-correct'); if(correct) correct.addEventListener('click',function(){saveSuggestionReview('corrected');});
    var candidateButton=document.getElementById('open-faq-candidate'); if(candidateButton) candidateButton.addEventListener('click',function(){openFaqCandidateFromSuggestion(item,p,confirmedReplies);});
  }

  async function saveSuggestionReview(decision) {
    var item=suggestionItems.find(function(x){return x.id===selectedSuggestionId;}); if(!item)return;
    var p=item.payload||{};var faqRefs=p.knowledge_refs||[];var intent=p.intent||item.intent||'unknown';var faqComparison=p.faq_comparison||{};
    var goldReplySequences=(p.gold_reply_sequences||[]).map(Number);var replyContext=p.reply_context||{};
    (replyContext.messages||[]).forEach(function(m){if(m.direction==='outbound'&&(m.relation==='explicit_reply'||m.reply_to_case===true))goldReplySequences.push(Number(m.sequence));});
    goldReplySequences=Array.from(new Set(goldReplySequences));
    var faqVerdicts={};Object.keys(faqComparison.verdicts||{}).forEach(function(id){faqVerdicts[id]=(faqComparison.verdicts[id]||{}).verdict||'uncertain';});
    var incompleteFaq=false;var hasFaqIssue=['no_hit','uncertain','partial','outdated','irrelevant'].indexOf(faqComparison.status)>=0;
    document.querySelectorAll('.suggestion-faq-verdict').forEach(function(select){faqVerdicts[select.dataset.faqId]=select.value;if(select.value==='unreviewed')incompleteFaq=true;if(select.value!==(goldReplySequences.length?'match':'valid'))hasFaqIssue=true;});
    if(incompleteFaq){toast('请先选择系统无法判断的项目','其他 FAQ 已按真实回复自动整理，不需要重复核对。');return;}
    var confirmedIntent=document.getElementById('suggestion-intent').value;var categoryChanged=confirmedIntent!==intent;
    if(decision==='correct'&&categoryChanged){toast('分类发现需要修正','请点“记录 FAQ / 分类问题并审核下一条”，并填写修正原因。');return;}
    var faqCoverage=!faqRefs.length?(faqComparison.status==='no_hit'?'no_hit':'not_applicable'):
      (Object.keys(faqVerdicts).some(function(id){return ['partial','outdated','irrelevant','uncertain'].indexOf(faqVerdicts[id])>=0;})?'exception_reviewed':'matched');
    var corrections={intent:confirmedIntent,faq_verdicts:faqVerdicts,faq_coverage:faqCoverage,
      faq_comparison_status:faqComparison.status||'unknown',gold_reply_sequences:goldReplySequences};
    var reason=document.getElementById('suggestion-reason').value.trim();
    if(decision==='corrected'&&categoryChanged&&!reason){toast('需要填写修正原因','请说明为什么改成当前选择的类别。');return;}
    if(decision==='corrected'&&!reason&&hasFaqIssue){reason=faqRefs.length?Object.keys(faqVerdicts).filter(function(id){return faqVerdicts[id]!== (goldReplySequences.length?'match':'valid');}).map(function(id){var faq=faqRefs.find(function(x,index){return String(x.id||('ref-'+index))===id;});return (faq&&faq.question||id)+'：'+({partial:'相关但答案不完整',outdated:'内容过期或与正确回复冲突',irrelevant:'与问题无关',uncertain:'无法判断'})[faqVerdicts[id]];}).join('；'):(faqComparison.note||'有真实回复但未命中已启用 FAQ；记录为待补充候选。');}
    if(decision==='corrected'&&!reason){toast('需要填写修正原因','请说明需要修正的分类或 FAQ 问题。');return;}
    try {
      await workflowPost('/api/suggestion/review',{suggestion_id:item.id,decision:decision,corrections:corrections,reason:reason});
      toast(decision==='correct'?'例外已核对':'已记录 FAQ / 分类问题','真实人工回复作为标准答案保留；FAQ 候选需单独确认后才会启用。');
      await loadSuggestions(true);
    } catch(error){toast('审核保存失败',error.message||'请重试');}
  }
  async function rebuildSuggestions() {
    var button=document.getElementById('rebuild-case-suggestions'); button.disabled=true;
    try {
      var result=await workflowPost('/api/workflow/rebuild',{});
      suggestionStatus='todo';
      await monitorWorkflowJob(true,result.job);
    } catch(error){toast('生成建议失败',error.message||'请稍后重试');}
    finally {button.disabled=false;}
  }

  async function monitorWorkflowJob(notify, initialJob) {
    if (workflowJobMonitoring) return;
    workflowJobMonitoring = true;
    var statusNode=document.getElementById('workflow-job-status');
    var button=document.getElementById('rebuild-case-suggestions');
    try {
      var job=initialJob;
      if (!job) {
        var initial=await fetch('/api/workflow/rebuild-status',{cache:'no-store'});
        if (!initial.ok) return;
        job=await initial.json();
      }
      if (!job || job.state==='idle') { if(statusNode)statusNode.hidden=true; return; }
      if (statusNode) statusNode.hidden=false;
      if (job.state==='running' && button) button.disabled=true;
      while (job.state==='running') {
        if (statusNode) statusNode.textContent='后台正在生成问题建议：'+Number(job.completed||0)+' / '+Number(job.total||0)+' 个问题；页面可继续浏览，云中客同步状态不变。';
        await new Promise(function(resolve){setTimeout(resolve,1000);});
        var response=await fetch('/api/workflow/rebuild-status',{cache:'no-store'});
        if(!response.ok) throw new Error('读取后台任务状态失败');
        job=await response.json();
      }
      if(job.state==='completed') {
        var report=job.report||{};
        if(statusNode) statusNode.textContent='对话整理完成：'+Number(report.cases_seen||job.completed||0)+' 个案例；真实回复已自动对照 FAQ，另整理候选 '+Number(report.faq_candidates_created||0)+' 条；仍需人工判断 '+Number(report.pending_uncertain||0)+' 条；本机处理，没有外部调用。';
        suggestionStatus='todo';
        await loadSuggestions(true);
        if(notify) toast('对话整理完成',Number(report.auto_matched_faq_cases||0)+' 条 FAQ 可支持、'+Number(report.auto_reviewed_partial||0)+' 条答案需补全、'+Number(report.auto_reviewed_conflict||0)+' 条冲突或无关结果已自动归档；待人工判断 '+Number(report.pending_uncertain||0)+' 条。');
      } else if(job.state==='failed') {
        if(statusNode) statusNode.textContent='后台生成失败：'+(job.error||'未知错误');
        if(notify) toast('生成建议失败',job.error||'请查看后台状态后重试');
      }
    } catch(error) {
      if(statusNode){statusNode.hidden=false;statusNode.textContent='后台任务状态读取失败：'+(error.message||'请刷新页面查看');}
      if(notify) toast('任务状态读取失败',error.message||'请刷新页面查看');
    } finally {
      workflowJobMonitoring=false;
      if(button) button.disabled=false;
    }
  }

  async function loadDevelopmentAuditSummary() {
    try {
      var response = await fetch('/api/dev-audit/summary', { cache: 'no-store' });
      if (!response.ok) throw new Error('开发标注统计读取失败');
      var summary = await response.json();
      var versionChanged = devAuditVersion && Number(summary.version || 0) !== devAuditVersion;
      devAuditVersion = Number(summary.version || 0);
      document.getElementById('dev-audit-version').textContent = String(summary.version || '—');
      document.getElementById('dev-audit-total').textContent = String(summary.total || 0);
      document.getElementById('dev-audit-reviewed').textContent = String(summary.reviewed || 0);
      document.getElementById('dev-audit-todo').textContent = String(summary.todo || 0);
      document.getElementById('dev-audit-tab-todo').textContent = String(summary.todo || 0);
      document.getElementById('dev-audit-tab-audited').textContent = String(summary.reviewed || 0);
      document.getElementById('dev-audit-accuracy').textContent = summary.category_accuracy == null ? '—' : Math.round(Number(summary.category_accuracy) * 100) + '%';
      var labelStats = summary.labels || {};
      var labelRows = Object.keys(labelStats).sort().map(function (key) {
        var value = labelStats[key];
        return '<tr><td>' + escapeHtml(devAuditLabels[key] || key) + '</td><td>' + Number(value.support || 0) + '</td><td>' + (value.precision == null ? '—' : Math.round(Number(value.precision) * 100) + '%') + '</td><td>' + (value.recall == null ? '—' : Math.round(Number(value.recall) * 100) + '%') + '</td></tr>';
      }).join('');
      var confusionRows = [];
      Object.keys(summary.confusion || {}).forEach(function (predicted) {
        Object.keys(summary.confusion[predicted] || {}).forEach(function (actual) {
          confusionRows.push('<tr><td>' + escapeHtml(devAuditLabels[predicted] || predicted) + '</td><td>' + escapeHtml(devAuditLabels[actual] || actual) + '</td><td>' + Number(summary.confusion[predicted][actual] || 0) + '</td></tr>');
        });
      });
      document.getElementById('dev-audit-report').innerHTML = Number(summary.reviewed || 0) ?
        '<p>当前规则 v' + Number(summary.version || 0) + '：人工及历史复核 ' + Number(summary.manual_reviewed || 0) + ' 条，真人回复自动确认 ' + Number(summary.automatic_reviewed || 0) + ' 条；准确率对照 ' + Number(summary.comparison_count || 0) + ' 条非自动标签。</p><p>' + escapeHtml(summary.sampling_note || '以下指标只基于已标注样本。') + '</p>' +
        '<h3>逐标签指标</h3><div class="dev-audit-table-wrap"><table><thead><tr><th>标签</th><th>人工正例</th><th>精确率</th><th>召回率</th></tr></thead><tbody>' + labelRows + '</tbody></table></div>' +
        '<h3>主类混淆（机器分类 → 人工分类）</h3><div class="dev-audit-table-wrap"><table><thead><tr><th>机器分类</th><th>人工分类</th><th>条数</th></tr></thead><tbody>' + (confusionRows.join('') || '<tr><td colspan="3">暂无数据</td></tr>') + '</tbody></table></div>' :
        '完成人工审核后显示逐标签指标和分类混淆。';
      if (versionChanged && activeView === 'dev-audit') loadDevelopmentAudits(true);
    } catch (error) { document.getElementById('dev-audit-list-count').textContent = error.message || '统计读取失败'; }
  }

  function developmentAuditParams(reset) {
    var params = new URLSearchParams({ limit: '50', status: document.getElementById('dev-audit-status').value });
    var kind = document.getElementById('dev-audit-kind').value;
    var query = document.getElementById('dev-audit-search').value.trim();
    if (kind) params.set('kind', kind);
    if (query) params.set('q', query);
    if (document.getElementById('dev-audit-differences').checked) params.set('differences', '1');
    if (!reset && devAuditBefore) params.set('before', String(devAuditBefore));
    return params;
  }

  function updateDevelopmentAuditControls() {
    var status = document.getElementById('dev-audit-status').value;
    document.querySelectorAll('[data-dev-audit-status]').forEach(function (button) {
      button.setAttribute('aria-pressed', String(button.dataset.devAuditStatus === status));
      button.disabled = devAuditSaving;
    });
    ['dev-audit-kind', 'dev-audit-search', 'dev-audit-refresh'].forEach(function (id) {
      document.getElementById(id).disabled = devAuditSaving;
    });
    document.getElementById('dev-audit-list').setAttribute('aria-busy', String(devAuditLoading));
    document.getElementById('dev-audit-load-more').disabled = devAuditLoading || devAuditSaving || !devAuditHasMore;
    var saveButton = document.getElementById('dev-audit-save');
    if (saveButton) {
      saveButton.disabled = devAuditSaving || devAuditLoading || devAuditDetailLoading || !devAuditDetail;
      var correction = document.getElementById('dev-audit-correction');
      saveButton.textContent = devAuditSaving ? '正在保存…' : correction && !correction.hidden ? (status === 'todo' ? '保存修正并审核下一条' : '保存分类修正') : (status === 'todo' ? '分类正确，保存并审核下一条' : '确认分类正确');
    }
    var skipButton = document.getElementById('dev-audit-skip');
    if (skipButton) skipButton.disabled = devAuditSaving || devAuditLoading;
    document.getElementById('dev-audit-detail').classList.toggle('is-saving', devAuditSaving);
    document.getElementById('dev-audit-detail').classList.toggle('is-loading', devAuditDetailLoading);
    document.getElementById('dev-audit-detail').setAttribute('aria-busy', String(devAuditDetailLoading));
  }

  function updateDevelopmentAuditPageState() {
    document.getElementById('dev-audit-list-count').textContent = devAuditItems.length + ' / ' + devAuditTotal + ' 条 · 当前规则 v' + devAuditVersion;
    document.getElementById('dev-audit-page-state').textContent = devAuditLoading ? '正在读取样本…' : devAuditHasMore ? '已加载 ' + devAuditItems.length + ' 条' : '当前筛选已加载完';
    updateDevelopmentAuditControls();
  }

  async function loadDevelopmentAudits(reset, selectAppended) {
    if (devAuditSaving || (!reset && (devAuditLoading || !devAuditHasMore))) return null;
    var requestId = ++devAuditRequestId;
    if (reset) invalidateDevelopmentAuditDetails();
    var filterKey = developmentAuditParams(true).toString();
    var filterChanged = filterKey !== devAuditListKey;
    var params = developmentAuditParams(!!reset);
    var loadError = '';
    devAuditLoading = true;
    updateDevelopmentAuditPageState();
    if (!devAuditLoaded) renderDevelopmentAuditList();
    try {
      var response = await fetch('/api/dev-audit?' + params.toString(), { cache: 'no-store' });
      if (!response.ok) throw new Error('开发审核样本读取失败');
      var payload = await response.json();
      if (requestId !== devAuditRequestId) return;
      var incoming = (payload.messages || []).map(function (item) {
        var message = item.message || {};
        return { sequence: Number(item.sequence), message: message, analysis: item.analysis || null, audit: item.audit || null };
      });
      if (reset) devAuditItems = incoming;
      else {
        var existing = new Set(devAuditItems.map(function (item) { return item.sequence; }));
        devAuditItems = devAuditItems.concat(incoming.filter(function (item) { return !existing.has(item.sequence); }));
      }
      devAuditListKey = filterKey;
      devAuditTotal = Number(payload.total || 0);
      devAuditVersion = Number(payload.version || devAuditVersion || 0);
      devAuditBefore = Number(payload.next_before || 0);
      devAuditHasMore = !!payload.has_more;
      devAuditLoaded = true;
      var nextSequence = devAuditSelectedSequence;
      if (selectAppended && !reset) nextSequence = incoming.length ? incoming[0].sequence : devAuditItems.length ? devAuditItems[devAuditItems.length - 1].sequence : 0;
      else if (filterChanged || !devAuditItems.some(function (item) { return item.sequence === nextSequence; })) nextSequence = devAuditItems.length ? devAuditItems[0].sequence : 0;
      if (reset || nextSequence !== devAuditSelectedSequence) {
        devAuditDetail = null;
        devAuditDetailRequestId += 1;
      }
      devAuditSelectedSequence = nextSequence;
      if (filterChanged) document.getElementById('dev-audit-list').scrollTop = 0;
      renderDevelopmentAuditList();
      if (activeView === 'dev-audit' && devAuditSelectedSequence && !devAuditDetail) loadDevelopmentAuditDetail(devAuditSelectedSequence);
      if (activeView === 'dev-audit' && !devAuditSelectedSequence && !devAuditItems.length) renderDevelopmentAuditEmptyDetail();
      return payload;
    } catch (error) {
      if (requestId === devAuditRequestId) {
        loadError = '样本读取失败，请点击刷新重试';
        toast('开发审核列表读取失败', error.message || '请重试');
      }
      return null;
    } finally {
      if (requestId === devAuditRequestId) {
        devAuditLoading = false;
        updateDevelopmentAuditPageState();
        if (loadError) document.getElementById('dev-audit-page-state').textContent = loadError;
        if (!devAuditItems.length) renderDevelopmentAuditList();
      }
    }
  }

  function renderDevelopmentAuditList() {
    var list = document.getElementById('dev-audit-list');
    var scrollTop = list.scrollTop;
    var status = document.getElementById('dev-audit-status').value;
    if (!devAuditItems.length) {
      list.innerHTML = '<div class="queue-empty"><b>' + (devAuditLoading ? '正在读取审核样本…' : devAuditHasMore ? '还有较早的未审核样本' : devAuditLoaded && status === 'todo' ? '当前筛选的样本已全部审核' : '没有匹配的消息') + '</b>可切换“已审核”回看，或调整筛选条件。</div>';
      list.scrollTop = scrollTop;
      return;
    }
    list.innerHTML = devAuditItems.map(function (item) {
      var msg = item.message || {};
      var analysis = item.analysis || {};
      var label = devAuditLabels[analysis.primary] || (analysis.primary ? analysis.primary : '待分析');
      var stamp = msg.timestamp ? new Date(msg.timestamp) : null;
      var time = stamp && !Number.isNaN(stamp.getTime()) ? stamp.toLocaleString('zh-CN', { hour12: false }) : '';
      var audited = !!item.audit;
      var active = Number(item.sequence) === Number(devAuditSelectedSequence);
      return '<article class="message-row dev-audit-row ' + (active ? 'selected' : '') + '" data-dev-audit-sequence="' + Number(item.sequence) + '" tabindex="0" role="button" aria-pressed="' + String(active) + '">' +
        '<div class="row-type-icon type-' + escapeHtml(analysis.primary || 'unknown') + '">' + messageIcon(analysis.primary || 'unknown') + '</div><div class="row-main"><div class="row-heading"><strong>' + escapeHtml(msg.sender || '未知成员') + '</strong><span class="row-time">' + escapeHtml(time) + '</span></div><div class="row-meta">' + escapeHtml(msg.group || '未知群') + ' · 消息 #' + Number(item.sequence) + '</div><div class="row-preview">' + escapeHtml(msg.content || '[无文字内容]') + '</div><div class="row-bottom"><span class="tag tag-' + escapeHtml(analysis.primary || 'unknown') + '">' + escapeHtml(label) + '</span>' + (audited ? '<span class="dev-audit-mark reviewed">已审核</span>' : '<span class="dev-audit-mark">未审核</span>') + (analysis.primary ? '' : '<span class="case-count">等待初步分类</span>') + '</div></div></article>';
    }).join('');
    list.scrollTop = scrollTop;
  }

  function updateDevelopmentAuditSelection() {
    var list = document.getElementById('dev-audit-list');
    list.querySelectorAll('.dev-audit-row.selected').forEach(function (row) {
      row.classList.remove('selected');
      row.setAttribute('aria-pressed', 'false');
    });
    var selected = list.querySelector('[data-dev-audit-sequence="' + Number(devAuditSelectedSequence) + '"]');
    if (selected) { selected.classList.add('selected'); selected.setAttribute('aria-pressed', 'true'); }
  }

  function invalidateDevelopmentAuditDetails() {
    devAuditDetailCache.clear();
    devAuditDetailCacheEpoch += 1;
    window.clearTimeout(devAuditPrefetchTimer);
  }

  function cachedDevelopmentAuditDetail(sequence) {
    var entry = devAuditDetailCache.get(sequence);
    if (!entry) return null;
    if (entry.payload.version !== devAuditVersion || Date.now() - entry.loadedAt > 30000) {
      devAuditDetailCache.delete(sequence);
      return null;
    }
    devAuditDetailCache.delete(sequence);
    devAuditDetailCache.set(sequence, entry);
    return entry.payload;
  }

  function rememberDevelopmentAuditDetail(payload) {
    var sequence = Number((payload.message || {}).sequence);
    devAuditDetailCache.delete(sequence);
    devAuditDetailCache.set(sequence, { payload: payload, loadedAt: Date.now() });
    while (devAuditDetailCache.size > 80) devAuditDetailCache.delete(devAuditDetailCache.keys().next().value);
  }

  function requestDevelopmentAuditDetail(sequence) {
    var cached = cachedDevelopmentAuditDetail(sequence);
    if (cached) return Promise.resolve(cached);
    var existing = devAuditDetailPending.get(sequence);
    if (existing && existing.epoch === devAuditDetailCacheEpoch && !existing.controller.signal.aborted) return existing.promise;
    if (existing) existing.controller.abort();
    var entry = { controller: new AbortController(), epoch: devAuditDetailCacheEpoch, promise: null };
    entry.promise = (async function () {
      var response = await fetch('/api/dev-audit/detail?sequence=' + encodeURIComponent(sequence), { cache: 'no-store', signal: entry.controller.signal });
      var payload = await response.json();
      if (!response.ok) throw new Error(payload.error || '消息详情读取失败');
      if (entry.epoch === devAuditDetailCacheEpoch && !entry.controller.signal.aborted) rememberDevelopmentAuditDetail(payload);
      return payload;
    })().finally(function () {
      if (devAuditDetailPending.get(sequence) === entry) devAuditDetailPending.delete(sequence);
    });
    devAuditDetailPending.set(sequence, entry);
    return entry.promise;
  }

  function prefetchNextDevelopmentAudit(sequence) {
    window.clearTimeout(devAuditPrefetchTimer);
    devAuditPrefetchTimer = window.setTimeout(function () {
      if (activeView !== 'dev-audit' || devAuditSelectedSequence !== sequence || devAuditSaving) return;
      var index = devAuditItems.findIndex(function (item) { return item.sequence === sequence; });
      var next = devAuditItems[index + 1];
      if (index >= 0 && next) requestDevelopmentAuditDetail(next.sequence).catch(function () {});
    }, 250);
  }

  function keepDevAuditSelectionVisible() {
    var list = document.getElementById('dev-audit-list');
    var selected = list && list.querySelector('.dev-audit-row.selected');
    if (!list || !selected) return;
    var listRect = list.getBoundingClientRect();
    var rowRect = selected.getBoundingClientRect();
    if (rowRect.top < listRect.top) list.scrollTop -= listRect.top - rowRect.top;
    else if (rowRect.bottom > listRect.bottom) list.scrollTop += rowRect.bottom - listRect.bottom;
  }

  function renderDevelopmentAuditEmptyDetail() {
    devAuditDetailLoading = false;
    var isTodo = document.getElementById('dev-audit-status').value === 'todo';
    document.getElementById('dev-audit-detail').innerHTML = '<div class="detail-placeholder"><div><div class="empty-symbol">✓</div><strong>' + (isTodo ? '当前筛选的样本已审核完' : '没有匹配的审核样本') + '</strong><span>可以切换“已审核”回看修改，或调整分类和搜索条件。</span></div></div>';
    updateDevelopmentAuditControls();
  }

  async function loadDevelopmentAuditDetail(sequence) {
    if (devAuditSaving) return;
    if (sequence === devAuditSelectedSequence && devAuditDetail && !devAuditDetailLoading && devAuditDetailEpoch === devAuditDetailCacheEpoch) return;
    devAuditSelectedSequence = sequence;
    devAuditDetail = null;
    var requestId = ++devAuditDetailRequestId;
    var detailEpoch = devAuditDetailCacheEpoch;
    window.clearTimeout(devAuditPrefetchTimer);
    devAuditDetailPending.forEach(function (entry, pendingSequence) { if (pendingSequence !== sequence) entry.controller.abort(); });
    updateDevelopmentAuditSelection();
    var cached = cachedDevelopmentAuditDetail(sequence);
    if (cached) {
      devAuditDetailLoading = false;
      devAuditDetail = cached;
      devAuditDetailEpoch = detailEpoch;
      renderDevelopmentAuditDetail();
      prefetchNextDevelopmentAudit(sequence);
      return;
    }
    devAuditDetailLoading = true;
    updateDevelopmentAuditControls();
    var loadingTimer = window.setTimeout(function () {
      if (requestId !== devAuditDetailRequestId) return;
      var row = devAuditItems.find(function (item) { return item.sequence === sequence; });
      document.getElementById('dev-audit-detail').innerHTML = '<div class="dev-audit-detail-body"><div class="detail-head"><strong>消息 #' + sequence + '</strong></div><div class="message-bubble">' + escapeHtml(row ? (row.message || {}).content || '[无文字内容]' : '正在读取消息') + '</div><div class="work-item-load-state"><p>正在读取分类、上下文与 FAQ 匹配…</p></div></div>';
    }, 120);
    try {
      var payload = await requestDevelopmentAuditDetail(sequence);
      if (requestId !== devAuditDetailRequestId) return;
      devAuditDetail = payload;
      devAuditDetailEpoch = detailEpoch;
      renderDevelopmentAuditDetail();
      prefetchNextDevelopmentAudit(sequence);
    } catch (error) {
      if (requestId === devAuditDetailRequestId && error.name !== 'AbortError') document.getElementById('dev-audit-detail').innerHTML = '<div class="queue-empty"><b>详情读取失败</b>' + escapeHtml(error.message || '请稍后重试') + '</div>';
    } finally {
      window.clearTimeout(loadingTimer);
      if (requestId === devAuditDetailRequestId) { devAuditDetailLoading = false; updateDevelopmentAuditControls(); }
    }
  }

  function renderDevelopmentAuditDetail() {
    var payload = devAuditDetail;
    if (!payload) return;
    var msg = payload.message || {};
    var analysis = payload.analysis || {};
    var audit = payload.audit || {};
    var options = devAuditLabels;
    var predicted = analysis.primary || 'unknown';
    var actual = audit.actual_primary || predicted;
    var predictedLabelSet = new Set([...(analysis.labels || []), predicted]);
    var actualLabelSet = new Set(audit.revision ? [...(audit.actual_labels || []), actual] : Array.from(predictedLabelSet));
    var correctionMode = !!audit.revision && (actual !== predicted || Array.from(predictedLabelSet).some(function (key) { return !actualLabelSet.has(key); }) || Array.from(actualLabelSet).some(function (key) { return !predictedLabelSet.has(key); }));
    var media = (msg.media || []).map(function (item, index) {
      if (item.state === 'stored') return '<a class="dev-audit-image" href="/api/media/' + Number(item.id) + '" target="_blank" rel="noopener noreferrer"><img src="/api/media/' + Number(item.id) + '" loading="lazy" alt="消息图片 ' + (index + 1) + '"><span>本机图片 ' + (index + 1) + '</span></a>';
      return '<div class="dev-audit-media-state">' + escapeHtml(item.state === 'failed' ? '图片归档失败：' + (item.error || '未知原因') : '图片仍在接收或归档') + '</div>';
    }).join('');
    var context = payload.context || [];
    var contextHtml = context.length ? '<div class="dev-audit-context">' + context.map(function (item) {
      return '<div class="dev-context-line ' + (item.same_sender ? 'same-sender' : '') + '"><span>' + (item.direction === 'outbound' ? '群内回复' : '上下文') + '</span><p>' + escapeHtml(item.content || '') + '</p></div>';
    }).join('') + '</div>' : '<p class="analysis-context">没有可关联的同群上下文。</p>';
    var categoryOptions = '<option value="">选择人工确认分类</option>' + Object.keys(options).map(function (key) {
      return '<option value="' + key + '"' + (actual === key ? ' selected' : '') + '>' + options[key] + '</option>';
    }).join('');
    var actualLabelOptions = Object.keys(options).filter(function (key) { return key !== 'unknown'; }).map(function (key) {
      var checked = actualLabelSet.has(key) || key === actual;
      return '<label><input type="checkbox" value="' + key + '"' + (checked ? ' checked' : '') + (key === actual ? ' disabled' : '') + '>' + options[key] + '</label>';
    }).join('');
    var time = msg.timestamp ? new Date(msg.timestamp).toLocaleString('zh-CN', { hour12: false }) : '时间未知';
    var machineLabels = Array.from(predictedLabelSet).filter(function (key) { return key !== predicted; }).map(function (key) { return options[key] || key; });
    var evidenceHtml = '<p class="decision-reason">' + escapeHtml(analysis.reason || '暂无分类依据') + '</p>' + ((analysis.evidence || []).length ? '<ul class="analysis-evidence">' + analysis.evidence.map(function (value) { return '<li>' + escapeHtml(value) + '</li>'; }).join('') + '</ul>' : '') + ((analysis.uncertainty || []).length ? '<p class="analysis-context">待确认：' + escapeHtml(analysis.uncertainty.join('；')) + '</p>' : '');
    document.getElementById('dev-audit-detail').innerHTML = '<div class="dev-audit-detail-body"><div class="detail-head"><div><div class="detail-kind"><span class="tag tag-' + escapeHtml(predicted) + '">机器分类：' + escapeHtml(options[predicted] || predicted) + '</span><span class="detail-title">开发样本 #' + Number(msg.sequence) + '</span></div><div class="detail-id">规则版本 ' + Number(payload.version || 0) + (audit.carried ? ' · 人工标签来自 v' + Number(audit.rule_version) : '') + ' · ' + escapeHtml(msg.source_id || '来源未提供消息 ID') + '</div></div><span class="detail-priority">' + (audit.revision ? (audit.carried ? '历史已审核 · 本轮可复验' : '已审核 · 可修改') : '未审核') + '</span></div>' +
      '<div class="detail-context"><div class="person-avatar">' + escapeHtml(String(msg.sender || '群').slice(0, 1)) + '</div><div class="context-main"><strong>' + escapeHtml(msg.sender || '未知成员') + '</strong><small>' + escapeHtml(msg.group || '未知群') + '</small></div><span class="context-time">' + escapeHtml(time) + '</span></div>' +
      '<div class="message-bubble">' + escapeHtml(msg.content || '[无文字内容]') + '</div>' + (media ? '<div class="dev-audit-media">' + media + '</div>' : '') +
      '<div class="dev-audit-prediction"><strong>承办角色：' + escapeHtml(analysis.assignee_name || '人工审核') + '</strong><p>群回复：' + (analysis.response_needed === false ? '无需回复（关联图片任务仍保留）' : analysis.response_needed === true ? '需要处理回复' : '待人工核实') + '</p><p>机器附加分类：' + escapeHtml(machineLabels.length ? machineLabels.join('、') : '无') + '</p></div>' +
      ((analysis.features && analysis.features.image_vision_results || []).length ? '<div class="dev-audit-prediction"><strong>图片视觉初筛 · 需人工核验</strong>' + analysis.features.image_vision_results.map(function (v) { return '<p>' + escapeHtml(({order_screenshot:'订单截图',product_feedback:'商品反馈图',product_reference:'商品参考图',emoji:'表情包',ad:'广告图',other:'其他图片',uncertain:'待判断'})[v.image_kind] || v.image_kind) + ' · 置信度 ' + Math.round(Number(v.confidence || 0)*100) + '% · ' + escapeHtml((v.evidence || []).join('；')) + '</p>'; }).join('') + '</div>' : '') +
      '<details class="dev-audit-fold"' + (context.length ? ' open' : '') + '><summary>同群客户上下文 · ' + context.length + ' 条</summary><div class="dev-audit-fold-body">' + contextHtml + '</div></details>' +
      '<div class="decision-card dev-audit-form-card"><div class="decision-top"><span class="decision-label">' + (correctionMode ? '检查或修改人工分类' : '默认采用机器分类') + '</span><button class="button button-secondary" id="dev-audit-correction-toggle" type="button">' + (correctionMode ? '放弃修正，按机器分类' : '分类不正确？修改分类') + '</button></div><div class="decision-body"><p class="dev-audit-confirm-copy" id="dev-audit-confirm-copy"' + (correctionMode ? ' hidden' : '') + '>将确认主分类“' + escapeHtml(options[predicted] || predicted) + '”及机器附加分类。若判断正确，直接点击下方保存即可。</p><div id="dev-audit-correction"' + (correctionMode ? '' : ' hidden') + '><label class="analysis-field">正确主分类<select id="dev-actual-primary">' + categoryOptions + '</select></label><details class="dev-audit-fold"' + (actualLabelSet.size > 1 ? ' open' : '') + '><summary>补充或调整同时适用的分类（可选）</summary><div class="dev-audit-fold-body"><div id="dev-actual-labels" class="analysis-checks dev-audit-labels">' + actualLabelOptions + '</div></div></details><label class="analysis-field">修正原因（必填）<textarea id="dev-audit-note" rows="2" placeholder="简要说明机器分类哪里不对，例如需结合上下文判断">' + escapeHtml(audit.note || '') + '</textarea></label></div></div></div>' +
      '<details class="dev-audit-fold"><summary>查看机器分类依据</summary><div class="dev-audit-fold-body">' + evidenceHtml + '</div></details></div>' +
      '<div class="dev-audit-actionbar"><small id="dev-audit-action-hint">' + (correctionMode ? '已进入修正模式，提交时需要填写原因。' : audit.revision ? '已审核 · 第 ' + Number(audit.revision) + ' 次保存。' : '若分类正确，无需改动即可保存。') + '</small><button class="button button-secondary" id="dev-audit-skip" type="button">' + (document.getElementById('dev-audit-status').value === 'todo' ? '稍后审核' : '下一条') + '</button><button class="button button-primary" id="dev-audit-save" type="button">' + (correctionMode ? (document.getElementById('dev-audit-status').value === 'todo' ? '保存修正并审核下一条' : '保存分类修正') : (document.getElementById('dev-audit-status').value === 'todo' ? '分类正确，保存并审核下一条' : '确认分类正确')) + '</button></div>';
    var correctionPanel = document.getElementById('dev-audit-correction');
    var correctionToggle = document.getElementById('dev-audit-correction-toggle');
    var saveButton = document.getElementById('dev-audit-save');
    var correctionHint = document.getElementById('dev-audit-action-hint');
    var confirmCopy = document.getElementById('dev-audit-confirm-copy');
    function setCorrectionMode(enabled) {
      correctionPanel.hidden = !enabled;
      confirmCopy.hidden = enabled;
      correctionToggle.textContent = enabled ? '放弃修正，按机器分类' : '分类不正确？修改分类';
      correctionHint.textContent = enabled ? '已进入修正模式，提交时需要填写原因。' : '将确认机器分类正确。';
      saveButton.textContent = enabled ? (document.getElementById('dev-audit-status').value === 'todo' ? '保存修正并审核下一条' : '保存分类修正') : (document.getElementById('dev-audit-status').value === 'todo' ? '分类正确，保存并审核下一条' : '确认分类正确');
    }
    correctionToggle.addEventListener('click', function () { setCorrectionMode(correctionPanel.hidden); });
    var actualPrimarySelect = document.getElementById('dev-actual-primary');
    actualPrimarySelect.addEventListener('change', function () {
      document.querySelectorAll('#dev-actual-labels input:disabled').forEach(function (checkbox) {
        checkbox.disabled = false;
        checkbox.checked = false;
      });
      var primaryCheckbox = document.querySelector('#dev-actual-labels input[value="' + this.value + '"]');
      if (primaryCheckbox) { primaryCheckbox.checked = true; primaryCheckbox.disabled = true; }
    });
    document.getElementById('dev-audit-skip').addEventListener('click', skipDevelopmentAudit);
    document.getElementById('dev-audit-save').addEventListener('click', saveDevelopmentAudit);
    updateDevelopmentAuditControls();
  }

  async function continueDevelopmentAudit(nextIndex, afterSave) {
    if (devAuditItems[nextIndex]) {
      await loadDevelopmentAuditDetail(devAuditItems[nextIndex].sequence);
    } else if (devAuditHasMore) {
      devAuditDetail = null;
      document.getElementById('dev-audit-detail').innerHTML = '<div class="work-item-load-state"><strong>正在加载下一批样本</strong><p>已审核结果保存在“已审核”，未审核列表会继续向后加载。</p></div>';
      var page = await loadDevelopmentAudits(false, true);
      if (!page) {
        document.getElementById('dev-audit-detail').innerHTML = '<div class="queue-empty"><b>' + (afterSave ? '标注已保存，下一批读取失败' : '下一批样本读取失败') + '</b>点击左侧“查看较早消息”或上方“刷新列表”继续。</div>';
        return;
      }
    } else if (devAuditItems.length) {
      await loadDevelopmentAuditDetail(devAuditItems[devAuditItems.length - 1].sequence);
    } else {
      devAuditSelectedSequence = 0;
      renderDevelopmentAuditEmptyDetail();
    }
    keepDevAuditSelectionVisible();
  }

  async function skipDevelopmentAudit() {
    if (devAuditSaving || devAuditLoading) return;
    var index = devAuditItems.findIndex(function (item) { return item.sequence === devAuditSelectedSequence; });
    if (index < 0) return;
    if (index + 1 >= devAuditItems.length && !devAuditHasMore) {
      toast('已到当前列表末尾', '本条仍保留待审核，可从左侧选择其他样本。');
      return;
    }
    await continueDevelopmentAudit(index + 1);
  }

  async function saveDevelopmentAudit() {
    if (devAuditSaving || devAuditLoading || !devAuditDetail) return;
    var correctionMode = !document.getElementById('dev-audit-correction').hidden;
    var analysis = devAuditDetail.analysis || {};
    var actual = correctionMode ? document.getElementById('dev-actual-primary').value : analysis.primary || 'unknown';
    var actualLabels = correctionMode ? Array.from(document.querySelectorAll('#dev-actual-labels input:checked')).map(function (input) { return input.value; }) : Array.from(new Set([...(analysis.labels || []), actual]));
    if (actual && actualLabels.indexOf(actual) === -1) actualLabels.push(actual);
    if (!actual) { toast('审核未保存', '请选择正确的主分类。'); return; }
    var savedSequence = devAuditSelectedSequence;
    var savedRequestId = devAuditRequestId;
    var savedStatus = document.getElementById('dev-audit-status').value;
    var savedVersion = Number(devAuditDetail.version || devAuditVersion);
    var note = correctionMode ? document.getElementById('dev-audit-note').value.trim() : '';
    if (correctionMode && !note) { toast('修正未保存', '请简要说明机器分类哪里不正确。'); return; }
    var payload;
    devAuditSaving = true;
    updateDevelopmentAuditControls();
    try {
      var response = await fetch('/api/dev-audit', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sequence: savedSequence, version: savedVersion, actual_primary: actual,
          actual_labels: actualLabels, note: note }) });
      payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.error || '开发标注保存失败');
    } catch (error) {
      toast('分类审核未保存', error.message || '请检查填写内容后重试');
      return;
    } finally {
      devAuditSaving = false;
      updateDevelopmentAuditControls();
    }
    loadDevelopmentAuditSummary();
    devAuditDetailCache.delete(savedSequence);
    if (devAuditDetail && devAuditSelectedSequence === savedSequence) {
      devAuditDetail.audit = payload.audit;
      rememberDevelopmentAuditDetail(devAuditDetail);
    }
    if (savedRequestId !== devAuditRequestId) {
      toast('分类审核已保存', '可在“已审核”中查看。');
      return;
    }
    var row = devAuditItems.find(function (item) { return item.sequence === savedSequence; });
    if (row) row.audit = payload.audit;
    var matchesMachine = actual === (analysis.primary || 'unknown') && Array.from(new Set([...(analysis.labels || []), analysis.primary || 'unknown'])).sort().join('|') === Array.from(new Set(actualLabels)).sort().join('|');
    if (savedStatus === 'todo' || (document.getElementById('dev-audit-differences').checked && matchesMachine)) {
      var currentIndex = devAuditItems.findIndex(function (item) { return item.sequence === savedSequence; });
      if (currentIndex >= 0) {
        devAuditItems.splice(currentIndex, 1);
        devAuditTotal = Math.max(0, devAuditTotal - 1);
      }
      devAuditSelectedSequence = 0;
      devAuditDetail = null;
      devAuditDetailRequestId += 1;
      updateDevelopmentAuditPageState();
      renderDevelopmentAuditList();
      toast('分类已审核', savedStatus === 'todo' ? '已移至“已审核”，继续下一条。' : '已与人工结论一致，移出差异列表。');
      await continueDevelopmentAudit(Math.max(currentIndex, 0), true);
    } else if (devAuditDetail && devAuditSelectedSequence === savedSequence) {
      devAuditDetail.audit = payload.audit;
      renderDevelopmentAuditList();
      var detailBody = document.querySelector('.dev-audit-detail-body');
      var detailScroll = detailBody ? detailBody.scrollTop : 0;
      renderDevelopmentAuditDetail();
      detailBody = document.querySelector('.dev-audit-detail-body');
      if (detailBody) detailBody.scrollTop = detailScroll;
      toast('分类已审核', '已更新分类结论，可继续修改或查看下一条。');
    }
  }

  async function loadReplies(reset) {
    if (repliesLoading || (!reset && !replyHasOlder)) return;
    repliesLoading = true;
    try {
      var url = '/api/replies?limit=100' + (reset ? '' : '&before=' + encodeURIComponent(replyBefore));
      var response = await fetch(url, { cache: 'no-store' });
      if (!response.ok) throw new Error('本机已发消息接口不可用');
      var payload = await response.json();
      replies = reset ? (payload.messages || []) : replies.concat(payload.messages || []);
      replyBefore = Number(payload.next_before || 0);
      replyHasOlder = !!payload.has_more;
      replyKnownCount = Number(payload.total || 0);
      if (!replies.some(function (item) { return item.sequence === selectedReplySequence; })) selectedReplySequence = replies[0]?.sequence || 0;
      renderReplies();
    } catch (error) {
      document.getElementById('reply-count').textContent = '读取已发消息失败';
      toast('已发消息读取失败', error.message || '请稍后重试。');
    } finally {
      repliesLoading = false;
    }
  }

  function renderReplies() {
    var list = document.getElementById('reply-list');
    document.getElementById('reply-count').textContent = '本机已保存 ' + replyKnownCount + ' 条已发消息，当前加载 ' + replies.length + ' 条';
    document.getElementById('load-more-replies').hidden = !replyHasOlder;
    list.innerHTML = replies.length ? replies.map(function (item) {
      var stamp = item.timestamp ? new Date(item.timestamp) : null;
      var time = stamp && !Number.isNaN(stamp.getTime()) ? stamp.toLocaleString('zh-CN', { hour12: false }) : '时间未知';
      return '<article class="message-row ' + (item.sequence === selectedReplySequence ? 'selected' : '') + '" data-reply-sequence="' + item.sequence + '" tabindex="0" role="button" aria-label="查看已发消息">' +
        '<div class="row-type-icon type-closing">↩</div><div class="row-main"><div class="row-heading"><strong>' + escapeHtml(item.sender) + '</strong><span class="row-time">' + escapeHtml(time) + '</span></div><div class="row-meta">' + escapeHtml(item.group) + '</div><div class="row-preview">' + escapeHtml(item.content) + '</div><div class="row-bottom"><span class="tag tag-closing">已发消息</span>' + (item.revoked ? '<span class="tag">已撤回</span>' : '') + '<span class="row-status">' + escapeHtml(item.send_status === 'succeed' ? '来源标记成功' : item.send_status) + '</span></div></div></article>';
    }).join('') : '<div class="queue-empty"><b>暂无已发消息</b>已发记录由云中客历史接口提供。</div>';
    list.querySelectorAll('[data-reply-sequence]').forEach(function (row) {
      var select = function () { selectedReplySequence = Number(row.dataset.replySequence); renderReplies(); };
      row.addEventListener('click', select);
      row.addEventListener('keydown', function (event) { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); select(); } });
    });
    var item = replies.find(function (reply) { return reply.sequence === selectedReplySequence; });
    var detail = document.getElementById('reply-detail');
    if (!item) {
      detail.innerHTML = '<div class="detail-placeholder"><div><strong>选择一条已发消息</strong><span>这里只显示来源已发记录，不进行审核或再次发送。</span></div></div>';
      return;
    }
    var stamp = item.timestamp ? new Date(item.timestamp) : null;
    var time = stamp && !Number.isNaN(stamp.getTime()) ? stamp.toLocaleString('zh-CN', { hour12: false }) : '时间未知';
    var type = { text: '文字', image: '图片', video: '视频' }[item.content_type] || item.content_type;
    detail.innerHTML = '<div class="detail-head"><div><div class="detail-kind"><span class="tag tag-closing">已发消息</span>' + (item.revoked ? '<span class="tag">已撤回</span>' : '') + '<span class="detail-title">' + escapeHtml(type) + '</span></div><div class="detail-id">云中客消息 ID · ' + escapeHtml(item.source_id || '未提供') + '</div></div><span class="detail-priority">只读</span></div>' +
      '<div class="detail-context"><div class="person-avatar">↩</div><div class="context-main"><strong>' + escapeHtml(item.sender) + '</strong><small>' + escapeHtml(item.group) + '</small></div><span class="context-time">' + escapeHtml(time) + '</span></div>' +
      '<div class="message-bubble">' + escapeHtml(item.content) + '</div><div class="decision-card"><div class="decision-body"><p class="decision-reason">来源：' + escapeHtml(item.source === 'history' ? '云中客历史接口' : item.source) + '；发送状态：' + escapeHtml(item.send_status === 'succeed' ? '云中客标记成功' : item.send_status) + '。' + (item.is_ai === false ? '来源标记为非 AI 消息。' : '来源未明确标记是否由人工发送。') + '</p><p class="decision-reason">目前无法可靠关联到某一条收到的消息，不能据此自动判定“该条已回复”。图片和视频原件尚未在此页归档预览。</p></div></div>';
  }

  function messageIcon(kind) {
    return { feedback: '▧', service: '↗', warning: '!', mini: '▣', qa: '?', closing: '✓', burst: '≋', order_image: '▤', requirement: '＋', product_link: '↗', thanks: '♡', ordinary: '•', unknown: '…' }[kind] || '•';
  }

  function filteredMessages() {
    return queueLoaded ? queueItems : state.messages;
  }

  function renderQueue() {
    var list = document.getElementById('message-list');
    var messages = filteredMessages();
    var groupToggle = document.getElementById('group-conversations');
    var groupedView = !!(groupToggle && groupToggle.checked);
    if (!messages.some(function (message) { return message.id === selectedMessageId; })) selectedMessageId = messages[0]?.id;
    if (!messages.length) {
      var hasQueueFilters = activeFilter !== 'all' || document.getElementById('message-search').value.trim() || document.getElementById('filter-kind').value || document.getElementById('filter-group').value.trim() || document.getElementById('filter-start').value || document.getElementById('filter-end').value || document.getElementById('filter-status').value || document.getElementById('filter-work-status').value;
      list.innerHTML = !hasQueueFilters && liveStatus.sync_enabled === false
        ? '<div class="queue-empty"><b>消息同步已暂停</b>可在连接设置中按日期补读历史；恢复同步后接收新消息。</div>'
        : !hasQueueFilters && liveStatus.connected
        ? '<div class="queue-empty"><b>实时连接正常，等待新消息</b>收到当前配置规则的群消息后会显示在这里。</div>'
        : '<div class="queue-empty"><b>没有匹配的消息</b>试试其他筛选条件。</div>';
    } else {
      var entries = [];
      if (groupedView) {
        var groups = new Map();
        messages.forEach(function (message) {
          var conversationId = message.analysis && message.analysis.conversation_id;
          var key = conversationId ? 'conversation:' + conversationId : 'event:' + message.sequence;
          if (!groups.has(key)) groups.set(key, []);
          groups.get(key).push(message);
        });
        groups.forEach(function (members) {
          members.sort(function (a, b) { return String(a.timestamp || a.date || '').localeCompare(String(b.timestamp || b.date || '')) || Number(a.sequence || 0) - Number(b.sequence || 0); });
          var actionable = members.filter(function (item) { return !['closing', 'ordinary'].includes(item.kind); });
          entries.push({ message: actionable[actionable.length - 1] || members[members.length - 1], members: members });
        });
        entries.sort(function (a, b) { return Number(b.members[b.members.length - 1].sequence || 0) - Number(a.members[a.members.length - 1].sequence || 0); });
      } else {
        entries = messages.map(function (message) { return { message: message, members: [message] }; });
      }
      list.innerHTML = entries.map(function (entry) {
        var message = entry.message;
        var members = entry.members;
        var handlingStatus = message.work_item?.status || message.work_status || 'pending';
        var disagreement = message.work_item
          ? message.work_item.ai_state === 'done' && message.work_item.ai?.route && message.work_item.ai.route !== message.work_item.route
          : message.work_ai_state === 'done' && message.work_ai_route && message.work_ai_route !== message.kind;
        var statusText = ({ pending: '处理待审核', approved: '草稿已批准', rejected: '已驳回', handled: '已处理', needs_human: '转人工' })[handlingStatus] || '处理待审核';
        var statusClass = handlingStatus === 'pending' ? '' : handlingStatus;
        var latest = members[members.length - 1];
        var categories = Array.from(new Set(members.map(function (item) { return item.label; }).filter(Boolean)));
        var pendingCount = members.filter(function (item) { return (item.work_item?.status || item.work_status || 'pending') === 'pending'; }).length;
        var preview = groupedView && members.length > 1
          ? '最近消息：' + String(latest.content || '').slice(0, 150)
          : message.content;
        var groupBadge = groupedView ? '<span class="case-count">' + members.length + ' 条消息' + (pendingCount ? ' · ' + pendingCount + ' 条待审核' : '') + '</span>' : '';
        var categorySummary = groupedView && categories.length > 1
          ? '<span class="case-categories" title="会话涉及：' + escapeHtml(categories.join('、')) + '">会话涉及：' + escapeHtml(categories.join('、')) + '</span>' : '';
        var selected = members.some(function (item) { return item.id === selectedMessageId; });
        return '<article class="message-row ' + (selected ? 'selected' : '') + '" data-message-id="' + escapeHtml(message.id) + '" tabindex="0" role="button" aria-label="查看' + escapeHtml(message.sender) + '的' + (groupedView ? members.length + '条会话消息' : escapeHtml(message.label) + '消息') + '">' +
          '<div class="row-type-icon type-' + escapeHtml(message.kind) + '">' + messageIcon(message.kind) + '</div><div class="row-main"><div class="row-heading"><strong>' + escapeHtml(message.sender) + '</strong><span class="row-time">' + escapeHtml(latest.time || message.time) + '</span></div><div class="row-meta">' + escapeHtml(message.group) + (groupedView && members.length > 1 ? ' · 同一客户会话' : '') + '</div><div class="row-preview">' + escapeHtml(preview) + '</div><div class="row-bottom"><span class="tag tag-' + escapeHtml(message.kind) + '">' + escapeHtml(message.label) + '</span>' + (members.some(function(x){return x.revoked;}) ? '<span class="tag">含已撤回消息</span>' : '') + groupBadge + categorySummary + (disagreement ? '<span class="priority-label">◍ 判断分歧</span>' : '') + (message.priority === '优先' && handlingStatus === 'pending' ? '<span class="priority-label">● 优先</span>' : '') + '<span class="row-status ' + statusClass + '">' + statusText + '</span></div></div></article>';
      }).join('');
    }
    list.querySelectorAll('.message-row').forEach(function (row) {
      row.addEventListener('click', function () { selectedMessageId = row.dataset.messageId; renderQueue(); renderDetail(); });
      row.addEventListener('keydown', function (event) { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); selectedMessageId = row.dataset.messageId; renderQueue(); renderDetail(); } });
    });
    renderDetail();
    updateCounters();
  }

  function selectedMessage() {
    return filteredMessages().find(function (message) { return message.id === selectedMessageId; });
  }

  function workItemHtml(message) {
    var item = message.work_item;
    if (!item) return '';
    var rec = item.recommendation || {};
    var blockers = Array.isArray(rec.blockers) ? rec.blockers : [];
    var checks = Array.isArray(rec.checklist) ? rec.checklist : [];
    var evidence = Array.isArray(rec.evidence) ? rec.evidence : [];
    var faq = Array.isArray(rec.faq) ? rec.faq : [];
    var draft = item.edited_draft != null ? item.edited_draft : (rec.draft || '');
    var ai = item.ai || null;
    var disagreement = item.ai_state === 'done' && ai && ai.route !== item.route;
    var requiresDraft = !String(draft || '').trim() && !['closing', 'ordinary', 'order_image', 'requirement'].includes(item.route);
    var routeNames = { service: '售后问题', feedback: '晒单图库', warning: '撤回/风险内容', mini: '小程序撤回', burst: '刷屏撤回', order_image: '订单截图表', requirement: '产品需求表', product_link: '当天商品链接', thanks: '强提醒感谢', qa: '人工答疑', closing: '无需回复', ordinary: '普通交流', unknown: '待判断' };
    var internalRoute = ['feedback', 'order_image', 'requirement'].includes(item.route);
    var handledLabel = item.route === 'feedback' ? '记录人工核验（未入库）' : item.route === 'order_image' ? '记录待登记（未入表）' : item.route === 'requirement' ? '记录待登记（未入表）' : '标记已处理';
    return '<div class="decision-card work-item-card"><div class="decision-top"><span class="decision-label">处理待办 · ' + escapeHtml(routeNames[item.route] || item.route) + '</span><span class="decision-chip">' + escapeHtml(({ pending: '待审核', approved: '已批准草稿', rejected: '已驳回', handled: '已处理', needs_human: '转人工' })[item.status] || item.status) + '</span></div><div class="decision-body">' +
      '<p class="decision-reason"><strong>建议动作：</strong>' + escapeHtml(rec.action || '人工核验') + '</p>' +
      (item.stale ? '<p class="work-alert">配置或分类已变化，本条建议需重新核验。</p>' : '') +
      (disagreement ? '<p class="work-alert">AI 与规则分类不同：' + escapeHtml(routeNames[item.route] || item.route) + ' → ' + escapeHtml(routeNames[ai.route] || ai.route) + '。请先复核分类，再处理草稿。</p>' : '') +
      (blockers.length ? '<div class="work-alert"><strong>当前阻断：</strong>' + blockers.map(escapeHtml).join('；') + '</div>' : '') +
      (evidence.length ? '<strong class="analysis-caption">处理依据</strong><ul class="analysis-evidence">' + evidence.map(function (value) { return '<li>' + escapeHtml(value) + '</li>'; }).join('') + '</ul>' : '') +
      businessContextMarkup(rec.business_context) +
      (checks.length ? '<strong class="analysis-caption">人工核验清单</strong><ul class="analysis-evidence">' + checks.map(function (value) { return '<li>' + escapeHtml(value) + '</li>'; }).join('') + '</ul>' : '') +
      (faq.length ? '<strong class="analysis-caption">本地 FAQ / 处理话术候选（答案仅供核对）</strong><ul class="analysis-evidence">' + faq.map(function (value) { var faqStatus = value.enabled && value.approved ? '已审核启用' : value.approved ? '已审核，已停用' : '待审核'; var answer = String(value.answer || ''); return '<li><strong>' + escapeHtml(value.question) + '</strong>' + (value.group ? ' · ' + escapeHtml(value.group) : '') + ' · 匹配 ' + Math.round(Number(value.score || 0) * 100) + '% · ' + faqStatus + '<br>答案候选：' + escapeHtml(answer.slice(0, 220)) + (answer.length > 220 ? '…' : '') + '</li>'; }).join('') + '</ul>' : '') +
      (rec.conversation_id ? '<p class="decision-reason">同一客户上下文（' + Math.round(Number(rec.conversation_window_seconds || 1800) / 60) + ' 分钟）· ' + Number((rec.conversation_sequences || []).length) + ' 条关联消息</p>' : '') +
      (rec.destination_url ? '<p class="decision-reason">目标表：<a href="' + escapeHtml(rec.destination_url) + '" target="_blank" rel="noopener noreferrer">打开目标</a></p>' : '') +
      (rec.context_text ? '<strong class="analysis-caption">同一客户上下文（30 分钟内）</strong><p class="decision-reason">' + escapeHtml(rec.context_text) + '</p>' : '') +
      (!rec.context_text && rec.nearby_text && rec.nearby_text.length ? '<strong class="analysis-caption">同一客户相关消息</strong><p class="decision-reason">' + escapeHtml(rec.nearby_text.join(' / ')) + '</p>' : '') +
      (rec.evidence_media_ids && rec.evidence_media_ids.length ? '<strong class="analysis-caption">问题截图 · 供答疑核对，不登记订单图表</strong><p class="decision-reason">' + rec.evidence_media_ids.map(function (id) { return '<a href="/api/media/' + Number(id) + '" target="_blank" rel="noopener noreferrer">查看问题截图</a>'; }).join(' · ') + '</p>' : '') +
      (rec.attachment_tasks && rec.attachment_tasks.length ? '<strong class="analysis-caption">关联图片待办 · 与群回复分别处理</strong><ul class="analysis-evidence">' + rec.attachment_tasks.map(function (task) { return '<li>' + escapeHtml(task.action) + ' · <a href="/api/media/' + Number(task.media_id) + '" target="_blank" rel="noopener noreferrer">查看图片</a>' + (task.blockers && task.blockers.length ? '<br>待补：' + escapeHtml(task.blockers.join('；')) : '') + '</li>'; }).join('') + '</ul>' : '') +
      (rec.secondary_actions && rec.secondary_actions.length ? '<strong class="analysis-caption">同一上下文中的其他处理</strong><ul class="analysis-evidence">' + rec.secondary_actions.map(function (item) { return '<li><strong>' + escapeHtml(item.action) + '</strong>' + (item.draft ? '<br>' + escapeHtml(item.draft) : '') + (item.destination_url ? '<br><a href="' + escapeHtml(item.destination_url) + '" target="_blank" rel="noopener noreferrer">打开目标表</a>' : '') + (item.blockers && item.blockers.length ? '<br>待补：' + escapeHtml(item.blockers.join('；')) : '') + '</li>'; }).join('') + '</ul>' : '') +
      '<p class="decision-reason">承办角色：' + escapeHtml(rec.assignee_name || '人工审核') + '</p>' +
      (rec.human_rule_disagreement ? '<p class="analysis-context">当前沿用人工分类“' + escapeHtml(routeNames[item.route] || item.route) + '”；本轮规则建议“' + escapeHtml(routeNames[rec.machine_intent] || rec.machine_intent) + '”。' + (rec.requires_classification_review ? '请先针对当前规则重新复核分类。' : '本轮已复核，当前采用人工结论。') + '</p>' : '') +
      (rec.image_understanding && rec.image_understanding.length ? '<strong class="analysis-caption">图片视觉初筛 · 需人工核验</strong><ul class="analysis-evidence">' + rec.image_understanding.map(function (visual) { return '<li>' + escapeHtml(({order_screenshot:'订单截图',product_feedback:'商品反馈图',product_reference:'商品参考图',emoji:'表情包',ad:'广告图',other:'其他图片',uncertain:'待判断'})[visual.image_kind] || visual.image_kind) + ' · 置信度 ' + Math.round(Number(visual.confidence || 0)*100) + '% · ' + escapeHtml((visual.evidence || []).join('；')) + '</li>'; }).join('') + '</ul>' : '') +
      '<div class="work-ai"><strong>文字 AI 状态</strong><span>' + escapeHtml(item.ai_state === 'disabled' ? '文字模型已关闭；本条文字采用本地规则和 FAQ，图片按视觉初筛单独核验' : item.ai_state === 'done' && ai ? '建议类别：' + (routeNames[ai.route] || ai.route) + ' · ' + (ai.reason || '无说明') : item.ai_state === 'pending' ? '正在分析，稍后刷新' : item.ai_state === 'error' ? '调用失败：' + (item.ai_error || '未知错误') : (message.media || []).length ? '纯图片交由人工核验，文本模型未读取图片' : '该消息无需模型判断') + '</span>' +
      (ai && ai.draft ? '<p>模型草稿：' + escapeHtml(ai.draft) + '</p><button class="text-button" id="use-ai-draft" type="button">采用为待审草稿</button>' : '') +
      (item.ai_state === 'error' ? '<button class="text-button" id="retry-ai" type="button">重试分析</button>' : '') + '</div>' +
      '<label class="analysis-field">待审回复草稿<textarea id="work-draft" rows="4" placeholder="不需要群回复时留空">' + escapeHtml(draft) + '</textarea></label>' +
      '<label class="analysis-field">审核说明<textarea id="work-note" rows="2" placeholder="说明修改、驳回或已处理的原因"></textarea></label>' +
      '<div class="work-actions"><button class="button button-primary" id="work-approve" type="button"' + (blockers.length || item.stale || requiresDraft || internalRoute || (disagreement && message.status !== 'approved') ? ' disabled' : '') + '>' + (item.route === 'closing' || item.route === 'ordinary' ? '确认无需回复' : internalRoute ? '飞书登记未开放' : '批准待审草稿') + '</button><button class="button button-secondary" id="work-handled" type="button">' + handledLabel + '</button><button class="button button-secondary" id="work-human" type="button">转人工</button><button class="button reject-button" id="work-reject" type="button">驳回</button><button class="text-button" id="work-refresh" type="button">刷新建议</button></div>' +
      '<small>这里只保存审核结论，不会发送群消息或上传飞书。</small></div></div>';
  }

  function bindWorkItem(message) {
    if (!message.work_item) return;
    var refresh = document.getElementById('work-refresh');
    if (refresh) refresh.addEventListener('click', function () { refreshWorkItem(message); });
    var use = document.getElementById('use-ai-draft');
    if (use) use.addEventListener('click', function () { document.getElementById('work-draft').value = message.work_item.ai.draft || ''; });
    var retry = document.getElementById('retry-ai');
    if (retry) retry.addEventListener('click', async function () {
      try {
        var response = await fetch('/api/work-item/ai/retry', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sequence: message.sequence }) });
        if (!response.ok) throw new Error((await response.json()).error || '重试失败');
        await refreshWorkItem(message);
      } catch (error) { toast('模型重试失败', error.message || '请稍后重试'); }
    });
    [['work-approve', 'approved'], ['work-handled', 'handled'], ['work-human', 'needs_human'], ['work-reject', 'rejected']].forEach(function (entry) {
      var button = document.getElementById(entry[0]);
      if (button) button.addEventListener('click', function () { saveWorkDecision(message, entry[1]); });
    });
  }

  async function refreshWorkItem(message) {
    if (message.work_item_loading) return;
    message.work_item_loading = true;
    message.work_item_error = '';
    try {
      var response = await fetch('/api/work-item?sequence=' + encodeURIComponent(message.sequence), { cache: 'no-store' });
      if (!response.ok) throw new Error('待办读取失败');
      var payload = await response.json();
      message.work_item = payload.work_item;
      if (payload.analysis) {
        message.analysis = payload.analysis;
        message.review = payload.review || null;
        message.kind = message.work_item.route;
        message.label = ({ service: '售后问题', feedback: '反馈图候选', warning: '疑似推广', mini: '小程序', burst: '疑似刷屏', order_image: '订单截图', requirement: '产品需求', product_link: '当天商品链接', thanks: '强提醒感谢', qa: '日常问题', closing: '无需回复', ordinary: '普通交流', unknown: '待判断' })[message.kind] || message.kind;
        message.labels = message.work_item.labels || payload.analysis.labels || [];
        message.reason = payload.analysis.reason || message.reason;
        message.priority = ['service', 'warning', 'mini', 'burst'].indexOf(message.kind) !== -1 ? '优先' : '普通';
        message.action = payload.analysis.needs_review ? '人工核验分类' : '确认分类';
        message.status = (payload.review && payload.review.status) || message.status || 'pending';
      }
      message.work_status = message.work_item.status;
      message.work_ai_state = message.work_item.ai_state;
      message.work_ai_route = message.work_item.ai && message.work_item.ai.route;
    } catch (error) {
      message.work_item_error = error.message || '请稍后重试';
      toast('处理建议读取失败', message.work_item_error);
    } finally {
      message.work_item_loading = false;
      if (selectedMessageId === message.id) renderQueue();
    }
  }

  async function saveWorkDecision(message, status) {
    var draft = document.getElementById('work-draft').value;
    var note = document.getElementById('work-note').value;
    try {
      var response = await fetch('/api/work-item', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sequence: message.sequence, status: status, draft: draft, note: note }) });
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.error || '审核未保存');
      message.work_item = payload.work_item;
      message.work_status = payload.work_item.status;
      if (document.getElementById('filter-work-status').value) loadQueue(true);
      else renderQueue();
      loadReviewHistory();
      toast('处理结论已保存', '工作台已记录；没有向群发送或上传飞书。');
    } catch (error) { toast('处理结论未保存', error.message || '请核对阻断原因'); }
  }

  function renderDetail() {
    var container = document.getElementById('message-detail');
    var message = selectedMessage();
    if (!message) {
      container.innerHTML = '<div class="detail-placeholder"><div><div class="empty-symbol">◫</div><strong>选择一条消息查看处理建议</strong><span>源消息保存在本机数据库中。</span></div></div>';
      return;
    }
    var done = message.status !== 'pending';
    var displayedSuggestion = suggestionFor(message);
    var statusBlock = done ? '<div class="decision-state ' + (message.status === 'approved' ? 'approved' : 'rejected') + '">' + (message.status === 'approved' ? '✓ 已记录模拟通过。没有发送消息或上传文件。' : '↶ 已记录驳回。没有发送消息或上传文件。') + '</div>' : '';
    var allowedImageHosts = ['oss.iyunzk.com', 'wxapp.tc.qq.com'];
    var media = Array.isArray(message.media) ? message.media : [];
    var imageUrls = (Array.isArray(message.image_urls) ? message.image_urls : []).filter(function (value) {
      try { var imageUrl = new URL(value); return imageUrl.protocol === 'https:' && allowedImageHosts.indexOf(imageUrl.hostname) !== -1; }
      catch (_error) { return false; }
    });
    var attachmentCount = Number(message.attachments || media.length || imageUrls.length);
    var attachmentCards = (media.length ? media : imageUrls.map(function (url) { return { url: url, state: 'pending' }; })).map(function (item, index) {
      var imageUrl = item.state === 'stored' ? '/api/media/' + Number(item.id) : item.url;
      if (item.state === 'failed') return '<div class="attachment-card attachment-missing"><span>图片归档失败 · ' + escapeHtml(item.error || '来源地址不可用') + '</span></div>';
      return '<a class="attachment-card image-attachment" href="' + escapeHtml(imageUrl) + '" target="_blank" rel="noopener noreferrer"><img src="' + escapeHtml(imageUrl) + '" alt="群图片 ' + (index + 1) + '" loading="lazy" decoding="async" referrerpolicy="no-referrer"><span>图片附件 · ' + (index + 1) + ' / ' + Math.max(attachmentCount, media.length || imageUrls.length) + (item.state === 'stored' ? ' · 本机已归档' : ' · 正在归档') + '</span></a>';
    }).join('');
    var missingImages = Math.max(0, attachmentCount - (media.length || imageUrls.length));
    for (var missingIndex = 0; missingIndex < missingImages; missingIndex += 1) {
      attachmentCards += '<div class="attachment-card attachment-missing"><span>图片附件 · 暂无可预览地址</span></div>';
    }
    var attachments = attachmentCount ? '<div class="attachment-strip">' + attachmentCards + '</div>' : '';
    if (message.analysis) {
      var analysis = message.analysis;
      var labels = Array.isArray(message.labels) ? message.labels : analysis.labels || [];
      var options = { service: '售后问题', feedback: '晒单图库', warning: '撤回/风险内容', mini: '小程序', burst: '刷屏', order_image: '订单截图', requirement: '产品需求', product_link: '当天商品链接', thanks: '强提醒感谢', qa: '日常问题', closing: '无需回复', ordinary: '普通交流', unknown: '待判断' };
      var evidence = (analysis.evidence || []).map(function (item) { return '<li>' + escapeHtml(item) + '</li>'; }).join('');
      var uncertainty = (analysis.uncertainty || []).map(function (item) { return '<li>' + escapeHtml(item) + '</li>'; }).join('');
      var statusText = message.status === 'approved' ? '已确认分类' : message.status === 'rejected' ? '已标记待判断' : '待人工审核';
      container.innerHTML = '<div class="detail-head"><div><div class="detail-kind"><span class="tag tag-' + escapeHtml(message.kind) + '">' + escapeHtml(message.label) + '</span>' + (message.revoked ? '<span class="tag">已撤回</span>' : '') + '<span class="detail-title">初步处理结果</span></div><div class="detail-id">来源消息 · ' + escapeHtml(message.id) + ' · 规则版本 ' + escapeHtml(analysis.version) + '</div></div><span class="detail-priority">' + escapeHtml(statusText) + '</span></div>' +
        '<div class="detail-context"><div class="person-avatar">' + escapeHtml(message.initials) + '</div><div class="context-main"><strong>' + escapeHtml(message.sender) + '</strong><small>' + escapeHtml(message.group) + '</small></div><span class="context-time">' + escapeHtml(message.date) + '</span></div>' +
        '<div class="message-bubble">' + escapeHtml(message.content) + '</div>' + attachments +
        '<div class="decision-card"><div class="decision-top"><span class="decision-label">分类依据与不确定性</span><span class="decision-chip">' + escapeHtml(analysis.confidence === 'low' ? '证据不足' : analysis.confidence === 'medium' ? '需要核验' : '规则命中') + '</span></div><div class="decision-body">' +
        '<div class="analysis-tags">' + labels.map(function (label) { return '<span class="tag tag-' + escapeHtml(label) + '">' + escapeHtml(options[label] || label) + '</span>'; }).join('') + '</div>' +
        (evidence ? '<strong class="analysis-caption">判断依据</strong><ul class="analysis-evidence">' + evidence + '</ul>' : '') +
        (uncertainty ? '<strong class="analysis-caption">待核实</strong><ul class="analysis-evidence analysis-uncertain">' + uncertainty + '</ul>' : '') +
        '<p class="analysis-context">自动展示当前客户前后 ' + Math.round(Number((analysis.features || {}).conversation_window_seconds || 1800) / 60) + ' 分钟内的消息及本群已发消息；已发消息暂不能可靠对应到某一条客户消息。</p><button class="text-button context-toggle" id="show-context" type="button" aria-expanded="true">收起上下文</button><div id="analysis-context-list" class="analysis-context-list"></div></div></div>' +
        '<div class="decision-card"><div class="decision-top"><span class="decision-label">人工复核分类</span><span class="decision-chip">不会发送</span></div><div class="decision-body"><label class="analysis-field">主类<select id="review-primary">' + Object.keys(options).map(function (key) { return '<option value="' + key + '"' + (message.kind === key ? ' selected' : '') + '>' + options[key] + '</option>'; }).join('') + '</select></label>' +
        '<div class="analysis-field"><span>涉及标签</span><div class="analysis-checks">' + Object.keys(options).filter(function (key) { return key !== 'ordinary' && key !== 'unknown'; }).map(function (key) { return '<label><input type="checkbox" value="' + key + '"' + (labels.indexOf(key) !== -1 ? ' checked' : '') + '>' + options[key] + '</label>'; }).join('') + '</div></div>' +
        '<label class="analysis-field">改判说明<textarea id="review-reason" rows="2" placeholder="说明人工确认或改判的依据">' + escapeHtml(message.review?.reason || '') + '</textarea></label></div></div>' +
        '<div class="detail-actions"><button class="button button-primary" id="confirm-analysis">确认分类</button><button class="button button-secondary" id="unknown-analysis">标为待判断</button>' + (message.status !== 'pending' ? '<button class="button edit-button" id="reopen-analysis">重新审核</button>' : '') + '</div>';
      container.querySelectorAll('.image-attachment img').forEach(function (image) {
        image.addEventListener('error', function () { image.alt = '图片暂不可预览'; });
      });
      if (message.work_item) {
        container.insertAdjacentHTML('beforeend', workItemHtml(message));
        bindWorkItem(message);
      } else if (message.work_item_error) {
        container.insertAdjacentHTML('beforeend', '<div class="decision-card work-item-load-state"><div class="decision-body"><strong>处理建议暂时无法加载</strong><p>' + escapeHtml(message.work_item_error) + '</p><button class="button button-secondary" id="retry-work-item-load" type="button">重试</button></div></div>');
        document.getElementById('retry-work-item-load').addEventListener('click', function () { refreshWorkItem(message); });
      } else {
        container.insertAdjacentHTML('beforeend', '<div class="decision-card work-item-load-state"><div class="decision-body"><strong>正在准备处理建议…</strong><p>消息已加载；建议只在打开时生成。</p></div></div>');
        if (!message.work_item_loading) refreshWorkItem(message);
      }
      document.getElementById('confirm-analysis').addEventListener('click', function () { submitAnalysisReview(message, 'approved'); });
      document.getElementById('unknown-analysis').addEventListener('click', function () { submitAnalysisReview(message, 'rejected', 'unknown'); });
      var contextButton = document.getElementById('show-context');
      var contextContainer = document.getElementById('analysis-context-list');
      contextButton.addEventListener('click', function () {
        var expanded = contextButton.getAttribute('aria-expanded') === 'true';
        contextButton.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        contextContainer.hidden = expanded;
        contextButton.textContent = expanded ? '展开上下文' : '收起上下文';
        if (!expanded && contextContainer.dataset.loaded !== 'true') showAnalysisContext(message);
      });
      showAnalysisContext(message);
      var reopen = document.getElementById('reopen-analysis');
      if (reopen) reopen.addEventListener('click', function () { submitAnalysisReview(message, 'pending'); });
      return;
    }
    container.innerHTML = '<div class="detail-head"><div><div class="detail-kind"><span class="tag tag-' + escapeHtml(message.kind) + '">' + escapeHtml(message.label) + '</span>' + (message.revoked ? '<span class="tag">已撤回</span>' : '') + '<span class="detail-title">' + escapeHtml(message.action) + '</span></div><div class="detail-id">云中客实时消息 · ' + escapeHtml(message.id) + '</div></div><span class="detail-priority">' + escapeHtml(message.priority === '优先' ? '优先审核' : '一般') + '</span></div>' +
      '<div class="detail-context"><div class="person-avatar">' + escapeHtml(message.initials) + '</div><div class="context-main"><strong>' + escapeHtml(message.sender) + '</strong><small>' + escapeHtml(message.group) + '</small></div><span class="context-time">' + escapeHtml(message.date) + '</span></div>' +
      '<div class="message-bubble">' + escapeHtml(message.content) + '</div>' + attachments +
      '<div class="decision-card"><div class="decision-top"><span class="decision-label"><span>✦</span>处理建议 <span class="source-mini">人工审核</span></span><span class="decision-chip">影子模式</span></div><div class="decision-body"><p class="decision-reason">' + escapeHtml(message.reason) + '</p><div class="suggestion-label"><span>拟处理内容</span><button class="text-button edit-suggestion">编辑</button></div><div class="suggestion-box" id="suggestion-box" contenteditable="false">' + escapeHtml(displayedSuggestion) + '</div></div></div>' + statusBlock +
      (!message.analysis ? (message.work_item_error
        ? '<div class="decision-card work-item-load-state"><div class="decision-body"><strong>分类建议暂时无法加载</strong><p>' + escapeHtml(message.work_item_error) + '</p><button class="button button-secondary" id="retry-work-item-load" type="button">重试</button></div></div>'
        : '<div class="decision-card work-item-load-state"><div class="decision-body"><strong>正在补充规则分类…</strong><p>消息已进入队列；规则分析与处理建议将在本条完成时显示。</p></div></div>') : '') +
      (message.analysis ? '<div class="detail-actions">' + (done ? '<button class="button button-secondary" id="undo-decision">撤销审核</button>' : '<button class="button button-primary" id="approve-simulated">✓ 通过建议（模拟）</button><button class="button edit-button" id="edit-suggestion-action">编辑</button><button class="button reject-button" id="reject-simulated">驳回</button>') + '</div>' : '');
    container.querySelectorAll('.image-attachment img').forEach(function (image) {
      image.addEventListener('error', function () {
        var card = image.parentElement;
        card.classList.add('image-failed');
        image.remove();
        var caption = card.querySelector('span');
        if (caption) caption.textContent = '预览失败 · 点击打开原图';
      }, { once: true });
    });
    var approve = document.getElementById('approve-simulated');
    if (approve) approve.addEventListener('click', function () { saveDecision('approved'); });
    var reject = document.getElementById('reject-simulated');
    if (reject) reject.addEventListener('click', function () { saveDecision('rejected'); });
    var undo = document.getElementById('undo-decision');
    if (undo) undo.addEventListener('click', function () { message.status = 'pending'; persist(); addAudit('撤销审核', message, '重新加入待审核队列'); renderQueue(); renderAudit(); toast('已撤销审核', '消息已回到待审核队列。'); });
    var edit = document.getElementById('edit-suggestion-action') || container.querySelector('.edit-suggestion');
    if (edit) edit.addEventListener('click', editSuggestion);
    var retryWorkItem = document.getElementById('retry-work-item-load');
    if (retryWorkItem) retryWorkItem.addEventListener('click', function () { refreshWorkItem(message); });
    if (!message.analysis && !message.work_item_loading && !message.work_item_error) refreshWorkItem(message);
  }

  async function showAnalysisContext(message) {
    var container = document.getElementById('analysis-context-list');
    if (!container) return;
    container.textContent = '正在读取同群上下文…';
    try {
      var response = await fetch('/api/context?sequence=' + encodeURIComponent(message.sequence), { cache: 'no-store' });
      if (!response.ok) throw new Error('上下文不可用');
      var payload = await response.json();
      if (!document.getElementById('analysis-context-list') || selectedMessageId !== message.id) return;
      container.innerHTML = (payload.messages || []).length ? payload.messages.map(function (item) {
        var stamp = item.timestamp ? new Date(item.timestamp) : null;
        var time = stamp && !Number.isNaN(stamp.getTime()) ? stamp.toLocaleTimeString('zh-CN', { hour12: false }) : '';
        var media = (item.media || []).map(function (attachment) {
          var mediaId = Number(attachment.id);
          if (!Number.isInteger(mediaId) || mediaId <= 0) return '';
          if (attachment.state !== 'stored') return '<span class="analysis-context-image-missing">图片未完整接收，暂不可预览</span>';
          var url = '/api/media/' + mediaId;
          return '<a class="analysis-context-image" href="' + url + '" data-image-preview="' + url + '" aria-label="点击查看大图"><img src="' + url + '" alt="上下文图片缩略图" loading="lazy" decoding="async"><span>点击查看大图</span></a>';
        }).join('');
        if (item.has_image && !media) media = '<span class="analysis-context-image-missing">图片消息 · 暂无可预览文件</span>';
        return '<div class="analysis-context-row"><strong>' + escapeHtml(item.direction === 'outbound' ? '已发消息' : item.same_sender ? '同一成员' : '其他成员') + ' · ' + escapeHtml(time) + '</strong><span class="analysis-context-text">' + escapeHtml(item.content || (item.has_image ? '[图片消息]' : '[附件或空消息]')) + '</span>' + (media ? '<div class="analysis-context-media">' + media + '</div>' : '') + '</div>';
      }).join('') : '<p class="analysis-context">所选时间窗没有可展示的同群上下文。</p>';
      container.dataset.loaded = 'true';
    } catch (error) { container.textContent = error.message || '上下文读取失败'; }
  }

  document.addEventListener('click', function (event) {
    var link = event.target.closest('[data-image-preview]');
    if (!link) return;
    var dialog = document.getElementById('image-preview-dialog');
    var image = document.getElementById('image-preview-image');
    if (!dialog || !image || typeof dialog.showModal !== 'function') return;
    event.preventDefault();
    image.src = link.getAttribute('data-image-preview');
    image.alt = link.querySelector('img')?.alt || '原图大图预览';
    dialog.showModal();
  });
  var imagePreviewDialog = document.getElementById('image-preview-dialog');
  if (imagePreviewDialog) {
    var imagePreviewClose = document.getElementById('image-preview-close');
    var imagePreviewImage = document.getElementById('image-preview-image');
    if (imagePreviewClose) imagePreviewClose.addEventListener('click', function () { imagePreviewDialog.close(); });
    imagePreviewDialog.addEventListener('click', function (event) {
      if (event.target === imagePreviewDialog) imagePreviewDialog.close();
    });
    if (imagePreviewImage) imagePreviewImage.addEventListener('error', function () {
      imagePreviewImage.alt = '原图暂不可用';
      imagePreviewImage.classList.add('preview-image-error');
    });
    if (imagePreviewImage) imagePreviewImage.addEventListener('load', function () { imagePreviewImage.classList.remove('preview-image-error'); });
  }

  async function submitAnalysisReview(message, status, forcedPrimary) {
    var wasReviewed = message.status !== 'pending';
    var primary = forcedPrimary || document.getElementById('review-primary').value;
    var labels = forcedPrimary === 'unknown' ? ['unknown'] : Array.from(document.querySelectorAll('.analysis-checks input:checked')).map(function (item) { return item.value; });
    if (labels.indexOf(primary) === -1) labels.unshift(primary);
    var reason = document.getElementById('review-reason').value.trim();
    if (status === 'pending') { primary = null; labels = null; reason = '重新审核'; }
    try {
      var response = await fetch('/api/review', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sequence: message.sequence, status: status, primary: primary, labels: labels, reason: reason }) });
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.error || '审核未保存');
      message.review = payload.review;
      message.status = status;
      message.kind = primary || message.analysis.primary;
      message.labels = labels || message.analysis.labels;
      message.label = { service: '售后问题', feedback: '晒单图库', warning: '撤回/风险内容', mini: '小程序', burst: '疑似刷屏', order_image: '订单截图', requirement: '产品需求', product_link: '当天商品链接', thanks: '强提醒感谢', qa: '日常问题', closing: '无需回复', ordinary: '普通交流', unknown: '待判断' }[message.kind];
      message.priority = ['service', 'warning', 'mini', 'burst', 'order_image', 'requirement', 'product_link', 'thanks'].indexOf(message.kind) !== -1 ? '优先' : '普通';
      if (liveStatus.analysis && wasReviewed !== (status !== 'pending')) liveStatus.analysis.reviewed += status === 'pending' ? -1 : 1;
      renderQueue();
      loadQueue(true);
      loadReviewHistory();
      toast('分类结论已保存', '保存在本机数据库；没有发送群消息。');
    } catch (error) { toast('审核保存失败', error.message || '请稍后重试'); }
  }

  function suggestionFor(message) {
    if (message.suggestionEdited) return message.suggestion;
    var rules = state.settings.rules;
    var ruleKey = { feedback: 'feedback', service: 'aftercare', warning: 'warning', burst: 'burst', qa: 'qa', closing: 'closing' }[message.kind];
    if (ruleKey && rules[ruleKey] === false) return '该类处理规则当前已关闭；消息仍保留在队列中供人工查看。';
    if (message.kind === 'warning' || message.kind === 'burst') {
      var allowlist = String(rules.allowlist || '').split(/\r?\n/).map(function (item) { return item.trim().toLowerCase(); }).filter(Boolean);
      if (allowlist.indexOf(message.sender.toLowerCase()) !== -1 || allowlist.indexOf(message.group.toLowerCase()) !== -1) {
        return '该消息命中白名单（' + message.sender + ' / ' + message.group + '），建议保留人工查看，不生成群规提醒。';
      }
    }
    if (message.kind === 'burst') {
      var count = Number((message.content.match(/(\d+)\s*条/) || [])[1] || 0);
      var seconds = Number((message.content.match(/(\d+)\s*秒/) || [])[1] || 0);
      count = count || Number(message.burstCount || 0);
      seconds = seconds || Number(message.burstSeconds || 0);
      if (count && seconds && (count < rules.burstCount || seconds > rules.burstSeconds)) {
        return '当前消息组未达到 ' + rules.burstSeconds + ' 秒内 ' + rules.burstCount + ' 条的刷屏阈值；建议人工查看，不生成刷屏提醒。';
      }
    }
    if (message.kind === 'service') {
      var template = state.settings.aftercareTemplate || message.suggestion;
      return template.replace(/\{\{\s*售后链接\s*\}\}/g, state.settings.aftercareUrl || '【售后链接】');
    }
    if (message.kind === 'qa') {
      var faqMatches = state.faqs.filter(function (faq) { return faq.enabled; }).map(function (faq) {
        var keywords = String(faq.keywords || '').split(/[，,、\s]+/).filter(function (keyword) { return keyword.length > 1; });
        var hits = keywords.filter(function (keyword) { return message.content.indexOf(keyword) !== -1; }).length;
        return { faq: faq, confidence: keywords.length ? Math.round(hits / keywords.length * 100) : 0 };
      }).filter(function (item) { return item.confidence > 0; }).sort(function (a, b) { return b.confidence - a.confidence; });
      if (faqMatches.length && faqMatches[0].confidence >= rules.qaConfidence) return faqMatches[0].faq.answer;
      if (faqMatches.length) return '本地 FAQ 关键词覆盖度约 ' + faqMatches[0].confidence + '%，低于当前 ' + rules.qaConfidence + '% 阈值；建议转人工确认。';
    }
    return message.suggestion;
  }

  function editSuggestion() {
    var box = document.getElementById('suggestion-box');
    var message = selectedMessage();
    if (!box || !message) return;
    if (box.isContentEditable) {
      message.suggestion = box.textContent.trim();
      message.suggestionEdited = true;
      box.contentEditable = 'false';
      persist();
      toast('建议已保存', '仅更新本机建议；未发送消息。');
      var edit = document.getElementById('edit-suggestion-action') || document.querySelector('.edit-suggestion');
      if (edit) edit.textContent = '编辑';
    } else {
      box.contentEditable = 'true';
      box.focus();
      var editButton = document.getElementById('edit-suggestion-action') || document.querySelector('.edit-suggestion');
      if (editButton) editButton.textContent = '完成编辑';
      if (!document.getElementById('save-suggestion-inline')) {
        var save = document.createElement('button');
        save.id = 'save-suggestion-inline';
        save.className = 'button button-secondary';
        save.textContent = '保存修改';
        save.addEventListener('click', function () { message.suggestion = box.textContent.trim(); message.suggestionEdited = true; box.contentEditable = 'false'; persist(); renderDetail(); toast('建议已保存', '仅更新本机建议；未发送消息。'); });
        document.querySelector('.detail-actions').prepend(save);
      }
    }
  }

  function saveDecision(status) {
    var message = selectedMessage();
    if (!message) return;
    var box = document.getElementById('suggestion-box');
    if (box) { message.suggestion = box.textContent.trim(); message.suggestionEdited = true; }
    message.status = status;
    persist();
    addAudit(status === 'approved' ? '模拟通过' : '驳回建议', message, message.action);
    renderQueue();
    renderAudit();
    toast(status === 'approved' ? '审核结论已记录' : '已驳回建议', '仅记录影子审核结论；未发送消息，也未上传文件。');
  }

  function addAudit(action, message, detail) {
    state.audit.unshift({ id: 'audit-' + Date.now(), action: action, messageId: message.id, label: message.label, detail: detail, time: new Date().toLocaleString('zh-CN', { hour12: false }) });
    state.audit = state.audit.slice(0, 100);
    persist();
  }

  function renderGroups() {
    groupNames = Array.from(new Set(groupNames.concat(state.faqs.map(function (faq) { return faq.group; }).filter(Boolean))));
    var groups = ['全部'].concat(groupNames);
    var list = document.getElementById('faq-groups-list');
    list.innerHTML = groups.map(function (group) {
      var count = group === '全部' ? state.faqs.length : state.faqs.filter(function (faq) { return faq.group === group; }).length;
      return '<button class="group-item ' + (activeFaqGroup === group ? 'active' : '') + '" data-group="' + escapeHtml(group) + '"><i class="group-dot"></i><span>' + escapeHtml(group) + '</span><span>' + count + '</span></button>';
    }).join('');
    list.querySelectorAll('.group-item').forEach(function (button) { button.addEventListener('click', function () { activeFaqGroup = button.dataset.group; renderFaq(); }); });
  }

  function renderFaq() {
    renderGroups();
    document.getElementById('faq-group-title').textContent = activeFaqGroup === '全部' ? '所有 FAQ' : activeFaqGroup;
    var query = document.getElementById('faq-search').value.trim().toLowerCase();
    var faqs = state.faqs.filter(function (faq) {
      var groupMatch = activeFaqGroup === '全部' || faq.group === activeFaqGroup;
      var text = [faq.question, faq.answer, faq.keywords, faq.group].join(' ').toLowerCase();
      return groupMatch && (!query || text.indexOf(query) !== -1);
    });
    document.getElementById('faq-group-subtitle').textContent = activeFaqGroup === '全部' ? '已启用内容可用于答疑建议' : '此分组中的启用内容可用于答疑建议';
    document.getElementById('faq-count-label').textContent = '显示 ' + faqs.length + ' 条';
    var body = document.getElementById('faq-table-body');
    body.innerHTML = faqs.length ? faqs.map(function (faq) {
      return '<tr><td><span class="faq-question">' + escapeHtml(faq.question) + '</span><span class="faq-keywords">' + escapeHtml(faq.keywords || '未设置关键词') + '</span></td><td><div class="faq-answer">' + escapeHtml(faq.answer) + '</div></td><td><span class="source-mini">' + escapeHtml(faq.source) + '</span></td><td><button class="switch ' + (faq.enabled ? 'on' : '') + '" data-toggle-faq="' + escapeHtml(faq.id) + '" aria-label="审核并切换启用状态"></button><small>' + escapeHtml(faq.enabled && faq.approved !== false ? '已审核启用' : '待审核 / 停用') + '</small></td><td><div class="row-actions"><button data-edit-faq="' + escapeHtml(faq.id) + '">编辑</button><button data-archive-faq="' + escapeHtml(faq.id) + '">停用</button></div></td></tr>';
    }).join('') : '<tr><td colspan="5"><div class="queue-empty"><b>还没有 FAQ</b>可以从这里新增本地答疑内容。</div></td></tr>';
    body.querySelectorAll('[data-toggle-faq]').forEach(function (button) { button.addEventListener('click', function () { var faq = findFaq(button.dataset.toggleFaq); faq.enabled = !faq.enabled; if (faq.enabled) faq.approved = true; faq.version = Number(faq.version || 0) + 1; persist(); renderFaq(); toast(faq.enabled ? 'FAQ 已审核启用' : 'FAQ 已停用', '仅影响本机的答疑建议。'); }); });
    body.querySelectorAll('[data-edit-faq]').forEach(function (button) { button.addEventListener('click', function () { openFaqDialog(button.dataset.editFaq); }); });
    body.querySelectorAll('[data-archive-faq]').forEach(function (button) { button.addEventListener('click', function () { var faq = findFaq(button.dataset.archiveFaq); faq.enabled = false; persist(); renderFaq(); toast('FAQ 已停用', '保留内容，可随时重新启用。'); }); });
  }

  function findFaq(id) { return state.faqs.find(function (faq) { return faq.id === id; }); }

  function openFaqDialog(id) {
    editingFaqId = id || null;
    var faq = id ? findFaq(id) : { group: groupNames[0], question: '', answer: '', keywords: '' };
    document.getElementById('faq-dialog-title').textContent = id ? '编辑 FAQ' : '新建 FAQ';
    document.getElementById('faq-edit-group').innerHTML = groupNames.map(function (group) { return '<option value="' + escapeHtml(group) + '">' + escapeHtml(group) + '</option>'; }).join('');
    document.getElementById('faq-edit-group').value = faq.group;
    document.getElementById('faq-edit-question').value = faq.question;
    document.getElementById('faq-edit-answer').value = faq.answer;
    document.getElementById('faq-edit-keywords').value = faq.keywords || '';
    document.getElementById('faq-edit-scope').value = faq.scope || 'general';
    document.getElementById('faq-edit-product').value = faq.product_name || '';
    document.getElementById('faq-edit-from').value = faq.valid_from || '';
    document.getElementById('faq-edit-until').value = faq.valid_until || '';
    document.getElementById('faq-edit-groups').value = (faq.group_scope || ['*']).join(',');
    document.getElementById('faq-dialog').showModal();
  }

  function saveFaq(event) {
    event.preventDefault();
    var data = { group: document.getElementById('faq-edit-group').value, question: document.getElementById('faq-edit-question').value.trim(), answer: document.getElementById('faq-edit-answer').value.trim(), keywords: document.getElementById('faq-edit-keywords').value.trim() };
    if (!data.question || !data.answer) return;
    data.scope=document.getElementById('faq-edit-scope').value;
    data.product_name=data.scope==='product'?document.getElementById('faq-edit-product').value.trim():'';
    data.valid_from=data.scope==='product'?document.getElementById('faq-edit-from').value:'';
    data.valid_until=data.scope==='product'?document.getElementById('faq-edit-until').value:'';
    data.group_scope=document.getElementById('faq-edit-groups').value.split(',').map(function(x){return x.trim();}).filter(Boolean);
    if(data.scope==='product'&&(!data.product_name||!data.valid_from||!data.valid_until||data.valid_until<data.valid_from)){toast('请补全商品和有效日期','产品 FAQ 需要商品名、开始日期和截止日期，结束不能早于开始。');return;}
    if(!data.group_scope.length){toast('请填写适用群范围','* 表示全部群，也可以填写逗号分隔的群 ID。');return;}
    if (editingFaqId) { var old = findFaq(editingFaqId); Object.assign(old, data, { source: '本地修改', version: Number(old.version || 0) + 1 }); }
    else state.faqs.unshift(Object.assign({ id: 'faq-local-' + Date.now(), source: '本地新增', enabled: true, approved: true, version: 1 }, data));
    persist();
    document.getElementById('faq-dialog').close();
    renderFaq();
    toast('FAQ 已保存', '本地修改不会写回云中客。');
  }

  async function loadFaqSourceStatus() {
    try {
      var response = await fetch('/api/faq/source-status', { cache: 'no-store' });
      if (!response.ok) throw new Error('来源快照状态不可用');
      var info = await response.json();
      document.getElementById('faq-source-summary').textContent = info.source_entries ?
        info.groups + ' 个分组 · ' + info.source_entries + ' 条来源条目 · 最近读取 ' +
        (info.last_seen ? new Date(info.last_seen).toLocaleString('zh-CN', { hour12: false }) : '未知') : '尚未导入来源正文';
      document.getElementById('faq-source-state').innerHTML = '<i class="status-dot ' + (info.source_entries ? 'green' : 'amber') + '"></i>' + (info.source_entries ? '本地快照' : '待导入');
    } catch (error) { document.getElementById('faq-source-summary').textContent = error.message || '读取失败'; }
  }

  async function importFaqSnapshot() {
    var text = document.getElementById('faq-import-data').value.trim();
    try {
      var entries = JSON.parse(text);
      if (!Array.isArray(entries)) throw new Error('必须是 JSON 数组');
      var response = await fetch('/api/faq/import', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ entries: entries }) });
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.error || '导入失败');
      state.faqs = payload.config.faqs || [];
      document.getElementById('faq-import-dialog').close();
      document.getElementById('faq-import-data').value = '';
      renderFaq(); loadFaqSourceStatus();
      toast('来源快照已导入', '新增待审核文字 FAQ ' + payload.report.faq_candidates_added + ' 条；媒体或空内容 ' + payload.report.media_or_empty + ' 条。');
    } catch (error) { toast('快照导入失败', error.message || '请检查 JSON 内容'); }
  }

  function renderSettings() {
    var settings = state.settings;
    document.getElementById('aftercare-name').value = settings.aftercareName || '';
    document.getElementById('aftercare-global-scope').checked = settings.aftercareScope === 'all_groups';
    document.getElementById('faq-service-contacts').checked = settings.useFaqServiceContacts === true;
    document.getElementById('aftercare-url').value = settings.aftercareUrl || '';
    document.getElementById('aftercare-template').value = settings.aftercareTemplate || '';
    document.getElementById('previous-activity-template').value = settings.previousActivityTemplate || '';
    document.getElementById('customer-service-template').value = settings.customerServiceTemplate || '需要一对一协助，可联系售后客服：{{售后链接}}';
    document.getElementById('image-understanding-enabled').checked = settings.imageUnderstandingEnabled !== false;
    document.getElementById('gallery-type').value = settings.galleryType || 'bitable';
    document.getElementById('gallery-url').value = settings.galleryUrl || '';
    document.getElementById('feishu-app-id').value = settings.feishuAppId || '';
    document.getElementById('feishu-app-secret').value = '';
    document.getElementById('demand-table-url').value = settings.demandTableUrl || '';
    document.getElementById('order-table-url').value = settings.orderTableUrl || '';
    document.getElementById('aftercare-evidence-url').value = settings.aftercareEvidenceUrl || '';
    document.getElementById('today-product-copy').value = settings.todayProductCopy || '';
    document.getElementById('sold-last-week-products').value = settings.soldLastWeekProducts || '';
    document.getElementById('sold-next-week-products').value = settings.soldNextWeekProducts || '';
    document.getElementById('thanks-template').value = settings.thanksTemplate || '';
    document.getElementById('allowed-share-domains').value = settings.allowedShareDomains || '';
    var time = localStorage.getItem('message-workbench-aftercare-updated');
    if (time) document.getElementById('aftercare-history').innerHTML = '<div class="history-empty"><div class="empty-symbol">✓</div><strong>最近一次修改</strong><span>' + escapeHtml(time) + '<br>保存在本机浏览器</span></div>';
  }

  function renderRules() {
    var rules = state.settings.rules;
    ['feedback', 'warning', 'aftercare', 'qa', 'closing', 'burst'].forEach(function (key) {
      document.getElementById('rule-' + key).checked = rules[key] !== false;
    });
    document.getElementById('burst-count').value = rules.burstCount || 8;
    document.getElementById('burst-seconds').value = rules.burstSeconds || 30;
    document.getElementById('conversation-minutes').value = Math.round(Number(rules.conversationSeconds || 1800) / 60);
    document.getElementById('qa-confidence').value = rules.qaConfidence || 82;
    document.getElementById('qa-confidence-output').textContent = (rules.qaConfidence || 82) + '%';
    document.getElementById('rule-allowlist').value = rules.allowlist || '';
    updateBurstExample();
  }

  function updateBurstExample() {
    document.getElementById('burst-example-count').textContent = document.getElementById('burst-count').value || '8';
    document.getElementById('burst-example-seconds').textContent = document.getElementById('burst-seconds').value || '30';
  }

  async function saveRules() {
    state.settings.rules = {
      feedback: document.getElementById('rule-feedback').checked,
      warning: document.getElementById('rule-warning').checked,
      aftercare: document.getElementById('rule-aftercare').checked,
      qa: document.getElementById('rule-qa').checked,
      closing: document.getElementById('rule-closing').checked,
      burst: document.getElementById('rule-burst').checked,
      burstCount: Math.min(100, Math.max(3, Number(document.getElementById('burst-count').value) || 8)),
      burstSeconds: Math.min(600, Math.max(5, Number(document.getElementById('burst-seconds').value) || 30)),
      conversationSeconds: Math.min(1800, Math.max(60, (Number(document.getElementById('conversation-minutes').value) || 30) * 60)),
      qaConfidence: Math.min(99, Math.max(50, Number(document.getElementById('qa-confidence').value) || 82)),
      allowlist: document.getElementById('rule-allowlist').value.trim()
    };
    persist(false);
    renderRules();
    var saved = await saveServerConfig();
    if (!saved) return;
    loadRuleVersions();
    isFirstLiveSync = true;
    syncLive();
    loadQueue(true);
    toast('处理规则已保存', '已生成版本 ' + saved.active_rule_version + '；历史消息已重新分析，人工结论保留。');
  }

  async function loadRuleVersions() {
    try {
      var response = await fetch('/api/rules/versions', { cache: 'no-store' });
      if (!response.ok) throw new Error('规则版本读取失败');
      var payload = await response.json();
      var box = document.getElementById('rule-version-list');
      box.innerHTML = (payload.versions || []).map(function (item) {
        var active = item.version === payload.active_version;
        return '<span class="rule-version-item">版本 ' + item.version + ' · ' + escapeHtml(item.created_at || '') +
          (active ? ' · 当前' : item.version < payload.algorithm_min_version ? ' · 旧算法留档' : ' <button type="button" data-activate-rule="' + item.version + '">启用此版本</button>') + '</span>';
      }).join('') || '暂无版本';
      box.querySelectorAll('[data-activate-rule]').forEach(function (button) {
        button.addEventListener('click', function () { activateRuleVersion(Number(button.dataset.activateRule)); });
      });
    } catch (error) { document.getElementById('rule-version-list').textContent = error.message || '读取失败'; }
  }

  async function activateRuleVersion(version) {
    try {
      var response = await fetch('/api/rules/activate', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version: version }) });
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.error || '切换失败');
      state.settings = Object.assign({}, seed.settings, payload.config.settings || {}, { rules: Object.assign({}, seed.settings.rules, (payload.config.settings || {}).rules || {}) });
      renderRules(); loadRuleVersions();
      isFirstLiveSync = true; syncLive(); loadQueue(true);
      toast('规则版本已切换', '已启用版本 ' + version + '；人工审核结论保留。');
    } catch (error) { toast('规则版本切换失败', error.message || '请稍后重试'); }
  }

  async function reprocessAnalysis() {
    try {
      var response = await fetch('/api/analyze/reprocess', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.error || '重算失败');
      isFirstLiveSync = true; syncLive(); loadQueue(true);
      toast('当前版本已重新分析', '处理 ' + payload.processed + ' 条；人工审核结论保留。');
    } catch (error) { toast('重新分析失败', error.message || '请稍后重试'); }
  }

  async function loadEvaluation() {
    try {
      var response = await fetch('/api/evaluation', { cache: 'no-store' });
      if (!response.ok) throw new Error('质量统计读取失败');
      var report = await response.json();
      var keyLabels = { warning: '疑似推广', service: '售后问题', feedback: '反馈图候选', closing: '无需回复' };
      var lines = Object.keys(keyLabels).map(function (key) {
        var value = (report.labels || {})[key] || {};
        return keyLabels[key] + '：正例 ' + Number(value.support || 0) + '，精确率 ' + (value.precision == null ? '未测' : Math.round(value.precision * 100) + '%') +
          '，召回率 ' + (value.recall == null ? '未测' : Math.round(value.recall * 100) + '%') + (value.sample_sufficient ? '' : '（样本不足）');
      });
      document.getElementById('evaluation-report').textContent = '已复核 ' + report.reviewed_sample + ' / 建议至少 ' + report.minimum_total + ' 条。' + lines.join('；') + '。此统计只代表已复核样本。';
    } catch (error) { document.getElementById('evaluation-report').textContent = error.message || '质量统计不可用'; }
  }

  function saveAftercare() {
    state.settings.aftercareName = document.getElementById('aftercare-name').value.trim() || '售后客服';
    state.settings.aftercareUrl = document.getElementById('aftercare-url').value.trim();
    state.settings.aftercareTemplate = document.getElementById('aftercare-template').value.trim();
    state.settings.previousActivityTemplate = document.getElementById('previous-activity-template').value.trim();
    state.settings.customerServiceTemplate = document.getElementById('customer-service-template').value.trim();
    state.settings.aftercareScope = document.getElementById('aftercare-global-scope').checked ? 'all_groups' : '';
    state.settings.useFaqServiceContacts = document.getElementById('faq-service-contacts').checked;
    persist();
    var time = new Date().toLocaleString('zh-CN', { hour12: false });
    localStorage.setItem('message-workbench-aftercare-updated', time);
    renderSettings();
    toast('售后配置已保存', '新建议将使用当前配置；不会发送到群里。');
  }

  async function saveWorkflowConfig() {
    state.settings.demandTableUrl = document.getElementById('demand-table-url').value.trim();
    state.settings.orderTableUrl = document.getElementById('order-table-url').value.trim();
    state.settings.aftercareEvidenceUrl = document.getElementById('aftercare-evidence-url').value.trim();
    state.settings.todayProductCopy = document.getElementById('today-product-copy').value.trim();
    state.settings.soldLastWeekProducts = document.getElementById('sold-last-week-products').value.trim();
    state.settings.soldNextWeekProducts = document.getElementById('sold-next-week-products').value.trim();
    state.settings.thanksTemplate = document.getElementById('thanks-template').value.trim() || '谢谢支持～';
    state.settings.allowedShareDomains = document.getElementById('allowed-share-domains').value.trim();
    state.settings.imageUnderstandingEnabled = document.getElementById('image-understanding-enabled').checked;
    persist(false);
    var saved = await saveServerConfig();
    if (!saved) return;
    loadQueue(true);
    toast('业务配置已保存', '规则和处理建议会使用最新配置；表格仍只生成本地待办。');
  }

  async function loadIntegrations() {
    try {
      var response = await fetch('/api/integrations', { cache: 'no-store' });
      if (!response.ok) return;
      var data = await response.json();
      if (data.feishu_app_id && !state.settings.feishuAppId) document.getElementById('feishu-app-id').value = data.feishu_app_id;
      document.getElementById('feishu-state').innerHTML = '<i class="status-dot amber"></i>' + (data.feishu_configured ? '凭证已配置 · 待测试' : '凭证待配置');
    } catch (_error) { /* service status remains visible elsewhere */ }
  }

  async function saveGallery() {
    state.settings.galleryType = document.getElementById('gallery-type').value;
    state.settings.galleryUrl = document.getElementById('gallery-url').value.trim();
    state.settings.feishuAppId = document.getElementById('feishu-app-id').value.trim();
    var secret = document.getElementById('feishu-app-secret').value.trim();
    persist(false);
    var saved = await saveServerConfig();
    if (!saved) return;
    if (state.settings.feishuAppId) {
      try {
        var response = await fetch('/api/integrations', { method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ feishu_app_id: state.settings.feishuAppId, feishu_app_secret: secret }) });
        var payload = await response.json();
        if (!response.ok || !payload.ok) throw new Error(payload.error || '应用凭证未保存');
      } catch (error) { toast('飞书凭证未保存', error.message || '请重试'); return; }
    }
    document.getElementById('feishu-app-secret').value = '';
    loadIntegrations();
    toast('图库配置已保存', '可进行只读连接测试；没有上传文件。');
  }

  async function testGallery() {
    var stateBox = document.getElementById('feishu-state');
    stateBox.textContent = '测试中…';
    try {
      var response = await fetch('/api/feishu/check', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.error || payload.message || '无权读取目标表格');
      stateBox.innerHTML = '<i class="status-dot green"></i>表格可读 · ' + Number(payload.field_count || 0) + ' 个字段';
      toast('飞书连接正常', '目标表格只读核验通过；尚未上传图片。');
    } catch (error) { stateBox.innerHTML = '<i class="status-dot amber"></i>读取失败'; toast('飞书连接未通过', error.message || '请检查应用权限和表格共享设置'); }
  }

  function renderAudit() {
    var container = document.getElementById('audit-list');
    document.getElementById('audit-count').textContent = state.audit.length + ' 条记录';
    if (!state.audit.length) {
      container.innerHTML = '<div class="audit-empty">还没有审核记录。对消息执行模拟通过或驳回后，记录会显示在这里。</div>';
      return;
    }
    container.innerHTML = state.audit.map(function (item) {
      var rejected = item.action === '驳回建议';
      return '<div class="audit-row"><div class="audit-icon ' + (rejected ? 'rejected' : '') + '">' + (rejected ? '↶' : '✓') + '</div><div class="audit-copy"><strong>' + escapeHtml(item.action) + ' · ' + escapeHtml(item.label) + '</strong><span>' + escapeHtml(item.detail) + '</span></div><span class="audit-time">' + escapeHtml(item.time) + '</span></div>';
    }).join('');
  }

  async function loadReviewHistory() {
    try {
      var response = await fetch('/api/reviews', { cache: 'no-store' });
      if (!response.ok) throw new Error('读取审核记录失败');
      var payload = await response.json();
      state.audit = (payload.reviews || []).map(function (item) {
        var decision = item.decision || {};
        return { id: item.id, action: decision.status === 'pending' ? '重新审核' : decision.status === 'rejected' ? '标为待判断' : '确认分类',
          label: decision.primary_label || '原始分类', detail: (item.group || '') + ' · ' + (decision.reason || '无备注'),
          time: item.time ? new Date(item.time).toLocaleString('zh-CN', { hour12: false }) : '' };
      });
      state.audit = state.audit.concat((payload.work_items || []).map(function (item) {
        return { id: 'work-' + item.id, action: ({ approved: '批准草稿', rejected: '驳回建议', handled: '标记已处理', needs_human: '转人工', pending: '重新审核' })[item.action] || item.action,
          label: '消息 #' + item.event_sequence, detail: (item.detail || {}).note || '处理建议审核',
          time: item.changed_at ? new Date(item.changed_at).toLocaleString('zh-CN', { hour12: false }) : '' };
      }));
      state.audit = state.audit.concat((payload.config_history || []).map(function (item) {
        return { id: 'config-' + item.id, action: item.kind + '配置更新', label: '工作台配置', detail: '已保存版本记录',
          time: item.changed_at ? new Date(item.changed_at).toLocaleString('zh-CN', { hour12: false }) : '' };
      }));
      state.audit = state.audit.concat((payload.legacy_audit || []).map(function (item, index) {
        return { id: 'legacy-' + index, action: '旧版模拟审核 · ' + (item.action || '记录'),
          label: item.label || '', detail: item.detail || '', time: item.time || '' };
      }));
      state.audit.sort(function (a, b) { return String(b.time).localeCompare(String(a.time)); });
      renderAudit();
    } catch (error) { toast('审核记录暂不可用', error.message || '请稍后重试'); }
  }

  function bindEvents() {
    document.querySelectorAll('.nav-item').forEach(function (button) { button.addEventListener('click', function () { setView(button.dataset.view); }); });
    var devAuditKind = document.getElementById('dev-audit-kind');
    Object.keys(devAuditLabels).forEach(function (key) {
      var option = document.createElement('option');
      option.value = key; option.textContent = devAuditLabels[key]; devAuditKind.appendChild(option);
    });
    var auditList = document.getElementById('dev-audit-list');
    var selectAuditRow = function (event) {
      var row = event.target.closest && event.target.closest('[data-dev-audit-sequence]');
      if (row && auditList.contains(row)) loadDevelopmentAuditDetail(Number(row.dataset.devAuditSequence));
    };
    auditList.addEventListener('click', selectAuditRow);
    auditList.addEventListener('keydown', function (event) {
      if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); selectAuditRow(event); }
    });
    document.getElementById('dev-audit-status').addEventListener('change', function () { loadDevelopmentAudits(true); });
    document.querySelectorAll('[data-dev-audit-status]').forEach(function (button) {
      button.addEventListener('click', function () {
        if (devAuditSaving || document.getElementById('dev-audit-status').value === button.dataset.devAuditStatus) return;
        document.getElementById('dev-audit-status').value = button.dataset.devAuditStatus;
        loadDevelopmentAudits(true);
      });
    });
    document.getElementById('dev-audit-kind').addEventListener('change', function () { loadDevelopmentAudits(true); });
    document.getElementById('dev-audit-differences').addEventListener('change', function () {
      if (this.checked && document.getElementById('dev-audit-status').value === 'todo') document.getElementById('dev-audit-status').value = 'audited';
      loadDevelopmentAudits(true);
    });
    document.getElementById('dev-audit-search').addEventListener('input', function () {
      window.clearTimeout(devAuditFilterTimer);
      devAuditFilterTimer = window.setTimeout(function () { loadDevelopmentAudits(true); }, 250);
    });
    document.getElementById('dev-audit-load-more').addEventListener('click', function () { if (devAuditHasMore) loadDevelopmentAudits(false); });
    document.getElementById('dev-audit-refresh').addEventListener('click', function () { loadDevelopmentAuditSummary(); loadDevelopmentAudits(true); });
    document.getElementById('message-search').addEventListener('input', function () { window.clearTimeout(filterTimer); filterTimer = window.setTimeout(function () { loadQueue(true); }, 250); });
    document.getElementById('group-conversations').addEventListener('change', renderQueue);
    document.querySelectorAll('.filter-tab').forEach(function (button) { button.addEventListener('click', function () { activeFilter = button.dataset.filter; document.querySelectorAll('.filter-tab').forEach(function (item) { item.classList.toggle('selected', item === button); }); loadQueue(true); }); });
    var advancedFilters = document.getElementById('queue-advanced-filters');
    var filterButton = document.getElementById('filter-button');
    filterButton.addEventListener('click', function () {
      advancedFilters.hidden = !advancedFilters.hidden;
      filterButton.setAttribute('aria-expanded', String(!advancedFilters.hidden));
      if (!advancedFilters.hidden) document.getElementById('filter-kind').focus();
    });
    document.getElementById('apply-filters').addEventListener('click', function () {
      updateFilterButtonCount();
      loadQueue(true);
      advancedFilters.hidden = true;
      filterButton.setAttribute('aria-expanded', 'false');
    });
    document.getElementById('clear-filters').addEventListener('click', function () {
      ['filter-kind', 'filter-group', 'filter-start', 'filter-end', 'filter-status', 'filter-work-status'].forEach(function (id) { document.getElementById(id).value = ''; });
      updateFilterButtonCount();
      activeFilter = 'all';
      document.querySelectorAll('.filter-tab').forEach(function (item) { item.classList.toggle('selected', item.dataset.filter === 'all'); });
      loadQueue(true);
    });
    document.getElementById('load-more').addEventListener('click', loadOlderMessages);
    document.getElementById('refresh-replies').addEventListener('click', function () { loadReplies(true); });
    document.getElementById('load-more-replies').addEventListener('click', function () { loadReplies(false); });
    document.getElementById('refresh-demo').addEventListener('click', function () {
      if (liveStatus.sync_enabled === false) loadHistoryOnce(false);
      else { this.dataset.refreshRequested = 'true'; syncLive(); }
    });
    document.getElementById('read-range-history').addEventListener('click', function () { loadHistoryOnce(true); });
    document.getElementById('toggle-sync').addEventListener('click', toggleSync);
    document.getElementById('faq-search').addEventListener('input', renderFaq);
    document.getElementById('faq-add').addEventListener('click', function () { openFaqDialog(null); });
    document.getElementById('faq-form').addEventListener('submit', saveFaq);
    document.getElementById('faq-candidate-form').addEventListener('submit', saveFaqCandidate);
    document.getElementById('faq-candidate-scope').addEventListener('change',setFaqCandidateScopeUI);
    document.getElementById('refresh-faq-candidates').addEventListener('click',loadFaqCandidates);
    document.querySelectorAll('[data-faq-candidate-status]').forEach(function(button){button.addEventListener('click',function(){
      faqCandidateStatus=button.dataset.faqCandidateStatus;
      document.querySelectorAll('[data-faq-candidate-status]').forEach(function(item){item.setAttribute('aria-pressed',String(item===button));});loadFaqCandidates();
    });});
    document.querySelectorAll('[data-faq-candidate-scope]').forEach(function(button){button.addEventListener('click',function(){
      faqCandidateScope=button.dataset.faqCandidateScope;
      document.querySelectorAll('[data-faq-candidate-scope]').forEach(function(item){item.setAttribute('aria-pressed',String(item===button));});loadFaqCandidates();
    });});
    document.getElementById('faq-candidate-list').addEventListener('click',function(event){
      var edit=event.target.closest('[data-faq-candidate-edit]');var approve=event.target.closest('[data-faq-candidate-approve]');
      var reject=event.target.closest('[data-faq-candidate-reject]');var restore=event.target.closest('[data-faq-candidate-restore]');
      if(edit)editFaqCandidate(edit.dataset.faqCandidateEdit);
      else if(approve)approveFaqCandidate(approve.dataset.faqCandidateApprove);
      else if(reject)setFaqCandidateStatus(reject.dataset.faqCandidateReject,'rejected');
      else if(restore)setFaqCandidateStatus(restore.dataset.faqCandidateRestore,'pending');
    });
    document.getElementById('faq-refresh').addEventListener('click', loadFaqSourceStatus);
    document.getElementById('faq-import-open').addEventListener('click', function () { document.getElementById('faq-import-dialog').showModal(); });
    document.getElementById('faq-import-close').addEventListener('click', function () { document.getElementById('faq-import-dialog').close(); });
    document.getElementById('faq-import-submit').addEventListener('click', importFaqSnapshot);
    document.getElementById('faq-sync-info').addEventListener('click', function () { toast('FAQ 来源规则', '来源快照只读；导入项需人工审核启用；本地修改不会写回云中客，也不会被来源刷新覆盖。'); });
    document.getElementById('save-aftercare').addEventListener('click', saveAftercare);
    document.getElementById('save-workflow-config').addEventListener('click', saveWorkflowConfig);
    document.getElementById('preview-aftercare').addEventListener('click', function () {
      var template = document.getElementById('aftercare-template').value || '请联系售后客服：{{售后链接}}';
      var url = document.getElementById('aftercare-url').value.trim() || '【售后链接】';
      var preview = document.getElementById('aftercare-preview');
      preview.textContent = template.replace(/\{\{\s*售后链接\s*\}\}/g, url);
      preview.classList.remove('hidden');
    });
    document.getElementById('save-gallery').addEventListener('click', saveGallery);
    document.getElementById('test-gallery').addEventListener('click', testGallery);
    document.getElementById('save-rules').addEventListener('click', saveRules);
    document.getElementById('reprocess-analysis').addEventListener('click', reprocessAnalysis);
    document.getElementById('refresh-evaluation').addEventListener('click', loadEvaluation);
    document.getElementById('qa-confidence').addEventListener('input', function () { document.getElementById('qa-confidence-output').textContent = this.value + '%'; });
    document.getElementById('burst-count').addEventListener('input', updateBurstExample);
    document.getElementById('burst-seconds').addEventListener('input', updateBurstExample);
    document.getElementById('clear-history').addEventListener('click', loadReviewHistory);
    document.querySelector('.banner-close').addEventListener('click', function () { document.querySelector('.privacy-banner').remove(); });
    document.getElementById('workflow-refresh').addEventListener('click', function(){workflowData=null;loadWorkflowData();});
    document.getElementById('workflow-source-list').addEventListener('click', async function(event){
      var saveButton=event.target.closest('[data-source-save]');
      var readButton=event.target.closest('[data-source-read]');
      if(saveButton){
        var sid=saveButton.dataset.sourceSave; var urlInput=Array.prototype.find.call(this.querySelectorAll('[data-source-url]'),function(x){return x.dataset.sourceUrl===sid;});
        var sheetInput=Array.prototype.find.call(this.querySelectorAll('[data-source-sheet]'),function(x){return x.dataset.sourceSheet===sid;});
        var accountInput=Array.prototype.find.call(this.querySelectorAll('[data-source-account]'),function(x){return x.dataset.sourceAccount===sid;});
        var intervalInput=Array.prototype.find.call(this.querySelectorAll('[data-source-interval]'),function(x){return x.dataset.sourceInterval===sid;});
        var periodicInput=Array.prototype.find.call(this.querySelectorAll('[data-source-periodic]'),function(x){return x.dataset.sourcePeriodic===sid;});
        try{await workflowPost('/api/workflow/source',{source_id:sid,url:urlInput&&urlInput.value,sheet_id:sheetInput&&sheetInput.value,source_account_id:accountInput&&accountInput.value,sync_interval_minutes:Number(intervalInput&&intervalInput.value||360),sync_enabled:Boolean(periodicInput&&periodicInput.checked)});toast('资料源设置已保存',periodicInput&&periodicInput.checked?'只读周期同步已开启；不会写飞书或影响云中客同步。':'周期同步已关闭；需要时可手动读取。');workflowData=null;loadWorkflowData();}
        catch(error){toast('资料源保存失败',error.message||'请检查链接格式');}
      } else if(readButton){runSourceRead(readButton.dataset.sourceRead,readButton.dataset.action||'preview');}
    });
    document.getElementById('service-existing').addEventListener('change',function(){
      var profile=(workflowData&&workflowData.service_profiles||[]).find(function(item){return item.id===this.value;}.bind(this));
      document.getElementById('service-name').value=profile?profile.name:'';
      document.getElementById('service-url').value=profile?profile.url:'';
      document.getElementById('service-template').value=profile?profile.template:'需要一对一协助，可联系售后客服：{{售后链接}}';
    });
    document.getElementById('save-service-profile').addEventListener('click',async function(){
      try{var result=await workflowPost('/api/workflow/profile',{id:document.getElementById('service-existing').value||undefined,name:document.getElementById('service-name').value,url:document.getElementById('service-url').value,template:document.getElementById('service-template').value});
        toast('已保存客服配置',result.profile.url?'可在群映射中选择':'链接仍为空，相关建议会显示待补');workflowData=null;loadWorkflowData();}
      catch(error){toast('客服配置保存失败',error.message||'请检查字段');}
    });
    document.getElementById('product-catalog-existing').addEventListener('change',function(){
      var product=(workflowData&&workflowData.products||[]).find(function(item){return item.id===this.value;}.bind(this));
      document.getElementById('product-catalog-name').value=product?product.name:'';
      document.getElementById('product-catalog-aliases').value=product?(product.aliases||[]).join(', '):'';
      document.getElementById('product-catalog-state').value=product?product.state:'draft';
    });
    document.getElementById('product-link-product').addEventListener('change',function(){
      var product=(workflowData&&workflowData.products||[]).find(function(item){return item.id===this.value;}.bind(this));
      var productName=document.getElementById('product-link-name');productName.value=product?product.name:'';productName.readOnly=Boolean(product);
    });
    document.getElementById('save-product-catalog').addEventListener('click',async function(){
      try{var result=await workflowPost('/api/workflow/product',{id:document.getElementById('product-catalog-existing').value||undefined,
          name:document.getElementById('product-catalog-name').value,
          aliases:document.getElementById('product-catalog-aliases').value.split(/[，,]/).map(function(x){return x.trim();}).filter(Boolean),
          state:document.getElementById('product-catalog-state').value});
        toast('商品目录已保存',result.product.name+'；这不会把商品标记为已排期或已售。');workflowData=null;loadWorkflowData();}
      catch(error){toast('商品目录保存失败',error.message||'请检查名称和别名');}
    });
    document.getElementById('group-id').addEventListener('change',function(){var found=(workflowData&&workflowData.observed_groups||[]).find(function(g){return g.group_id===document.getElementById('group-id').value;});if(found){document.getElementById('group-account').value=found.source_account_id;if(!document.getElementById('group-name').value)document.getElementById('group-name').value=found.name;}});
    document.getElementById('save-business-group').addEventListener('click',async function(){
      try{await workflowPost('/api/workflow/group',{source_account_id:document.getElementById('group-account').value,group_id:document.getElementById('group-id').value,name:document.getElementById('group-name').value,service_profile_id:document.getElementById('group-profile').value});
        toast('群映射已保存','该映射会影响后续售后建议；现存建议需重新生成。');workflowData=null;loadWorkflowData();}
      catch(error){toast('群映射保存失败',error.message||'请检查群 ID 和客服配置');}
    });
    document.getElementById('save-product-link').addEventListener('click',async function(){
      try{var groups=document.getElementById('product-link-groups').value.split(',').map(function(x){return x.trim();}).filter(Boolean);
        var requestedState=document.getElementById('product-link-state').value;
        await workflowPost('/api/workflow/product-link',{source_account_id:document.getElementById('group-account').value,product_id:document.getElementById('product-link-product').value||undefined,product_name:document.getElementById('product-link-name').value,business_date:document.getElementById('product-link-date').value,url:document.getElementById('product-link-url').value,copy:document.getElementById('product-link-copy').value,group_scope:groups,state:requestedState});
        toast(requestedState==='confirmed'?'商品链接已确认':'商品链接草稿已保存',requestedState==='confirmed'?'后续建议按群和日期精确匹配；不会自动发送。':'草稿不会进入可直接采用的答复。');workflowData=null;loadWorkflowData();}
      catch(error){toast('商品链接保存失败',error.message||'请检查日期和 HTTPS 链接');}
    });
    document.getElementById('import-manual-material').addEventListener('click',async function(){
      try{var result=await workflowPost('/api/workflow/import',{source_account_id:document.getElementById('group-account').value,source_id:document.getElementById('manual-material-source').value,format:document.getElementById('manual-material-format').value,content:document.getElementById('manual-material-content').value,material_type:document.getElementById('manual-material-type').value});
        toast('资料已导入为草稿',result.report.imported+' 条；在线飞书仍未同步。');document.getElementById('manual-material-content').value='';workflowData=null;loadWorkflowData();}
      catch(error){toast('手动导入失败',error.message||'请检查 JSON 或 CSV');}
    });
    document.getElementById('manual-material-list').addEventListener('click',async function(event){
      var button=event.target.closest('[data-material-confirm]');if(!button)return;var materialId=button.dataset.materialConfirm;
      var dateInput=Array.prototype.find.call(this.querySelectorAll('[data-material-date]'),function(x){return x.dataset.materialDate===materialId;});
      var groupsInput=Array.prototype.find.call(this.querySelectorAll('[data-material-groups]'),function(x){return x.dataset.materialGroups===materialId;});
      try{await workflowPost('/api/workflow/material',{material_id:materialId,source_account_id:button.dataset.account,state:button.dataset.state,business_date:dateInput&&dateInput.value,valid_until:dateInput&&dateInput.value,group_scope:(groupsInput&&groupsInput.value||'').split(',').map(function(x){return x.trim();}).filter(Boolean)});
        toast(button.dataset.state==='confirmed'?'资料已确认生效':'资料已停用','仅影响之后生成的规则建议；不回写飞书。');workflowData=null;loadWorkflowData();}
      catch(error){toast('资料状态未更新',error.message||'请核对日期和群范围');}
    });
    document.getElementById('confirm-schedule').addEventListener('click',async function(){
      try{var groups=document.getElementById('schedule-groups').value.split(',').map(function(x){return x.trim();}).filter(Boolean);var year=Number(document.getElementById('schedule-year').value);
        var result=await workflowPost('/api/workflow/schedule/confirm',{year:year,group_ids:groups,source_account_id:document.getElementById('group-account').value,source_id:'feishu-product-schedule'});
        document.getElementById('workflow-operation-result').textContent='已确认 '+result.report.year+' 年 '+result.report.month+' 月排期范围，适用群 '+result.report.groups.join('、')+'；确认候选 '+result.report.confirmed_schedule_items+' 项。此操作没有登记任何实际已售事实。';workflowData=null;loadWorkflowData();}
      catch(error){toast('排期确认未保存',error.message||'请补齐年份和群范围');}
    });
    document.getElementById('save-source-account').addEventListener('click',async function(){
      try{var result=await workflowPost('/api/workflow/account',{id:document.getElementById('account-id').value,display_name:document.getElementById('account-name').value,rule_id:document.getElementById('account-rule').value,robot_id:document.getElementById('account-robot').value,enabled:false});
        toast('来源账号已登记为暂停配置',result.account.id+'；本次未登录或接收消息。');workflowData=null;loadWorkflowData();}
      catch(error){toast('来源账号保存失败',error.message||'请检查账号标识');}
    });
    document.getElementById('rebuild-case-suggestions').addEventListener('click',rebuildSuggestions);
    document.getElementById('load-more-suggestions').addEventListener('click',function(){loadSuggestions(false);});
    document.getElementById('suggestion-list').addEventListener('click',function(event){var row=event.target.closest('[data-suggestion-id]');if(row){selectedSuggestionId=row.dataset.suggestionId;renderSuggestionDetail();this.querySelectorAll('.suggestion-item').forEach(function(x){x.classList.toggle('selected',x.dataset.suggestionId===selectedSuggestionId);});}});
    document.querySelectorAll('[data-suggestion-status]').forEach(function(button){button.addEventListener('click',function(){suggestionStatus=this.dataset.suggestionStatus;document.querySelectorAll('[data-suggestion-status]').forEach(function(x){x.setAttribute('aria-pressed',String(x.dataset.suggestionStatus===suggestionStatus));});selectedSuggestionId='';loadSuggestions(true);});});
  }

  bindEvents();
  var currentDate = new Date().toLocaleDateString('sv-SE');
  document.getElementById('history-start-date').value = currentDate;
  document.getElementById('history-end-date').value = currentDate;
  renderSettings();
  renderRules();
  renderQueue();
  renderFaq();
  renderAudit();
  initializeWorkspace().then(function () {
    var initialView = new URLSearchParams(window.location.search).get('view');
    if (['inbox', 'processing', 'exports', 'replies', 'rules', 'faq', 'faq-candidates', 'copy', 'settings', 'history', 'dev-audit', 'business-data', 'suggestion-audit'].indexOf(initialView) !== -1) setView(initialView);
    else setView('processing');
  });
  loadReviewHistory();
  syncLive();
  loadQueue(true);
  window.setInterval(syncLive, 1500);
  window.setInterval(function () {
    var message = selectedMessage();
    if (activeView === 'inbox' && message && message.work_item && message.work_item.ai_state === 'pending' &&
        document.activeElement !== document.getElementById('work-draft') && document.activeElement !== document.getElementById('work-note')) {
      refreshWorkItem(message);
    }
  }, 5000);
})();


