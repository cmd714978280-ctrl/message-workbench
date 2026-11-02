(function () {
  'use strict';
  var state={items:[],selected:'',status:'todo',search:'',total:0,more:false,loading:false,request:0,detailRequest:0,generation:'',loaded:false};
  var cache=new Map(),edits=new Map(),pending=new Map();
  var labels={service:'客服服务',requirement:'产品需求',qa:'日常答疑',product_link:'购买链接',warning:'广告或违规',mini:'小程序',burst:'刷屏',closing:'无需回复',ordinary:'普通交流',feedback:'反馈图',order_image:'订单图',thanks:'感谢回应',unknown:'待判断'};
  var byId=function(id){return document.getElementById(id);};
  var initialParams=new URLSearchParams(window.location?window.location.search:'');
  if(initialParams.get('filter')==='classification_conflict'){
    state.status='classification_conflict';byId('core-filter').value=state.status;
    state.selected=initialParams.get('case')||'';
  }
  function esc(value){return String(value==null?'':value).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function displayContent(value){return String(value||'').replace(/\\\//g,'/').replace(/<a\b[^>]*>/gi,'').replace(/<\/a\s*>/gi,'').trim();}
  function active(){return byId('view-processing').classList.contains('active');}
  async function request(url,body){var response=await fetch(url,body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{cache:'no-store'});var data=await response.json();if(!response.ok)throw new Error(data.error||'请求未完成');return data;}
  function message(text){byId('core-notice').textContent=text;byId('core-notice').hidden=false;}
  function media(value){var id=Number(value.id);if(!Number.isInteger(id)||id<=0)return '';if(value.state!=='stored')return '<span class="suggestion-media-missing">图片未完整接收，待补收</span>';var url='/api/media/'+id;return '<a class="suggestion-media core-preview-image" href="'+url+'" data-image-preview="'+url+'" target="_blank" rel="noopener" aria-label="点击查看原图"><img src="'+url+'" alt="对话图片缩略图" loading="lazy"></a>';}
  function list(){
    var host=byId('core-list'),scroll=host.scrollTop;
    host.innerHTML=state.items.map(function(item){return '<button class="message-row core-row '+(item.case_id===state.selected?'selected':'')+'" data-core-case="'+esc(item.case_id)+'"><span class="row-main"><span class="row-heading"><strong>'+esc(item.sender_name||'客户昵称待补')+'</strong><span class="core-status">'+esc(item.status_label)+'</span></span><span class="row-meta">'+esc(item.group_name||'群名称待补')+' · '+esc(item.business_date||'日期待核对')+'</span><span class="row-preview">'+esc(displayContent(item.preview)||'图片或非文字消息')+'</span><span class="row-bottom">'+'分类：'+esc((item.labels||[]).map(function(x){return labels[x]||'待判断';}).join('、')||'待判断')+' · '+(item.classification_matched_count?'真人回复已核对 '+Number(item.classification_matched_count)+' 条':'未由真人回复自动核对')+' · '+Number(item.message_count||0)+' 条消息'+(item.classification_conflict_count?' · '+Number(item.classification_conflict_count)+' 条分类冲突':'')+'</span></span></button>';}).join('')||'<div class="queue-empty">当前筛选下没有对话。可切换“全部”查看已整理记录。</div>';
    host.scrollTop=scroll;byId('core-count').textContent='共 '+state.total+' 段对话';byId('core-more').hidden=!state.more;
  }
  async function loadList(append){
    var epoch=++state.request;state.loading=true;byId('core-list').setAttribute('aria-busy','true');
    var query=new URLSearchParams({status:state.status,limit:'80',offset:String(append?state.items.length:0),search:state.search});
    try{var data=await request('/api/processing/cases?'+query);if(epoch!==state.request)return;
      state.items=append?state.items.concat(data.items||[]):data.items||[];state.total=data.total;state.more=data.has_more;state.loaded=true;
      if(!state.items.some(function(x){return x.case_id===state.selected;}))state.selected=state.items[0]&&state.items[0].case_id||'';
      list();if(state.selected)select(state.selected);else byId('core-detail').innerHTML='<div class="detail-placeholder"><div><strong>当前没有待处理对话</strong><span>已有真实回复和无需回复的对话可在对应筛选中查看。</span></div></div>';
    }catch(error){if(epoch===state.request)message('读取处理列表失败：'+error.message);}
    finally{if(epoch===state.request){state.loading=false;byId('core-list').setAttribute('aria-busy','false');}}
  }
  async function getDetail(id){
    var row=state.items.find(function(x){return x.case_id===id;}),known=cache.get(id);
    if(known&&row&&known.value.id===row.id&&Date.now()-known.at<30000)return known.value;
    if(pending.has(id))return pending.get(id);
    var promise=request('/api/processing/case?id='+encodeURIComponent(id)).then(function(data){cache.set(id,{value:data.case,at:Date.now()});if(cache.size>100)cache.delete(cache.keys().next().value);return data.case;}).finally(function(){pending.delete(id);});
    pending.set(id,promise);return promise;
  }
  async function select(id){
    state.selected=id;var epoch=++state.detailRequest;
    byId('core-list').querySelectorAll('[data-core-case]').forEach(function(button){button.classList.toggle('selected',button.dataset.coreCase===id);});
    byId('core-detail').setAttribute('aria-busy','true');
    try{var item=await getDetail(id);if(epoch!==state.detailRequest||state.selected!==id)return;detail(item);
      var index=state.items.findIndex(function(x){return x.case_id===id;});state.items.slice(index+1,index+3).forEach(function(next){getDetail(next.case_id).catch(function(){});});
    }catch(error){if(epoch===state.detailRequest)message('读取对话失败：'+error.message);}
    finally{if(epoch===state.detailRequest)byId('core-detail').setAttribute('aria-busy','false');}
  }
  function detail(item){
    var p=item.payload||{},plan=p.processing_plan||{},scope=p.business_context||{};
    var saved=edits.get(item.case_id),draft=saved?saved.draft:item.operator_draft||((plan.tasks||[]).find(function(t){return t.draft;})||{}).draft||'';
    var note=saved?saved.note:item.operator_note||'';
    var timeline=(p.messages||[]).map(function(x){return Object.assign({direction:'inbound'},x);}).concat(((p.reply_context||{}).messages||[]).filter(function(x){return x.reply_to_case;}));
    timeline.sort(function(a,b){return (Date.parse(a.timestamp)||0)-(Date.parse(b.timestamp)||0)||a.sequence-b.sequence;});
    var threads=timeline.map(function(x){var outboundLabel=x.reply_link_type==='direct_reply'||x.reply_link_type==='quoted'?'明确引用关联 · 回复对照':x.reply_link_type==='recipient_inferred'?'按唯一收件人推定关联 · 回复对照':x.reply_link_type==='recipient_ambiguous'?'发给该客户但对应问题不明确':'群内消息（上下文）';return '<div class="conversation-context-item '+(x.direction==='outbound'?'outbound':'inbound')+'"><small>'+esc(x.direction==='outbound'?outboundLabel:x.sender_name||p.sender_name||'客户')+' · '+esc(String(x.timestamp||'').replace('T',' ').replace('+08:00',''))+(x.revoked?' · 已撤回':'')+'</small><div>'+esc(displayContent(x.content)||'图片或非文字消息')+'</div><div class="suggestion-media-grid">'+(x.media||[]).map(media).join('')+'</div></div>';}).join('');
    var tasks=(plan.tasks||[]).map(function(t){return '<article class="core-task"><strong>'+esc(t.title)+'</strong>'+(t.media_id?'<div class="suggestion-media-grid">'+media({id:t.media_id,state:(p.image_actions||[]).find(function(m){return m.media_id===t.media_id;})?.state||'pending'})+'</div>':'')+(t.destination_url?'<p><a href="'+esc(t.destination_url)+'" target="_blank" rel="noopener">查看目标位置</a></p>':'')+(t.note?'<p>'+esc(t.note)+'</p>':'')+(t.blockers||[]).map(function(reason){return '<div class="core-blocker">'+esc(reason)+'</div>';}).join('')+(t.media_id&&['feedback','order_image','service_evidence'].indexOf(t.route)>=0&&!(t.blockers||[]).length?'<button class="button button-secondary" data-core-approve-image="'+esc(t.key)+'">确认此图片分类</button><p class="core-muted">确认后到飞书入库页收集和执行。</p>':'')+'</article>';}).join('')||'<p class="core-muted">'+(plan.status==='answered'?'已关联真实回复，保留为对照依据。':'无需群回复；原始消息继续保留。')+'</p>';
    var refs=(p.knowledge_refs||[]).map(function(x){return '<details><summary>'+esc(x.question)+'</summary><p>'+esc(x.answer)+'</p></details>';}).join('');
    var compare=p.faq_comparison||{};
    var audits=(p.turns||[]).filter(function(t){return t.classification_audit&&t.classification_audit.reply_sequences&&t.classification_audit.reply_sequences.length;}).map(function(t){return '<p>消息 #'+Number(t.sequence)+'：'+esc(t.classification_audit.label)+'</p>';}).join('');
    audits+=(p.turns||[]).filter(function(t){return t.service_contact_source&&t.service_contact_source.state==='observed_group_reply';}).map(function(t){return '<p>客服链接来自本群同日的真人回复 #'+esc((t.service_contact_source.reply_sequences||[]).join('、'))+'；使用前请核对是否仍有效。</p>';}).join('');
    var conflicts=(p.turns||[]).filter(function(turn){return turn.requires_classification_review;});
    var classification=conflicts.length?'<section><h3>规则冲突待复核</h3><p class="core-muted">只核对下面这条消息的分类与标签；保存后，后台更新会将它移出冲突列表。</p>'+conflicts.map(function(turn){var original=(p.messages||[]).find(function(x){return x.sequence===turn.sequence;})||{};return '<article class="core-task"><p><strong>原消息：'+esc(original.content||'图片或非文字消息')+'</strong></p><div class="suggestion-media-grid">'+(original.media||[]).map(media).join('')+'</div><p>消息 #'+Number(turn.sequence)+'：历史人工分类为“'+esc(labels[turn.primary]||'待判断')+'”（标签：'+esc((turn.labels||[]).map(function(x){return labels[x]||'待判断';}).join('、'))+'），当前规则为“'+esc(labels[turn.machine_intent]||'待判断')+'”（标签：'+esc((turn.machine_labels||[]).map(function(x){return labels[x]||'待判断';}).join('、'))+'）。仅核对本条分类；完整对话中的真实回复继续作为参考。</p><div class="core-actions"><button class="button button-secondary" data-core-class-sequence="'+Number(turn.sequence)+'" data-core-class-choice="machine">确认采用当前规则分类</button><button class="button button-quiet" data-core-class-sequence="'+Number(turn.sequence)+'" data-core-class-choice="human">保留历史人工分类，并确认已复核</button></div></article>';}).join('')+'</section>':'';
    var canAI=plan.response_needed&&['qa','service','requirement','product_link'].indexOf(p.intent)>=0&&!draft;
    byId('core-detail').innerHTML='<div class="core-detail-body"><div class="detail-head"><div><h2>'+esc(p.sender_name||'客户昵称待补')+'</h2><p>'+esc(p.group_name||'群名称待补')+'</p></div><span class="core-status">'+esc(item.status_label)+'</span></div><div class="core-scope"><strong>'+esc(scope.business_date||'日期待核对')+' · '+esc((scope.matched_products||[]).join('、')||'商品尚未唯一确定')+'</strong><span>类别：'+esc((plan.labels||[]).map(function(x){return labels[x]||'待核对';}).join('、'))+' · 承办：'+esc({aftercare:'一对一客服',group_human:'群内人工',reviewer:'人工审核'}[plan.assignee]||'人工审核')+'</span></div>'+
      classification+'<section class="conversation-context"><h3>客户对话与真实回复</h3>'+threads+'</section><section><h3>处理任务</h3>'+tasks+'</section>'+
      '<details class="core-evidence"><summary>分类及 FAQ 与真人回复对照 · '+esc(compare.label||'待整理')+'</summary>'+audits+'<p>'+esc(compare.note||'')+'</p>'+refs+(scope.daily_plan_title?'<p>当天子表：'+esc(scope.daily_plan_title)+'</p>':'')+(scope.daily_copy_excerpt?'<pre>'+esc(scope.daily_copy_excerpt)+'</pre>':'')+'<p>来源文案仅作参考；未确认的价格、库存和占位链接不能直接采用。</p></details>'+
      (item.processing_state==='stale'?'<div class="core-blocker">资料或任务已变化，旧处理意见仍保留，请查看新建议后重新确认。</div>':'')+
      '<section class="core-editor"><h3>本地处理意见</h3>'+(plan.response_needed?'<label class="field"><span>推荐回复（可编辑）</span><textarea id="core-draft" rows="4" placeholder="确认事实后填写回复；保存只记录本地意见。">'+esc(draft)+'</textarea></label>':'<input id="core-draft" type="hidden" value="">')+
      '<label class="field"><span>处理说明</span><textarea id="core-note" rows="2" placeholder="标记已人工处理时，请说明完成了什么。">'+esc(note)+'</textarea></label><div class="core-actions"><button class="button button-primary" data-core-decision="confirmed" '+((plan.hard_blockers||plan.blockers||[]).length?'disabled title="请先补齐资料或转人工"':'')+'>确认建议，仅保存</button><button class="button button-secondary" data-core-decision="human">转人工</button><button class="button button-secondary" data-core-decision="waiting">等待资料</button><button class="button button-quiet" data-core-decision="handled">已人工处理</button></div>'+(canAI?'<button class="button button-secondary core-ai" id="core-ai">调用 DeepSeek 生成参考回复</button><small>点击才会调用文字 API；结果仍需人工确认。</small>':'')+'<p class="core-muted">FAQ 对照结果与日常处理记录分别保存。确认建议只保存本地，不代表已发送、撤回或入库。</p></section></div>';
    byId('core-detail').querySelectorAll('[data-core-approve-image]').forEach(function(button){button.addEventListener('click',async function(){button.disabled=true;try{await request('/api/exports/approve-image',{case_id:item.case_id,suggestion_id:item.id,plan_fingerprint:p.plan_fingerprint,task_key:button.dataset.coreApproveImage});button.textContent='此图片已确认';message('图片分类已确认，请在飞书入库页收集并执行。');}catch(error){button.disabled=false;message('图片确认失败：'+error.message);}});});
    function retain(){edits.set(item.case_id,{draft:byId('core-draft').value,note:byId('core-note').value});}
    byId('core-draft').addEventListener('input',retain);byId('core-note').addEventListener('input',retain);
    byId('core-detail').querySelectorAll('[data-core-decision]').forEach(function(button){button.addEventListener('click',function(){save(item,button.dataset.coreDecision);});});
    if(conflicts.length)byId('core-detail').querySelectorAll('[data-core-class-sequence]').forEach(function(button){button.addEventListener('click',async function(){
      var turn=conflicts.find(function(value){return value.sequence===Number(button.dataset.coreClassSequence);});if(!turn)return;
      button.disabled=true;var machine=button.dataset.coreClassChoice==='machine';
      try{await request('/api/review',{case_id:item.case_id,suggestion_id:item.id,plan_fingerprint:p.plan_fingerprint,rule_version:(p.versions||{}).rules,sequence:turn.sequence,status:'approved',primary:machine?turn.machine_intent:turn.primary,labels:machine?turn.machine_labels:turn.labels,reason:machine?'在信息处理页复核后采用当前规则分类':'在信息处理页保留历史人工分类，并确认已按当前规则复核'});cache.delete(item.case_id);message('分类复核已保存，后台将更新处理计划。');}
      catch(error){button.disabled=false;message('分类复核未保存：'+error.message);}
    });});
    if(canAI)byId('core-ai').addEventListener('click',async function(){this.disabled=true;message('正在调用 DeepSeek，结果仅供本地参考…');try{await request('/api/case/ai-preview',{case_id:item.case_id});cache.delete(item.case_id);edits.delete(item.case_id);await loadList(false);message('参考建议已生成，请核对后保存。');}catch(error){message(error.message);this.disabled=false;}});
  }
  async function save(item,decision){
    var buttons=byId('core-detail').querySelectorAll('[data-core-decision]');buttons.forEach(function(x){x.disabled=true;});
    try{await request('/api/processing/decision',{case_id:item.case_id,suggestion_id:item.id,plan_fingerprint:item.payload.plan_fingerprint,decision:decision,draft:byId('core-draft').value,note:byId('core-note').value});
      edits.delete(item.case_id);cache.delete(item.case_id);message('处理意见已保存到本机。');await Promise.all([loadList(false),summary()]);
    }catch(error){message('未保存：'+error.message);buttons.forEach(function(x){x.disabled=false;});}
  }
  async function summary(){
    try{var data=await request('/api/processing/summary'),s=data.states||{};
      byId('core-summary').innerHTML=[['待处理',(s.human||0)+(s.ready||0)+(s.needs_configuration||0)+(s.stale||0)+(s.unprocessed||0)],['待补资料',s.needs_configuration||0],['已有真实回复',s.answered||0],['无需回复',s.no_reply||0]].map(function(x){return '<div class="core-stat"><span>'+x[0]+'</span><strong>'+x[1]+'</strong></div>';}).join('');
      byId('core-conflicts').textContent='规则冲突待复核 · '+Number((data.classification_conflicts||{}).messages||0)+' 条';
      if(data.needs_initial_build)byId('core-summary').innerHTML='<span>已有 '+Number((data.counts||{}).inbound_events||0)+' 条客户消息，正在后台整理处理计划…</span>';
      byId('core-runtime').textContent=(data.message_sync_enabled?'消息同步已启用':'消息同步已暂停')+' · 文字 AI 手动调用 · 处理结果保存在本机';
      var missing=data.missing_configuration||[];
      byId('core-readiness').innerHTML='<summary>资料缺口 '+missing.length+' 项 · FAQ 启用 '+Number(data.faq.enabled)+' 条 · 待确认候选 '+Number(data.faq.candidates.pending||0)+' 条</summary>'+missing.map(function(x){return '<p><a href="?view='+esc(x.view)+'">'+esc(x.label)+'</a>：'+esc(x.affects)+'</p>';}).join('')+(data.business_targets||[]).filter(function(x){return x.url;}).map(function(x){return '<p><a href="'+esc(x.url)+'" target="_blank" rel="noopener">'+esc(x.name)+'</a>：'+(x.verification&&x.verification.schema_ready?'字段已核验，业务记录待人工确认后登记':'地址已配置，字段待核验')+'</p>';}).join('')+((data.customer_service||{}).faq_fallback_enabled?'<p>客服入口：本群专属配置优先，其次读取启用 FAQ 中用途一致且唯一的链接。</p>':'')+(data.failed_media?'<p>另有 '+Number(data.failed_media)+' 个图片附件未完整接收。</p>':'')+'<p>'+esc(data.audit_note)+'</p>';
      if(data.job&&data.job.state==='running')message('正在后台更新处理计划，当前列表仍可查看。');
      var generation=data.generation_signature||'';
      if(state.loaded&&state.generation&&generation!==state.generation&&!(document.activeElement&&byId('core-detail').contains(document.activeElement))){cache.clear();loadList(false);}
      if(!state.loaded||!state.generation||!(document.activeElement&&byId('core-detail').contains(document.activeElement)))state.generation=generation;
    }catch(error){message('处理状态读取失败：'+error.message);}
  }
  window.processingCore={load:function(){return Promise.all([summary(),state.loaded?Promise.resolve():loadList(false)]);}};
  byId('core-list').addEventListener('click',function(event){var button=event.target.closest('[data-core-case]');if(button)select(button.dataset.coreCase);});
  byId('core-filter').addEventListener('change',function(){state.status=this.value;loadList(false);});
  byId('core-conflicts').addEventListener('click',function(){state.status='classification_conflict';state.search='';byId('core-search').value='';byId('core-filter').value=state.status;cache.clear();loadList(false);});
  var searchTimer;byId('core-search').addEventListener('input',function(){clearTimeout(searchTimer);searchTimer=setTimeout(function(){state.search=byId('core-search').value;loadList(false);},250);});
  byId('core-more').addEventListener('click',function(){if(!state.loading)loadList(true);});
  byId('core-refresh').addEventListener('click',function(){cache.clear();summary();loadList(false);});
  setInterval(function(){if(active())summary();},10000);
}());
