const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const tick=()=>new Promise(resolve=>setImmediate(resolve));

function harness(search=''){
  const elements=new Map(),calls=[],removed=new Set(),delays=new Map();
  let rows=['a','b'];
  function element(id){
    if(elements.has(id))return elements.get(id);
    let html='';const x={value:'',textContent:'',hidden:false,scrollTop:0,disabled:false,dataset:{},listeners:{},children:[],
      classList:{contains:()=>true,toggle(){}},setAttribute(){},contains:()=>false,
      addEventListener(event,callback){this.listeners[event]=callback;},querySelectorAll(){return this.children;}};
    Object.defineProperty(x,'innerHTML',{get(){return html;},set(value){html=value;
      if(id==='core-detail'){
        const draft=value.match(/<textarea id="core-draft"[^>]*>([\s\S]*?)<\/textarea>/);
        element('core-draft').value=draft?draft[1]:'';element('core-note').value='';
        x.children=[...value.matchAll(/data-core-decision="([^"]+)"/g)].map(match=>({disabled:false,dataset:{coreDecision:match[1]},listeners:{},addEventListener(event,callback){this.listeners[event]=callback;}}));
      }
    }});elements.set(id,x);return x;
  }
  function detail(id){return {id:'s-'+id,case_id:id,status_label:'待人工处理',payload:{case_id:id,plan_fingerprint:'p-'+id,sender_name:'客户'+id,group_name:'测试群',intent:'qa',messages:[{sequence:id==='a'?1:2,content:'问题'+id,timestamp:'2026-10-21T10:00:00+08:00'}],processing_plan:{status:'human',response_needed:true,labels:['qa'],tasks:[],blockers:[],hard_blockers:[]}}};}
  const context={URLSearchParams,Map,Promise,Date,setInterval(){},setTimeout,clearTimeout,console,
    document:{getElementById:element,activeElement:null},window:{location:{search}},
    fetch:async(url,options={})=>{
      calls.push({url,options});
      if(options.method==='POST'){const payload=JSON.parse(options.body);removed.add(payload.case_id);return {ok:true,json:async()=>({ok:true})};}
      let result;
      if(url.includes('/summary'))result={states:{human:2},faq:{enabled:1,candidates:{pending:0}},missing_configuration:[],generation_signature:'g1'};
      else if(url.includes('/cases?'))result={items:rows.filter(id=>!removed.has(id)).map(id=>({id:'s-'+id,case_id:id,sender_name:'客户'+id,group_name:'测试群',status_label:'待人工处理',labels:['qa']})),total:rows.filter(id=>!removed.has(id)).length,has_more:false};
      else{
        const id=new URL(url,'http://localhost').searchParams.get('id');
        if(delays.has(id))await delays.get(id).promise;
        result={case:detail(id)};
      }
      return {ok:true,json:async()=>result};
    }};
  vm.createContext(context);vm.runInContext(fs.readFileSync('processing-core.js','utf8'),context);
  function click(id){const target={dataset:{coreCase:id},closest(){return this;}};return element('core-list').listeners.click({target});}
  return {context,calls,element,click,delay(id){let resolve;const promise=new Promise(r=>{resolve=r;});delays.set(id,{promise,resolve});return resolve;},async load(){await context.window.processingCore.load();await tick();await tick();}};
}

test('case switching reuses pending reads and ignores stale details without calling AI',async()=>{
  const app=harness();const release=app.delay('b');await app.load();
  assert.match(app.element('core-detail').innerHTML,/客户a/);
  app.click('b');app.click('a');release();await tick();await tick();
  assert.match(app.element('core-detail').innerHTML,/客户a/);
  assert.equal(app.calls.filter(x=>x.options.method==='POST').length,0);
  assert.equal(app.calls.filter(x=>x.url==='/api/processing/case?id=b').length,1);
});

test('a local processing decision uses the visible plan version and advances the queue',async()=>{
  const app=harness();await app.load();
  app.element('core-draft').value='人工核对后的答复';app.element('core-draft').listeners.input();
  const confirm=app.element('core-detail').children.find(x=>x.dataset.coreDecision==='confirmed');
  confirm.listeners.click();for(let i=0;i<8;i++)await tick();
  const posts=app.calls.filter(x=>x.options.method==='POST');assert.equal(posts.length,1);
  assert.equal(posts[0].url,'/api/processing/decision');
  const body=JSON.parse(posts[0].options.body);assert.equal(body.case_id,'a');assert.equal(body.plan_fingerprint,'p-a');
  assert.equal(body.draft,'人工核对后的答复');assert.match(app.element('core-detail').innerHTML,/客户b/);
  assert.ok(!app.calls.some(x=>x.url.includes('/api/suggestion/review')));
});

test('an unsaved reply draft survives navigating to another conversation and back',async()=>{
  const app=harness();await app.load();
  app.element('core-draft').value='尚未保存的人工草稿';app.element('core-draft').listeners.input();
  app.click('b');await tick();await tick();app.click('a');await tick();await tick();
  assert.equal(app.element('core-draft').value,'尚未保存的人工草稿');
});

test('conflict review link opens the dedicated queue and requested customer without posting a decision',async()=>{
  const app=harness('?view=processing&filter=classification_conflict&case=b');await app.load();
  assert.equal(app.element('core-filter').value,'classification_conflict');
  assert.ok(app.calls.some(x=>x.url.includes('status=classification_conflict')));
  assert.match(app.element('core-detail').innerHTML,/客户b/);
  assert.equal(app.calls.filter(x=>x.options.method==='POST').length,0);
});
