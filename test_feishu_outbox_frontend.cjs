const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function harness(){
  const elements=new Map(),calls=[];
  function element(id){if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',disabled:false,listeners:{},classList:{contains:()=>true},addEventListener(name,fn){this.listeners[name]=fn;}});return elements.get(id);}
  const context={document:{getElementById:element},window:{},setInterval(){},console,
    fetch:async(url,options={})=>{calls.push({url,options});return {ok:true,json:async()=>options.method==='POST'?{added:1,existing:0,errors:[],accepted:true}:{counts:{succeeded:1},items:[],running:false}};}};
  vm.createContext(context);vm.runInContext(fs.readFileSync('exports.js','utf8'),context);
  return {calls,element,load:()=>context.window.feishuExportUI.load()};
}
test('viewing export records only reads status and never uploads or prepares tasks',async()=>{
 const app=harness();await app.load();assert.equal(app.calls.length,1);assert.equal(app.calls[0].url,'/api/exports');assert.ok(!app.calls[0].options.method);
});
test('collecting confirmed tasks requires a click and does not start execution',async()=>{
 const app=harness();app.element('export-prepare').listeners.click();for(let i=0;i<4;i++)await tick();
 const posts=app.calls.filter(x=>x.options.method==='POST');assert.equal(posts.length,1);assert.equal(posts[0].url,'/api/exports/prepare');
});
test('explicit execution is limited to five queued records and has no message send call',async()=>{
 const app=harness();app.element('export-run').listeners.click();for(let i=0;i<4;i++)await tick();
 const posts=app.calls.filter(x=>x.options.method==='POST');assert.equal(posts.length,1);assert.equal(posts[0].url,'/api/exports/run');assert.deepEqual(JSON.parse(posts[0].options.body),{limit:5});
});
