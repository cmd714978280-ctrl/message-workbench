(function(){
  'use strict';
  var busy=false,loading=false;
  var $=function(id){return document.getElementById(id);};
  var PHASH_VERSION='miaoda-canvas-dct32-v1';
  var N=32;
  var CM=(function(){
    var values=[];
    for(var k=0;k<N;k++){
      values.push([]);
      var scale=k===0?Math.sqrt(1/N):Math.sqrt(2/N);
      for(var x=0;x<N;x++)values[k].push(scale*Math.cos((2*x+1)*k*Math.PI/(2*N)));
    }
    return values;
  }());
  function esc(value){return String(value==null?'':value).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  async function request(url,body){
    var response=await fetch(url,body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{cache:'no-store'});
    var data=await response.json();
    if(!response.ok)throw new Error(data.error||'入库请求未完成');
    return data;
  }
  function notice(text){$('export-notice').textContent=text;}
  function controls(){['export-prepare','export-run','export-retry'].forEach(function(id){$(id).disabled=busy;});}
  function canvasPhash(image){
    var canvas=document.createElement('canvas');canvas.width=N;canvas.height=N;
    var context=canvas.getContext('2d',{willReadFrequently:true});
    if(!context)throw new Error('浏览器无法创建图片画布');
    context.drawImage(image,0,0,N,N);
    var data=context.getImageData(0,0,N,N).data;
    var pixels=new Array(N);
    for(var y=0;y<N;y++){
      pixels[y]=new Array(N);
      for(var x=0;x<N;x++){
        var i=(y*N+x)*4;
        pixels[y][x]=0.299*data[i]+0.587*data[i+1]+0.114*data[i+2];
      }
    }
    var temp=new Array(N),out=new Array(N);
    for(var u=0;u<N;u++){
      temp[u]=new Array(N);
      for(var v=0;v<N;v++){
        var sum=0;
        for(var row=0;row<N;row++)sum+=CM[u][row]*pixels[row][v];
        temp[u][v]=sum;
      }
    }
    for(var u2=0;u2<N;u2++){
      out[u2]=new Array(N);
      for(var v2=0;v2<N;v2++){
        var sum2=0;
        for(var x2=0;x2<N;x2++)sum2+=temp[u2][x2]*CM[v2][x2];
        out[u2][v2]=sum2;
      }
    }
    var low=[];
    for(var u3=0;u3<8;u3++)for(var v3=0;v3<8;v3++)low.push(out[u3][v3]);
    var sorted=low.slice().sort(function(a,b){return a-b;});
    var median=sorted[31],bits='';
    for(var bit=0;bit<64;bit++)bits+=(low[bit]>median)?'1':'0';
    var hex='';
    for(var offset=0;offset<64;offset+=4)hex+=parseInt(bits.substr(offset,4),2).toString(16);
    return '0x'+hex;
  }
  function hashImage(candidate){
    return new Promise(function(resolve,reject){
      var image=new Image();
      image.onload=function(){try{resolve(canvasPhash(image));}catch(error){reject(error);}};
      image.onerror=function(){reject(new Error('图片 '+candidate.media_id+' 读取失败'));};
      image.src=candidate.image_url+'?phash_version='+encodeURIComponent(PHASH_VERSION);
    });
  }
  function hashesHtml(values){
    return (values||[]).map(function(value){
      if(!value.sha256&&!value.phash)return '';
      return '<details class="export-hash"><summary>图片指纹</summary><p>妙搭 pHash：'+esc(value.phash||'尚未计算')+'</p><p>文件 SHA-256：'+esc(value.sha256||'')+'</p></details>';
    }).join('');
  }
  async function load(){
    if(loading)return;loading=true;
    try{
      var data=await request('/api/exports');busy=Boolean(data.running);controls();
      var counts=data.counts||{};
      $('export-summary').textContent='待入库 '+Number(counts.pending||0)+' · 入库成功 '+Number(counts.succeeded||0)+' · 失败或待核对 '+Number((counts.failed||0)+(counts.uncertain||0))+' · 资料已变化 '+Number(counts.stale||0)+' · 旧图库指纹待回补 '+Number(data.legacy_hash_repair_required||0)+(busy?' · 正在执行':'');
      $('export-list').innerHTML=(data.items||[]).map(function(item){
        return '<article class="panel export-record"><div class="export-heading"><strong>'+esc(item.customer)+' · '+esc(item.group_name)+'</strong><span class="core-status">'+esc(item.state_label)+'</span></div><p>'+esc(item.route_label)+' · 已尝试 '+Number(item.attempts)+' 次</p>'+
          (item.caption?'<p class="export-caption">'+esc(item.caption)+'</p>':'')+
          '<div class="suggestion-media-grid">'+(item.media_ids||[]).map(function(id){return '<a class="suggestion-media" href="/api/media/'+Number(id)+'" target="_blank" rel="noopener"><img src="/api/media/'+Number(id)+'" alt="待入库图片" loading="lazy"></a>';}).join('')+'</div>'+hashesHtml(item.media_hashes)+
          (item.error?'<p class="core-blocker">'+esc(item.error)+'</p>':'')+
          '<div class="core-actions"><a class="button button-quiet" href="'+esc(item.target_url)+'" target="_blank" rel="noopener">查看飞书目标表</a>'+(['failed','uncertain'].indexOf(item.state)>=0?'<button class="button button-secondary" data-export-retry="'+esc(item.id)+'" '+(busy?'disabled':'')+'>重试并核对结果</button>':'')+'</div>'+
          (item.remote_record_id?'<details><summary>入库凭据</summary><p>飞书记录编号：'+esc(item.remote_record_id)+'</p><p>最近核验：'+esc(item.updated_at)+'</p></details>':'')+'</article>';
      }).join('')||'<div class="panel export-record">暂无入库任务。点击“收集已确认任务”，将已确认的图库、订单图和需求加入队列。</div>';
    }catch(error){notice('读取入库记录失败：'+error.message);}finally{loading=false;}
  }
  async function prepare(){
    if(busy)return;busy=true;controls();
    try{
      var candidates=await request('/api/exports/hash-candidates');
      var images=candidates.items||[],phashes=[];
      for(var i=0;i<images.length;i++){
        notice('正在按妙搭原算法计算图片指纹 '+(i+1)+'/'+images.length);
        phashes.push({media_id:images[i].media_id,sha256:images[i].sha256,phash:await hashImage(images[i])});
      }
      var data=await request('/api/exports/prepare',{phashes:phashes});
      var notes=[];
      if(data.added)notes.push('新增 '+Number(data.added)+' 项');
      if(data.existing)notes.push('已有或更新 '+Number(data.existing)+' 项');
      if(images.length)notes.push('已计算 '+images.length+' 张妙搭兼容指纹');
      if((data.errors||[]).length)notes.push('部分未入队：'+data.errors.map(function(x){return x.error;}).join('；'));
      if(Number(data.legacy_hash_repair_required||0))notes.push('另有 '+Number(data.legacy_hash_repair_required)+' 条旧图库记录仍需单独回补，尚未改写');
      notice(notes.join('；')||'没有新增入库任务。');
    }catch(error){notice('收集入库任务失败：'+error.message);}finally{busy=false;await load();}
  }
  async function act(path,body){
    if(busy)return;busy=true;controls();
    try{
      var data=await request(path,body);
      notice('入库执行已启动，结果会自动刷新。');
    }catch(error){notice(error.message);}finally{busy=false;await load();}
  }
  $('export-prepare').addEventListener('click',prepare);
  $('export-run').addEventListener('click',function(){act('/api/exports/run',{limit:5});});
  $('export-retry').addEventListener('click',function(){act('/api/exports/run',{limit:5,retry:true});});
  $('export-refresh').addEventListener('click',load);
  $('export-list').addEventListener('click',function(event){var button=event.target.closest('[data-export-retry]');if(button)act('/api/exports/run',{job_id:button.dataset.exportRetry,retry:true});});
  window.feishuExportUI={load:load};
  setInterval(function(){if($('view-exports').classList.contains('active'))load();},4000);
}());
