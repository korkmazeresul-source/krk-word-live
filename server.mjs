import express from 'express';
import {createServer} from 'node:http';
import {Server} from 'socket.io';
import { TikTokLiveConnection } from 'tiktok-live-connector';
import {timingSafeEqual} from 'node:crypto';
const app=express(), server=createServer(app), io=new Server(server);
const token=process.env.ADMIN_TOKEN;
let username=(process.env.TIKTOK_USERNAME||'').trim().replace(/^@/,'');
function report(e){const raw=String(e?.message||e||'Unknown error');const safe=raw.replace(/https?:\/\/[^\s]+/gi,'[URL]').replace(/(?:token|cookie|sessionid|api[_-]?key|authorization)\s*[:=]\s*[^\s,;]+/gi,'[REDACTED]');console.warn('[TikTok]',token?safe.split(token).join('[REDACTED]').slice(0,300):safe.slice(0,300));}
let lastError='';
let connection, state='idle', timer, attempts=0, generation=0;
const seen=new Map(), streaks=new Map();const metrics={chat:0,gift:0,duplicates:0,lastChat:null,lastGift:null};let sequence=0;const backlog=[];function publish(kind,data){const event={...data,eventId:++sequence};backlog.push({kind,event});if(backlog.length>1000)backlog.shift();io.emit(kind,event);metrics[kind]++;metrics[kind==='chat'?'lastChat':'lastGift']=new Date().toISOString();}
function snapshot(){return {state,username,error:lastError};}
function status(s){state=s;io.emit('status',snapshot());}
function explain(e){const m=String(e?.message||e).toLowerCase();if(/offline|not live|not currently live|live has ended/.test(m))return 'Hesap şu anda canlı yayında değil.';if(/429|rate limit/.test(m))return 'TikTok istek sınırı; tekrar deneniyor.';if(/403|captcha|blocked|sign|webcast/.test(m))return 'TikTok erişim veya imzalama hatası; Render Logs kontrol edilmeli.';if(/404|not found|user.*exist/.test(m))return 'Hesap bulunamadı; kullanıcı adını kontrol edin.';return 'TikTok bağlantı hatası; Render Logs kontrol edilmeli.';}
function valid(t){const a=Buffer.from(String(t||'')),b=Buffer.from(token||'');return !!b.length&&a.length===b.length&&timingSafeEqual(a,b);}
io.use((socket,next)=>valid(socket.handshake.auth.token)?next():next(new Error('Unauthorized')));
function unique(id){if(!id)return true;id=String(id);if(seen.has(id)){metrics.duplicates++;return false;}seen.set(id,Date.now());if(seen.size>10000)seen.delete(seen.keys().next().value);return true;}
function changeAccount(name){if(name===username)return;username=name;generation++;clearTimeout(timer);timer=null;attempts=0;lastError='';status('connecting');connect();}
function retry(g){if(g!==generation||timer)return;status('retrying');timer=setTimeout(()=>{timer=null;connect();},(/istek sınırı/i.test(lastError)?Math.min(1800000,300000*2**Math.min(attempts++,3)):30000));}
async function connect(){
 const g=++generation;
 if(!username||!token){status('configuration_required');return;}
 if(typeof TikTokLiveConnection!=='function'){status('connector_incompatible');return;}
 try{
 if(connection)await Promise.resolve(connection.disconnect()).catch(()=>{});if(g!==generation)return;console.log('[TikTok] Connecting to @'+username);const c=connection=new TikTokLiveConnection(username,{processInitialData:false});
 c.on('connected',()=>{if(g!==generation)return;clearTimeout(timer);timer=null;attempts=0;lastError='';status('connected');});
 c.on('chat',d=>{if(g!==generation||!unique((d.msgId??d.common?.msgId)?'chat:'+(d.msgId??d.common?.msgId):null))return;const user=d.uniqueId??d.user?.uniqueId??d.user?.username;const comment=String(d.comment??'').trim().slice(0,300);if(user&&comment)publish('chat',{user:String(user),comment});});
 c.on('gift',d=>{if(g!==generation)return;
 const type=Number(d.giftType??d.giftDetails?.giftType??0),end=d.repeatEnd===true||d.repeatEnd===1||d.repeatEnd==='1';
 const user=d.uniqueId??d.user?.uniqueId??d.user?.username;const gift=d.giftId??d.giftDetails?.giftId;const count=Math.floor(Number(d.repeatCount??1)),unit=Number(d.diamondCount??d.giftDetails?.diamondCount??d.gift?.diamond_count??0);
 if(!user||!Number.isSafeInteger(count)||count<1||!Number.isFinite(unit)||unit<=0)return;
 const name=d.giftName??d.giftDetails?.giftName??'Gift';const group=d.groupId??d.group_id;
 if(type===1&&!end){io.emit('giftProgress',{user:String(user),giftId:gift,name,count,diamondCount:unit});return;}
 const id=d.msgId??d.common?.msgId;if(!unique(id?'gift:'+id:null))return;
 if(group&&type===1){const key=[user,gift,group].join(':');if(streaks.has(key)){metrics.duplicates++;return;}streaks.set(key,true);if(streaks.size>10000)streaks.delete(streaks.keys().next().value);}
 publish('gift',{user:String(user),giftId:gift,name,count,diamondCount:unit});});
 c.on('disconnected',()=>{if(g===generation)console.log('[TikTok] Disconnected');retry(g)});c.on('streamEnd',()=>{if(g===generation)console.log('[TikTok] Stream ended');retry(g)});c.on('error',e=>{if(g===generation){lastError=explain(e);report(e);io.emit('notice',lastError);}});
 status('connecting');await c.connect();if(g===generation){attempts=0;lastError='';console.log('[TikTok] Connected');status('connected');}
 }catch(e){if(g===generation){lastError=explain(e);report(e);io.emit('notice',lastError);retry(g);}}
}
app.get('/health',(_,res)=>res.json({ok:true,state,version:31}));
app.use(express.static('public'));
io.on('connection',s=>{const name=String(s.handshake.auth.username||'').trim().replace(/^@/,'').toLowerCase();if(name&&/^[a-z0-9_.]{1,24}$/.test(name))changeAccount(name);s.emit('status',snapshot());s.on('diagnostics',()=>s.emit('diagnostics',{...snapshot(),...metrics}));});
server.listen(Number(process.env.PORT||3000),'0.0.0.0',()=>{console.log('KRK WORD LIVE ready');connect();});
