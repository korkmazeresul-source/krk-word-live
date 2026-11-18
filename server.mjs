import express from 'express';
import {createServer} from 'node:http';
import {Server} from 'socket.io';
import * as TikTok from 'tiktok-live-connector';
import {timingSafeEqual} from 'node:crypto';
const app=express(), server=createServer(app), io=new Server(server);
const token=process.env.ADMIN_TOKEN;
const username=(process.env.TIKTOK_USERNAME||'').replace(/^@/,'');
let connection, state='idle', timer, attempts=0, generation=0;
const seen=new Map(), streaks=new Map();
function status(s){state=s;io.emit('status',{state:s});}
function valid(t){const a=Buffer.from(String(t||'')),b=Buffer.from(token||'');return !!b.length&&a.length===b.length&&timingSafeEqual(a,b);}
io.use((socket,next)=>valid(socket.handshake.auth.token)?next():next(new Error('Unauthorized')));
function unique(id){if(!id)return true;id=String(id);if(seen.has(id))return false;seen.set(id,Date.now());if(seen.size>10000)seen.delete(seen.keys().next().value);return true;}
function retry(g){if(g!==generation||timer)return;status('retrying');timer=setTimeout(()=>{timer=null;connect();},Math.min(60000,2000*2**Math.min(attempts++,5)));}
async function connect(){
 const g=++generation;
 if(!username||!token){status('configuration_required');return;}
 const C=TikTok.TikTokLiveConnection||TikTok.WebcastPushConnection;
 if(!C){status('connector_incompatible');return;}
 try{
 connection?.disconnect();const c=connection=new C(username);
 c.on('chat',d=>{if(g!==generation||!unique(d.msgId??d.common?.msgId))return;io.emit('chat',{user:d.uniqueId??d.user?.uniqueId??'unknown',comment:String(d.comment??'').slice(0,300)});});
 c.on('gift',d=>{if(g!==generation)return;const type=d.giftType??d.giftDetails?.giftType;if(type===1&&!d.repeatEnd)return;
 const id=d.msgId??d.common?.msgId;
 const group=d.groupId??d.group_id;const user=d.uniqueId??d.user?.uniqueId??'unknown';const gift=d.giftId??d.giftDetails?.giftId;
 if(!unique(id))return;
 if(group&&type===1){const key=[user,gift,group].join(':');if(streaks.has(key))return;streaks.set(key,true);if(streaks.size>10000)streaks.delete(streaks.keys().next().value);}
 io.emit('gift',{user,giftId:gift,name:d.giftName??d.giftDetails?.giftName??'Gift',count:Number(d.repeatCount??1),diamondCount:Number(d.diamondCount??d.giftDetails?.diamondCount??0)});});
 c.on('disconnected',()=>retry(g));c.on('streamEnd',()=>retry(g));c.on('error',()=>{if(g===generation)io.emit('notice','TikTok connector error');});
 status('connecting');await c.connect();if(g===generation){attempts=0;status('connected');}
 }catch(e){if(g===generation){io.emit('notice',String(e.message||e).slice(0,200));retry(g);}}
}
app.get('/health',(_,res)=>res.json({ok:true,state}));
app.use(express.static('public'));
io.on('connection',s=>s.emit('status',{state}));
server.listen(Number(process.env.PORT||3000),'0.0.0.0',()=>{console.log('KRK WORD LIVE ready');connect();});
