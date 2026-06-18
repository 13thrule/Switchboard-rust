const WebSocket = require('ws');

const url = 'ws://localhost:7777';
const topic = 'ci_demo';
const payload = 'hello-ci';

function makeSubscribe(topic){
  const topicBytes = Buffer.from(topic, 'utf8');
  const buf = Buffer.alloc(1 + topicBytes.length);
  buf[0] = 0x01;
  topicBytes.copy(buf,1);
  return buf;
}

function makePublish(topic, payload){
  const t = Buffer.from(topic, 'utf8');
  const p = Buffer.from(payload, 'utf8');
  const buf = Buffer.alloc(1 + 2 + t.length + p.length);
  buf[0] = 0x02;
  buf.writeUInt16BE(t.length,1);
  t.copy(buf,3);
  p.copy(buf,3+t.length);
  return buf;
}

const ws = new WebSocket(url);
let done=false;

const to = setTimeout(()=>{ if(!done){ console.error('timeout'); process.exit(2); } }, 10000);

ws.on('open', ()=>{
  ws.send(makeSubscribe(topic));
  setTimeout(()=> ws.send(makePublish(topic, payload)), 100);
});

ws.on('message', (data)=>{
  const buf = Buffer.from(data);
  // Expecting publish frame: type 0x02 at position 0
  if(buf[0]===0x02){
    // read topic len
    const tlen = buf.readUInt16BE(1);
    const t = buf.slice(3,3+tlen).toString('utf8');
    const p = buf.slice(3+tlen).toString('utf8');
    if(t===topic && p===payload){
      clearTimeout(to);
      console.log('ok');
      done=true;
      process.exit(0);
    }
  }
});

ws.on('error',(e)=>{ console.error('ws error',e); process.exit(3); });
