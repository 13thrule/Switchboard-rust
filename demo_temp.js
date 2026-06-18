const WebSocket = require('ws');
const url = 'ws://localhost:7777';
const topic = 'ci_demo';
const payload = 'hello-ci';
function makeSubscribe(topic){const t=Buffer.from(topic);const b=Buffer.alloc(1+t.length);b[0]=0x01;t.copy(b,1);return b}
function makePublish(topic,payload){const t=Buffer.from(topic);const p=Buffer.from(payload);const b=Buffer.alloc(1+2+t.length+p.length);b[0]=0x02;b.writeUInt16BE(t.length,1);t.copy(b,3);p.copy(b,3+t.length);return b}
const ws=new WebSocket(url);
ws.on('open',()=>{console.log('open');ws.send(makeSubscribe(topic));setTimeout(()=>{console.log('sending publish');ws.send(makePublish(topic,payload));},500)});
ws.on('message',data=>{const buf=Buffer.from(data);console.log('got',buf.toString('hex'));});
ws.on('error',e=>console.error('ws error',e));
setTimeout(()=>process.exit(0),3000);
