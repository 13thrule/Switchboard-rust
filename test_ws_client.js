const ws = new WebSocket('ws://localhost:7777');
ws.binaryType = 'arraybuffer';

ws.addEventListener('open', () => {
  console.log('open');
  const topic = 'trades';
  const payload = new TextEncoder().encode('AAPL BUY 150 shares @ 195.50');
  const topicBytes = new TextEncoder().encode(topic);
  const frame = new Uint8Array(1 + 2 + topicBytes.length + payload.length);
  frame[0] = 0x02;
  frame[1] = topicBytes.length >> 8;
  frame[2] = topicBytes.length & 0xff;
  frame.set(topicBytes, 3);
  frame.set(payload, 3 + topicBytes.length);
  ws.send(frame);
});

ws.addEventListener('message', (event) => {
  const data = new Uint8Array(event.data);
  console.log('message', data);
  const kind = data[0];
  if (kind === 0x02) {
    const topicLen = (data[1] << 8) | data[2];
    const topic = new TextDecoder().decode(data.subarray(3, 3 + topicLen));
    const payload = new TextDecoder().decode(data.subarray(3 + topicLen));
    console.log('topic', topic);
    console.log('payload', payload);
  }
});

ws.addEventListener('error', (event) => console.error('error', event));
ws.addEventListener('close', () => console.log('closed'));