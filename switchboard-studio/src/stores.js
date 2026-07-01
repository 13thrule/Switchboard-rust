import { writable, derived } from 'svelte/store';

// Connection state
export const connectionStore = writable({
  connected: false,
  broker: 'ws://localhost:7777',
  transport: 'websocket', // tcp, ws, shm
  latency: 0
});

// Messages
export const messagesStore = writable([]);

// Models
export const modelsStore = writable([
  {
    id: 'mistral',
    name: 'Mistral 7B',
    params: '7B',
    tokensPerSec: '25-30',
    active: true
  },
  {
    id: 'llama2',
    name: 'Llama 2 7B',
    params: '7B',
    tokensPerSec: '15-20',
    active: false
  }
]);

// Metrics
export const metricsStore = writable({
  messages: 0,
  throughput: 0,
  latency: 0,
  errors: 0,
  backpressure: false
});

// Graph/Pipeline
export const graphStore = writable({
  nodes: [],
  edges: []
});

// WebSocket client
export const switchboardStore = {
  ws: null,
  
  connect(url) {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(url);
        this.ws.binaryType = 'arraybuffer';
        
        this.ws.onopen = () => {
          connectionStore.set({
            connected: true,
            broker: url,
            transport: 'websocket',
            latency: 0
          });
          resolve();
        };
        
        this.ws.onmessage = (evt) => {
          const data = new Uint8Array(evt.data);
          this.handleMessage(data);
        };
        
        this.ws.onerror = (err) => {
          console.error('WebSocket error:', err);
          reject(err);
        };
        
        this.ws.onclose = () => {
          connectionStore.set({
            connected: false,
            broker: url,
            transport: 'websocket',
            latency: 0
          });
        };
      } catch (err) {
        reject(err);
      }
    });
  },
  
  handleMessage(data) {
    // Parse Switchboard protocol
    if (data.length === 0) return;
    
    const messageType = data[0];
    
    if (messageType === 0x02) {
      // Publish message
      const topicLen = (data[1] << 8) | data[2];
      const topic = new TextDecoder().decode(data.slice(3, 3 + topicLen));
      const payload = data.slice(3 + topicLen);
      
      messagesStore.update(msgs => [
        ...msgs,
        {
          id: Math.random().toString(36),
          topic,
          payload: new TextDecoder().decode(payload),
          timestamp: new Date(),
          latency: Math.random() * 10
        }
      ].slice(-100)); // Keep last 100
      
      metricsStore.update(m => ({
        ...m,
        messages: m.messages + 1
      }));
    }
  },
  
  publish(topic, payload) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    
    const topicBytes = new TextEncoder().encode(topic);
    const payloadBytes = new TextEncoder().encode(payload);
    const frame = new Uint8Array(1 + 2 + topicBytes.length + payloadBytes.length);
    
    frame[0] = 0x02; // Publish
    frame[1] = topicBytes.length >> 8;
    frame[2] = topicBytes.length & 0xff;
    frame.set(topicBytes, 3);
    frame.set(payloadBytes, 3 + topicBytes.length);
    
    this.ws.send(frame.buffer);
  },
  
  subscribe(topic) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    
    const topicBytes = new TextEncoder().encode(topic);
    const frame = new Uint8Array(1 + topicBytes.length);
    
    frame[0] = 0x01; // Subscribe
    frame.set(topicBytes, 1);
    
    this.ws.send(frame.buffer);
  }
};
