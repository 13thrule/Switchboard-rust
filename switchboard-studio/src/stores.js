import { writable, derived } from 'svelte/store';

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const DEFAULT_TOPICS = ['prompt.in', 'tokens.out', 'stream.text', 'metrics', 'demo'];

function decodePayload(bytes) {
  try {
    return textDecoder.decode(bytes);
  } catch {
    return Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join(' ');
  }
}

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

export const ollamaStore = writable({
  connected: false,
  baseUrl: 'http://localhost:11434',
  models: [],
  lastCheck: null,
  lastError: null
});

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

export const subscriptionsStore = writable([]);

export function setActiveModel(modelId) {
  modelsStore.update((models) =>
    models.map((model) => ({
      ...model,
      active: model.id === modelId
    }))
  );
}

function normalizeModelName(name) {
  if (!name) return 'unknown';
  return name.split(':')[0];
}

export async function checkOllama() {
  const endpoints = [
    'http://localhost:11434/api/tags',
    '/ollama/api/tags'
  ];

  try {
    let data = null;
    let endpointUsed = null;
    let lastError = null;

    for (const endpoint of endpoints) {
      try {
        const res = await fetch(endpoint, { method: 'GET' });
        if (!res.ok) {
          throw new Error(`tags request failed (${res.status})`);
        }

        data = await res.json();
        endpointUsed = endpoint;
        break;
      } catch (err) {
        lastError = err;
      }
    }

    if (!data) {
      throw lastError ?? new Error('Unable to query Ollama tags');
    }

    const models = Array.isArray(data.models) ? data.models : [];

    ollamaStore.set({
      connected: true,
      baseUrl: endpointUsed?.startsWith('http') ? 'http://localhost:11434' : 'via-dev-proxy',
      models,
      lastCheck: new Date(),
      lastError: null
    });

    if (models.length > 0) {
      modelsStore.update((existing) => {
        const active = existing.find((m) => m.active);
        const mapped = models.slice(0, 8).map((m) => {
          const id = normalizeModelName(m.name);
          const existingMatch = existing.find((e) => e.id === id);
          return {
            id,
            name: normalizeModelName(m.name),
            params: existingMatch?.params ?? 'local',
            tokensPerSec: existingMatch?.tokensPerSec ?? '--',
            active: active ? active.id === id : false
          };
        });

        if (!mapped.some((m) => m.active) && mapped.length > 0) {
          mapped[0].active = true;
        }

        return mapped;
      });
    }

    return true;
  } catch (err) {
    ollamaStore.update((current) => ({
      ...current,
      connected: false,
      lastCheck: new Date(),
      lastError:
        err?.message ??
        'Unable to connect to Ollama. If Ollama runs on your local machine while Studio runs in a remote container, direct connection may fail without CORS or a reachable endpoint.'
    }));
    return false;
  }
}

// WebSocket client
export const switchboardStore = {
  ws: null,
  connectedAtMs: 0,
  recentMessageTimes: [],
  
  connect(url, topics = DEFAULT_TOPICS) {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(url);
        this.ws.binaryType = 'arraybuffer';
        
        this.ws.onopen = () => {
          this.connectedAtMs = Date.now();
          connectionStore.set({
            connected: true,
            broker: url,
            transport: 'websocket',
            latency: 0
          });

          topics.forEach((topic) => this.subscribe(topic));
          subscriptionsStore.set([...topics]);

          resolve();
        };
        
        this.ws.onmessage = (evt) => {
          const data = new Uint8Array(evt.data);
          this.handleMessage(data);
        };
        
        this.ws.onerror = (err) => {
          console.error('WebSocket error:', err);
          metricsStore.update((m) => ({
            ...m,
            errors: m.errors + 1
          }));
          reject(err);
        };
        
        this.ws.onclose = () => {
          connectionStore.set({
            connected: false,
            broker: url,
            transport: 'websocket',
            latency: 0
          });
          subscriptionsStore.set([]);
        };
      } catch (err) {
        metricsStore.update((m) => ({
          ...m,
          errors: m.errors + 1
        }));
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
      const topic = textDecoder.decode(data.slice(3, 3 + topicLen));
      const payload = data.slice(3 + topicLen);

      const now = Date.now();
      this.recentMessageTimes.push(now);
      this.recentMessageTimes = this.recentMessageTimes.filter((t) => now - t <= 5000);
      const throughput = this.recentMessageTimes.length / 5;
      
      messagesStore.update(msgs => [
        ...msgs,
        {
          id: Math.random().toString(36),
          topic,
          payload: decodePayload(payload),
          provenance: 'broker',
          timestamp: new Date(),
          latency: Math.max(1, Math.random() * 10)
        }
      ].slice(-100)); // Keep last 100
      
      metricsStore.update(m => ({
        ...m,
        messages: m.messages + 1,
        throughput,
        latency: Math.max(1, Math.random() * 10)
      }));
    }
  },
  
  publish(topic, payload) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      metricsStore.update((m) => ({
        ...m,
        errors: m.errors + 1
      }));
      return false;
    }

    const now = new Date();
    messagesStore.update((msgs) => [
      ...msgs,
      {
        id: Math.random().toString(36),
        topic,
        payload,
        provenance: 'studio',
        timestamp: now,
        latency: 0
      }
    ].slice(-100));
    
    const topicBytes = textEncoder.encode(topic);
    const payloadBytes = textEncoder.encode(payload);
    const frame = new Uint8Array(1 + 2 + topicBytes.length + payloadBytes.length);
    
    frame[0] = 0x02; // Publish
    frame[1] = topicBytes.length >> 8;
    frame[2] = topicBytes.length & 0xff;
    frame.set(topicBytes, 3);
    frame.set(payloadBytes, 3 + topicBytes.length);
    
    this.ws.send(frame.buffer);
    return true;
  },
  
  subscribe(topic) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return false;
    
    const topicBytes = textEncoder.encode(topic);
    const frame = new Uint8Array(1 + topicBytes.length);
    
    frame[0] = 0x01; // Subscribe
    frame.set(topicBytes, 1);
    
    this.ws.send(frame.buffer);
    return true;
  }
};
