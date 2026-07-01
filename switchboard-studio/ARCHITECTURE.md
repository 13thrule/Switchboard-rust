# Switchboard Studio Architecture

## Project Overview

Switchboard Studio is a premium web interface for visualizing, debugging, and monitoring high-performance message pipelines. It connects to Switchboard brokers and displays real-time data flow, metrics, and diagnostic information with a focus on low-latency perception and intuitive interaction design.

## Directory Structure

```
switchboard-studio/
├── src/
│   ├── components/
│   │   ├── StatusBar.svelte           # Top bar: connection, model, controls
│   │   ├── Navigation.svelte          # Left sidebar: mode, models
│   │   ├── ChatCanvas.svelte          # Center: message timeline
│   │   ├── PipelineVisualizer.svelte  # Center: interactive graph
│   │   ├── BottomComposer.svelte      # Bottom: prompt input
│   │   ├── MetricsPanel.svelte        # Right: real-time stats
│   │   ├── ModelCard.svelte           # Model info card
│   │   ├── KVInspector.svelte         # KV cache state viewer
│   │   └── DebugViewer.svelte         # (planned) Binary frame inspector
│   ├── services/
│   │   ├── switchboardClient.ts       # Binary protocol WebSocket client
│   │   └── metrics.ts                 # Prometheus scraper
│   ├── App.svelte                     # Main layout & routing
│   ├── stores.js                      # Svelte reactive stores
│   ├── main.js                        # Entry point
│   └── app.css                        # Global styles + design tokens
├── design/
│   └── DESIGN_SYSTEM.md               # Complete design spec
├── public/
│   └── (static assets)
├── package.json
├── vite.config.js
├── tailwind.config.js
├── index.html
├── README.md
└── .gitignore
```

## Key Components

### StatusBar
- **Purpose**: Connection status, model selector, quick controls
- **Size**: Fixed 48px height
- **Contents**:
  - Left: Connection badge (transport type, latency)
  - Center: Active model with throughput spec
  - Right: Settings & help icons

### Navigation (Left Sidebar)
- **Width**: 256px (collapsible on mobile)
- **Contents**:
  - Mode selector (Focus / Engineer / Presentation)
  - Model list with quick-activate buttons
  - Footer: version

### ChatCanvas (Center, Tab 1)
- **Purpose**: Conversation view with streaming tokens
- **Features**:
  - Message timeline with scroll history
  - Animated token arrival
  - Latency badges
  - Topic source (adapter name)
  - Regenerate & Explain actions

### PipelineVisualizer (Center, Tab 2)
- **Purpose**: Interactive data flow graph
- **Features**:
  - SVG node graph with animated edges
  - Clickable nodes show message history
  - Edge thickness = throughput
  - Node pulse = active processing
  - Mini legend
  - Replay controls

### BottomComposer
- **Purpose**: Message input with advanced controls
- **Features**:
  - Compact single-row mode (expandable)
  - Topic selector
  - Advanced options:
    - Explain response (checkbox)
    - Sandbox mode (checkbox)
    - Presets (dropdown)
  - Keyboard shortcut: Cmd+Enter to send

### MetricsPanel (Right Sidebar)
- **Width**: 288px
- **Contents**:
  - Message counter
  - Throughput gauge (msg/sec)
  - Latency histogram
  - Error counter
  - Backpressure alert with throttle button
  - Connection footer

### ModelCard
- **Purpose**: Model info display
- **Shows**: Name, params, throughput, status
- **Interactive**: Activate button

### KVInspector
- **Purpose**: KV cache state table
- **Shows**: Key, size, hit rate
- **Actions**: Snapshot, replay

## State Management (Svelte Stores)

### connectionStore
```javascript
{
  connected: boolean,
  broker: string,
  transport: 'tcp' | 'ws' | 'shm',
  latency: number
}
```

### messagesStore
```javascript
[
  {
    id: string,
    topic: string,
    payload: string,
    timestamp: Date,
    latency: number
  }
]
// Keeps last 100 messages
```

### modelsStore
```javascript
[
  {
    id: string,
    name: string,
    params: string,
    tokensPerSec: string,
    active: boolean
  }
]
```

### metricsStore
```javascript
{
  messages: number,
  throughput: number,
  latency: number,
  errors: number,
  backpressure: boolean
}
```

### graphStore
```javascript
{
  nodes: [
    {
      id: string,
      label: string,
      type: 'source' | 'transform' | 'sink',
      x: number,
      y: number,
      throughput?: number
    }
  ],
  edges: [
    {
      from: string,
      to: string
    }
  ]
}
```

## WebSocket Client Implementation

The `switchboardStore` object provides native Switchboard binary protocol support:

```javascript
// Connect
switchboardStore.connect('ws://localhost:7777')

// Publish message
switchboardStore.publish('topic.name', 'payload text')

// Subscribe to topic
switchboardStore.subscribe('topic.name')
```

### Binary Protocol (from Switchboard)
- **Subscribe (0x01)**: [type:1 byte] [topic:UTF-8 string]
- **Publish (0x02)**: [type:1 byte] [topic_len:2 bytes] [topic:bytes] [payload:bytes]

## Design System Integration

### CSS Variables (in app.css)
```css
:root {
  --bg: #0B0E13;
  --panel: #11131A;
  --accent: #4C8BF5;
  --accent-2: #7EE787;
  --text: #E6E6E6;
  --muted: #9AA3B2;
  --warn: #F59E0B;
  --ok: #22C55E;
}
```

### Tailwind Extensions (tailwind.config.js)
- Custom colors mapped to CSS variables
- Animation keyframes (tokenIn, nodePulse)
- Default border radius: 12px
- Font families: Inter (UI), JetBrains Mono (code)

## Interaction Flows

### First-Run Onboarding
1. **Auto-connect** to default broker (localhost:7777)
2. **Show animation**: "Connecting..."
3. **Load sample data**: Subscribe to demo topic
4. **Auto-send** test prompt → watch tokens stream
5. **Highlight UI**: Show each panel with callouts
6. **Offer templates**: "Try Chat", "Try Code Review"

### Send Prompt Flow
1. User types in BottomComposer
2. Click Send or Cmd+Enter
3. Publish to `prompt.in` topic
4. Message appears in ChatCanvas
5. Model processes (if subscribed to responses)
6. Tokens stream into ChatCanvas with animation
7. Metrics update (latency, throughput, message count)

### Explain Mode Flow
1. User toggles "Explain response" in BottomComposer
2. After model response is complete
3. Studio auto-sends: "Explain your reasoning for the above response"
4. Explanation appears in collapsible pane
5. Provenance shown: "Generated by model X"

### Replay Flow
1. Click message in ChatCanvas timeline
2. "Time travel" scrubber appears
3. Drag to select replay window
4. Click "Replay" button
5. Messages re-flow through pipeline in graph view
6. Metrics update in real-time

## Plugin System

### Plugin Interface
```javascript
export default {
  name: 'plugin-name',
  version: '0.1.0',
  
  // Register node types for pipeline
  nodes: [
    {
      type: 'custom-node',
      label: 'My Node',
      icon: '⚡',
      color: '#4C8BF5'
    }
  ],
  
  // Add custom metrics
  metrics: [
    {
      name: 'metric_name',
      type: 'gauge|counter|histogram',
      label: 'Display Name',
      unit: 'msg/s|ms|%'
    }
  ],
  
  // Add UI panels
  panels: [
    {
      id: 'panel-id',
      label: 'Panel Label',
      component: SvelteComponent,
      icon: '📊'
    }
  ],
  
  // Custom adapters
  adapters: [
    {
      name: 'ollama',
      connect: async (url) => { /* ... */ },
      disconnect: async () => { /* ... */ }
    }
  ]
}
```

## Performance Targets

- **Bundle**: <200KB gzipped
- **First Paint**: <1.5s on 3G
- **Interaction**: <100ms response time
- **Memory**: <50MB browser + 15MB Tauri

## Development Workflow

### Setup
```bash
npm install
npm run dev
```

### Development Server
Runs on http://localhost:5173 with HMR.

### Building
```bash
npm run build    # Vite SPA build
npm run preview  # Test production build
```

### Desktop (Tauri)
```bash
npm run tauri:dev    # Debug mode
npm run tauri:build  # Production binary
```

## Future Enhancements

### Short Term (v0.2)
- Real Ollama HTTP integration
- Message replay with timeline scrubber
- Custom node template builder
- YAML graph export

### Medium Term (v0.3)
- Multi-broker federation
- Distributed tracing (OpenTelemetry)
- Schema registry integration
- Mobile-responsive design

### Long Term (v1.0)
- Desktop Tauri app with native notifications
- Plugin marketplace
- Enterprise auth & RBAC
- Persistent message storage (IndexedDB)
- Collaborative features (multi-user view)

## Testing Strategy

### Unit Tests
- Component snapshots
- Store mutations
- Binary protocol parsing

### Integration Tests
- WebSocket connection flow
- Message publish/subscribe
- Graph visualization rendering

### E2E Tests (Playwright)
- Full user workflows
- Cross-browser compatibility
- Performance benchmarks

## Deployment

### Static Hosting (Vercel/Netlify)
```bash
npm run build
# Deploy dist/ folder
```

### Docker
```dockerfile
FROM node:18-alpine as build
WORKDIR /app
COPY . .
RUN npm install && npm run build

FROM node:18-alpine
WORKDIR /app
COPY --from=build /app/dist ./dist
EXPOSE 3000
CMD ["npm", "run", "preview"]
```

### Tauri Desktop
Outputs native binary for macOS, Linux, Windows.

---

**Last Updated**: July 1, 2026  
**Status**: MVP Foundation Complete  
**Next Phase**: Real Ollama Integration + Desktop App
