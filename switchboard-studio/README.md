# Switchboard Studio

Switchboard Studio is the visual operations console for the Switchboard ecosystem.

It provides a dark, high-contrast interface for:
- live chat streams
- pipeline visualization
- real-time broker metrics
- interactive debugging workflows

## Current Status

- Build: passing (`npm run build`)
- Stack: Svelte 4 + Vite 5 + Tailwind CSS
- Runtime protocol: WebSocket (Switchboard binary framing)
- Modes: Focus, Engineer, Presentation

## What Is Implemented

- Top status bar with connection state and active model display
- Left navigation with mode switching and model cards
- Center canvas with chat/pipeline tabs
- Animated chat timeline cards with topic + latency badges
- Interactive pipeline graph selection
- Right metrics panel with backpressure alert surface
- Bottom composer for quick prompt publish
- Keyboard-accessible node selection in the pipeline canvas

## Quick Start

1. Start Switchboard broker:

```bash
cd ../switchboard_refactored/switchboard
cargo run --release -- --port 7777
```

2. In another terminal, run Studio:

```bash
cd /workspaces/Switchboard-rust/switchboard-studio
npm install
npm run dev
```

3. Open:

- http://localhost:5173

Studio attempts to connect to `ws://localhost:7777` on load.

## Build for Production

```bash
npm run build
npm run preview
```

## Project Layout

```text
switchboard-studio/
  src/
    App.svelte
    app.css
    stores.js
    components/
      StatusBar.svelte
      Navigation.svelte
      ChatCanvas.svelte
      PipelineVisualizer.svelte
      MetricsPanel.svelte
      BottomComposer.svelte
      KVInspector.svelte
      ModelCard.svelte
  index.html
  package.json
  vite.config.js
  tailwind.config.js
```

## Design Tokens

Studio uses the shared visual language:

- `--bg: #0B0E13`
- `--panel: #11131A`
- `--accent: #4C8BF5`
- `--accent-2: #7EE787`
- `--text: #E6E6E6`
- `--muted: #9AA3B2`
- `--warn: #F59E0B`
- `--ok: #22C55E`

Defined in `src/app.css`.

## Notes

- Tauri scripts exist in `package.json` but desktop packaging is not wired yet (no `src-tauri` in this folder at the moment).
- Some advanced panels described in design docs are planned but not fully wired to live broker topics yet.

## Related Docs

- `DESIGN_SYSTEM.md`
- `ARCHITECTURE.md`
- `../README.md`
