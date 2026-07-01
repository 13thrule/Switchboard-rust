<script>
  import { connectionStore, modelsStore } from '../stores';

  let showMode = false;

  function toggleMode() {
    showMode = !showMode;
  }
</script>

<div class="h-12 bg-panel border-b border-panel/50 flex items-center justify-between px-6 gap-4">
  <!-- Left: Connection Status -->
  <div class="flex items-center gap-2">
    {#if $connectionStore.connected}
      <div class="w-2 h-2 rounded-full bg-ok animate-pulse" />
      <span class="text-xs text-muted">Connected</span>
    {:else}
      <div class="w-2 h-2 rounded-full bg-warn" />
      <span class="text-xs text-muted">Offline</span>
    {/if}
    <span class="text-xs text-muted">
      {$connectionStore.transport} • {$connectionStore.latency}ms
    </span>
  </div>

  <!-- Center: Model Selector -->
  <div class="flex items-center gap-2">
    {#each $modelsStore as model}
      {#if model.active}
        <div class="flex items-center gap-1 px-2 py-1 rounded bg-accent/10 border border-accent/30">
          <span class="text-xs font-medium">{model.name}</span>
          <span class="text-xs text-muted">{model.tokensPerSec} tok/s</span>
        </div>
      {/if}
    {/each}
  </div>

  <!-- Right: Controls -->
  <div class="flex items-center gap-2 ml-auto">
    <button class="text-xs text-muted hover:text-text transition-colors">⚙️</button>
    <button class="text-xs text-muted hover:text-text transition-colors">❓</button>
  </div>
</div>

<style>
</style>
