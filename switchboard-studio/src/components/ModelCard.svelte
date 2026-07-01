<script>
  // Model Card Component
  // Displays model info with status and controls
  
  export let model = {
    id: 'mistral',
    name: 'Mistral 7B',
    params: '7B',
    tokensPerSec: '25-30',
    active: true,
    status: 'running',
    inference: true
  };
</script>

<div class="p-4 rounded bg-panel border border-panel/50 hover:border-accent/30 transition-all glass">
  <div class="flex items-start justify-between mb-3">
    <div>
      <div class="text-lg font-bold text-text">{model.name}</div>
      <div class="text-xs text-muted">{model.params} parameters</div>
    </div>
    {#if model.active}
      <div class="w-3 h-3 rounded-full bg-ok animate-pulse" />
    {/if}
  </div>

  <div class="flex gap-4 mb-3 text-xs">
    <div>
      <div class="text-muted">Throughput</div>
      <div class="font-mono text-accent">{model.tokensPerSec}</div>
    </div>
    <div>
      <div class="text-muted">Status</div>
      <div class="font-mono text-text capitalize">{model.status}</div>
    </div>
  </div>

  <button class="w-full px-3 py-2 rounded bg-accent text-bg font-medium text-sm hover:bg-accent/90 transition-colors">
    {model.active ? 'Active' : 'Activate'}
  </button>
</div>

<style>
</style>
