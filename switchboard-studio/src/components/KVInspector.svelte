<script>
  // KV Cache Inspector Component
  // Shows distributed KV cache state and history
  
  const kvCache = [
    { key: "context_0", size: "2.4 KB", lastUpdate: "2s ago", hitRate: 98.5 },
    { key: "context_1", size: "1.8 KB", lastUpdate: "4s ago", hitRate: 97.2 },
    { key: "embeddings", size: "512 KB", lastUpdate: "12s ago", hitRate: 99.1 }
  ];
</script>

<div class="space-y-3">
  <div class="text-xs text-muted uppercase tracking-wide">KV Cache</div>
  
  <div class="overflow-x-auto">
    <table class="w-full text-xs">
      <thead class="text-muted border-b border-panel/50">
        <tr>
          <th class="text-left py-2">Key</th>
          <th class="text-right">Size</th>
          <th class="text-right">Hit Rate</th>
        </tr>
      </thead>
      <tbody class="space-y-1">
        {#each kvCache as entry}
          <tr class="hover:bg-panel/50 transition-colors">
            <td class="py-2 font-mono text-accent">{entry.key}</td>
            <td class="text-right text-muted">{entry.size}</td>
            <td class="text-right text-ok">{entry.hitRate}%</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>

  <div class="flex gap-2">
    <button class="flex-1 px-2 py-1 rounded bg-accent/10 text-accent text-xs hover:bg-accent/20 transition-colors">
      Snapshot
    </button>
    <button class="flex-1 px-2 py-1 rounded bg-panel text-muted text-xs hover:bg-panel/80 transition-colors">
      Replay
    </button>
  </div>
</div>

<style>
</style>
