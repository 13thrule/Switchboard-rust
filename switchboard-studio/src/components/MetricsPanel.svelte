<script>
  import { metricsStore } from '../stores';
</script>

<div class="w-72 bg-panel border-l border-panel/50 flex flex-col overflow-hidden">
  <!-- Header -->
  <div class="h-12 border-b border-panel/50 flex items-center px-4">
    <span class="text-sm font-medium">📊 Metrics</span>
  </div>

  <!-- Scrollable Metrics -->
  <div class="flex-1 overflow-y-auto p-4 space-y-4">
    <!-- Messages -->
    <div class="p-3 rounded bg-bg border border-panel/50">
      <div class="text-xs text-muted uppercase tracking-wide mb-1">Messages</div>
      <div class="text-2xl font-bold text-accent">{$metricsStore.messages}</div>
      <div class="text-xs text-muted mt-1">total published</div>
    </div>

    <!-- Throughput -->
    <div class="p-3 rounded bg-bg border border-panel/50">
      <div class="text-xs text-muted uppercase tracking-wide mb-1">Throughput</div>
      <div class="text-2xl font-bold text-accent-2">{$metricsStore.throughput.toFixed(1)}</div>
      <div class="text-xs text-muted mt-1">msg/sec</div>
    </div>

    <!-- Latency -->
    <div class="p-3 rounded bg-bg border border-panel/50">
      <div class="text-xs text-muted uppercase tracking-wide mb-1">Latency</div>
      <div class="text-2xl font-bold text-text">{$metricsStore.latency.toFixed(1)}</div>
      <div class="text-xs text-muted mt-1">milliseconds</div>
    </div>

    <!-- Errors -->
    <div class="p-3 rounded bg-bg border border-panel/50">
      <div class="text-xs text-muted uppercase tracking-wide mb-1">Errors</div>
      <div class="text-2xl font-bold text-warn">{$metricsStore.errors}</div>
      <div class="text-xs text-muted mt-1">last 24h</div>
    </div>

    <!-- Backpressure Alert -->
    {#if $metricsStore.backpressure}
      <div class="p-3 rounded bg-warn/10 border border-warn/30">
        <div class="text-xs font-medium text-warn mb-2">⚠️ Backpressure Detected</div>
        <div class="text-xs text-text mb-2">Ring buffer at 85% capacity. Consider throttling publishers.</div>
        <button class="w-full px-2 py-1 bg-warn text-bg rounded text-xs font-medium hover:bg-warn/90 transition-colors">
          Throttle Now
        </button>
      </div>
    {/if}
  </div>

  <!-- Footer -->
  <div class="h-12 border-t border-panel/50 flex items-center px-4 text-xs text-muted">
    🔄 Real-time
  </div>
</div>

<style>
</style>
