<script>
  import { fly } from 'svelte/transition';
  import { messagesStore, switchboardStore } from '../stores';
</script>

<div class="flex-1 flex flex-col p-6 gap-4 overflow-auto">
  <!-- Messages Timeline -->
  <div class="space-y-3">
    {#each $messagesStore as msg (msg.id)}
      <div
        class="p-4 rounded bg-panel border border-panel/50 hover:border-accent/30 transition-all glass"
        in:fly={{ x: 18, duration: 360, easing: t => 1 - Math.pow(1 - t, 3) }}
      >
        <div class="flex items-center justify-between mb-2">
          <span class="text-xs font-mono text-accent">{msg.topic}</span>
          <span class="text-xs text-muted">{msg.latency.toFixed(1)}ms</span>
        </div>
        <div class="text-sm text-text break-words">
          {msg.payload}
        </div>
        <div class="text-xs text-muted mt-2">
          {msg.timestamp.toLocaleTimeString()}
        </div>
      </div>
    {/each}

    {#if $messagesStore.length === 0}
      <div class="flex items-center justify-center h-64 text-muted">
        <div class="text-center">
          <div class="text-3xl mb-2">💬</div>
          <div>No messages yet. Send a prompt below!</div>
        </div>
      </div>
    {/if}
  </div>
</div>

<style>
</style>
