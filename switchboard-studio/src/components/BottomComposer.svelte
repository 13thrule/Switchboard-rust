<script>
  import { switchboardStore } from '../stores';

  let prompt = '';
  let expanded = false;
  let topic = 'prompt.in';

  function handleSend() {
    if (prompt.trim()) {
      switchboardStore.publish(topic, prompt);
      prompt = '';
    }
  }

  function handleKeydown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  }
</script>

<div class="bg-panel border-t border-panel/50 p-4">
  {#if expanded}
    <div class="space-y-3 mb-4">
      <div class="flex gap-2">
        <input
          type="text"
          placeholder="Topic..."
          bind:value={topic}
          class="flex-1 bg-bg border border-panel rounded px-3 py-2 text-sm text-text placeholder-muted focus:border-accent outline-none transition-colors"
        />
        <button class="px-3 py-2 bg-accent/10 text-accent rounded text-sm hover:bg-accent/20 transition-colors">
          Templates
        </button>
      </div>

      <div class="flex gap-2 text-xs">
        <label class="flex items-center gap-1">
          <input type="checkbox" class="rounded" />
          <span>Explain response</span>
        </label>
        <label class="flex items-center gap-1">
          <input type="checkbox" class="rounded" />
          <span>Sandbox mode</span>
        </label>
      </div>
    </div>
  {/if}

  <div class="flex gap-2">
    <textarea
      placeholder="Send a prompt..."
      bind:value={prompt}
      on:keydown={handleKeydown}
      class="flex-1 bg-bg border border-panel rounded px-3 py-2 text-sm text-text placeholder-muted focus:border-accent outline-none transition-colors resize-none"
      rows={expanded ? 3 : 1}
    />
    <button
      class="px-4 py-2 bg-accent text-bg rounded font-medium hover:bg-accent/90 transition-colors"
      on:click={handleSend}
    >
      Send
    </button>
    <button
      class="px-3 py-2 text-muted hover:text-text transition-colors"
      on:click={() => (expanded = !expanded)}
    >
      {expanded ? '▼' : '▶'}
    </button>
  </div>
</div>

<style>
</style>
