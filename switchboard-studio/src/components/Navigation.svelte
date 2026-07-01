<script>
  import { modelsStore } from '../stores';

  export let mode = 'engineer';

  const modes = [
    { id: 'focus', label: 'Focus', icon: '🎯' },
    { id: 'engineer', label: 'Engineer', icon: '⚙️' },
    { id: 'presentation', label: 'Present', icon: '🎪' }
  ];
</script>

<nav class="w-64 bg-panel border-r border-panel/50 flex flex-col p-4 gap-6">
  <!-- Logo/Title -->
  <div class="text-lg font-bold text-accent">
    🔌 Switchboard Studio
  </div>

  <!-- Mode Selector -->
  <div class="space-y-2">
    <div class="text-xs text-muted uppercase tracking-wide">Mode</div>
    {#each modes as m}
      <button
        class="w-full px-3 py-2 rounded transition-colors text-sm"
        class:bg-accent={mode === m.id}
        class:text-text={mode === m.id}
        class:bg-panel={mode !== m.id}
        class:text-muted={mode !== m.id}
        on:click={() => (mode = m.id)}
      >
        {m.icon} {m.label}
      </button>
    {/each}
  </div>

  <hr class="border-panel/50" />

  <!-- Models -->
  <div class="space-y-2 flex-1 overflow-y-auto">
    <div class="text-xs text-muted uppercase tracking-wide">Models</div>
    {#each $modelsStore as model}
      <button
        class="w-full px-3 py-2 rounded transition-all text-sm text-left border"
        class:bg-panel={!model.active}
        class:text-muted={!model.active}
        class:border-panel={!model.active}
        class:bg-accent={model.active}
        class:bg-opacity-20={model.active}
        class:border-accent={model.active}
      >
        <div class="font-medium text-text">{model.name}</div>
        <div class="text-xs text-muted">{model.params} • {model.tokensPerSec}</div>
      </button>
    {/each}
  </div>

  <!-- Footer -->
  <div class="text-xs text-muted text-center py-2 border-t border-panel/50">
    v0.1.0
  </div>
</nav>

<style>
</style>
