<script>
  import { onMount } from 'svelte';
  import { checkOllama, switchboardStore } from './stores';
  import Navigation from './components/Navigation.svelte';
  import StatusBar from './components/StatusBar.svelte';
  import BottomComposer from './components/BottomComposer.svelte';
  import ChatCanvas from './components/ChatCanvas.svelte';
  import PipelineVisualizer from './components/PipelineVisualizer.svelte';
  import MetricsPanel from './components/MetricsPanel.svelte';
  import './app.css';

  let mode = 'engineer'; // 'focus', 'engineer', 'presentation'
  let activeTab = 'chat'; // 'chat', 'pipeline'

  onMount(() => {
    // Initialize connection and subscribe to core LLM/demo topics.
    switchboardStore
      .connect('ws://localhost:7777', ['prompt.in', 'tokens.out', 'stream.text', 'metrics', 'demo'])
      .catch((err) => {
        console.error('Failed to connect to Switchboard broker:', err);
      });

    checkOllama();
    const timer = setInterval(() => {
      checkOllama();
    }, 5000);

    return () => clearInterval(timer);
  });
</script>

<div class="min-h-screen bg-bg text-text flex flex-col">
  <!-- Status Bar -->
  <StatusBar {mode} />

  <!-- Main Layout -->
  <div class="flex flex-1 overflow-hidden">
    <!-- Left Sidebar -->
    {#if mode !== 'focus'}
      <Navigation bind:mode bind:activeTab />
    {/if}

    <!-- Center Canvas -->
    <div class="flex-1 flex flex-col min-w-0">
      <!-- Tab Navigation -->
      <div class="flex border-b border-panel bg-panel/50">
        <button
          class="px-6 py-3 text-sm font-medium transition-colors"
          class:text-accent={activeTab === 'chat'}
          class:text-muted={activeTab !== 'chat'}
          on:click={() => (activeTab = 'chat')}
        >
          💬 Chat
        </button>
        <button
          class="px-6 py-3 text-sm font-medium transition-colors"
          class:text-accent={activeTab === 'pipeline'}
          class:text-muted={activeTab !== 'pipeline'}
          on:click={() => (activeTab = 'pipeline')}
        >
          🔗 Pipeline
        </button>
      </div>

      <!-- Content Area -->
      <div class="flex-1 overflow-auto">
        {#if activeTab === 'chat'}
          <ChatCanvas />
        {:else}
          <PipelineVisualizer />
        {/if}
      </div>
    </div>

    <!-- Right Sidebar -->
    {#if mode === 'engineer'}
      <MetricsPanel />
    {/if}
  </div>

  <!-- Bottom Composer -->
  <BottomComposer />
</div>

<style>
  :global(body) {
    margin: 0;
    padding: 0;
  }
</style>
