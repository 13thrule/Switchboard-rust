<script>
  import { connectionStore, modelsStore, ollamaStore, runOllamaInference, switchboardStore } from '../stores';

  let prompt = '';
  let expanded = false;
  let topic = 'prompt.in';
  let status = '';
  let showTemplates = false;
  let explain = false;
  let sandbox = true;
  let useOllama = true;

  const templates = [
    { label: 'Chat', topic: 'prompt.in', prompt: 'Help me reason through this step by step.' },
    { label: 'Summarize', topic: 'prompt.in', prompt: 'Summarize the following text:\n\n' },
    { label: 'Code Review', topic: 'prompt.in', prompt: 'Review this code for correctness, performance, and security:\n\n' },
    { label: 'Agent Chain', topic: 'prompt.in', prompt: 'Break this task into an agent execution plan with checkpoints.' }
  ];

  $: activeModel = $modelsStore.find((m) => m.active)?.name ?? 'unknown';

  function applyTemplate(template) {
    topic = template.topic;
    prompt = template.prompt;
    showTemplates = false;
    status = `template loaded: ${template.label}`;
  }

  function handleSend() {
    if (prompt.trim()) {
      const effectiveTopic = sandbox ? `sandbox.${topic}` : topic;
      const basePrompt = explain
        ? `[model=${activeModel}] ${prompt}\n\nExplain your reasoning clearly.`
        : `[model=${activeModel}] ${prompt}`;
      const ok = switchboardStore.publish(effectiveTopic, basePrompt);
      if (ok) {
        status = `sent to ${effectiveTopic}`;

        if (useOllama && $ollamaStore.connected && topic === 'prompt.in') {
          status = `sent to ${effectiveTopic}, running ${activeModel}...`;
          runOllamaInference(basePrompt, activeModel).then((result) => {
            if (result.ok) {
              switchboardStore.publish(sandbox ? 'sandbox.stream.text' : 'stream.text', result.response);
              status = `response received from ${activeModel}`;
            } else {
              status = `ollama inference failed: ${result.error}`;
            }
          });
        }

        prompt = '';
      } else {
        status = 'not connected to broker';
      }
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
  {#if status}
    <div class="mb-2 text-xs text-muted">{status}</div>
  {/if}

  {#if expanded}
    <div class="space-y-3 mb-4">
      <div class="flex gap-2">
        <input
          type="text"
          placeholder="Topic..."
          bind:value={topic}
          class="flex-1 bg-bg border border-panel rounded px-3 py-2 text-sm text-text placeholder-muted focus:border-accent outline-none transition-colors"
        />
        <button
          type="button"
          class="px-3 py-2 bg-accent/10 text-accent rounded text-sm hover:bg-accent/20 transition-colors"
          on:click={() => (showTemplates = !showTemplates)}
        >
          Templates
        </button>
      </div>

      {#if showTemplates}
        <div class="grid grid-cols-2 gap-2">
          {#each templates as template}
            <button
              class="px-2 py-2 text-xs bg-bg border border-panel rounded text-left hover:border-accent transition-colors"
              on:click={() => applyTemplate(template)}
            >
              {template.label}
            </button>
          {/each}
        </div>
      {/if}

      <div class="flex gap-2 text-xs">
        <label class="flex items-center gap-1">
          <input type="checkbox" class="rounded" bind:checked={explain} />
          <span>Explain response</span>
        </label>
        <label class="flex items-center gap-1">
          <input type="checkbox" class="rounded" bind:checked={sandbox} />
          <span>Sandbox mode</span>
        </label>
        <label class="flex items-center gap-1">
          <input type="checkbox" class="rounded" bind:checked={useOllama} />
          <span>Use Ollama</span>
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
      disabled={!$connectionStore.connected}
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
