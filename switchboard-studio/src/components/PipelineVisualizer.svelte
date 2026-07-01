<script>
  import { graphStore } from '../stores';

  let selectedNode = null;

  function onNodeClick(node) {
    selectedNode = node;
  }

  function onNodeKeyDown(event, node) {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      onNodeClick(node);
    }
  }

  // Sample graph for demo
  const sampleGraph = {
    nodes: [
      { id: 'prompt', label: 'prompt.in', type: 'source', x: 100, y: 150 },
      { id: 'model', label: 'Ollama', type: 'transform', x: 300, y: 150 },
      { id: 'tokens', label: 'tokens.out', type: 'sink', x: 500, y: 150 },
      { id: 'text', label: 'stream.text', type: 'sink', x: 500, y: 300 }
    ],
    edges: [
      { from: 'prompt', to: 'model' },
      { from: 'model', to: 'tokens' },
      { from: 'model', to: 'text' }
    ]
  };
</script>

<div class="flex-1 p-6 overflow-auto">
  <svg class="w-full h-full min-h-96" viewBox="0 0 800 500">
    <!-- Edges -->
    {#each sampleGraph.edges as edge}
      <line
        x1={sampleGraph.nodes.find(n => n.id === edge.from)?.x}
        y1={sampleGraph.nodes.find(n => n.id === edge.from)?.y}
        x2={sampleGraph.nodes.find(n => n.id === edge.to)?.x}
        y2={sampleGraph.nodes.find(n => n.id === edge.to)?.y}
        stroke="rgba(76,139,245,0.3)"
        stroke-width="2"
        class="transition-all hover:stroke-accent"
      />
    {/each}

    <!-- Nodes -->
    {#each sampleGraph.nodes as node (node.id)}
      <g
        role="button"
        tabindex="0"
        aria-label={`Inspect node ${node.label}`}
        on:click={() => onNodeClick(node)}
        on:keydown={(event) => onNodeKeyDown(event, node)}
        class="cursor-pointer"
      >
        <rect
          x={node.x - 60}
          y={node.y - 30}
          width="120"
          height="60"
          rx="8"
          fill="rgba(17,19,26,0.8)"
          stroke={selectedNode?.id === node.id ? '#4C8BF5' : 'rgba(76,139,245,0.3)'}
          stroke-width={selectedNode?.id === node.id ? '2' : '1'}
          class="transition-all hover:stroke-accent"
        />
        <text
          x={node.x}
          y={node.y + 5}
          text-anchor="middle"
          fill="#E6E6E6"
          font-size="12"
          font-weight="500"
          class="pointer-events-none"
        >
          {node.label}
        </text>
      </g>
    {/each}
  </svg>

  <!-- Legend -->
  <div class="mt-6 flex gap-4 text-xs">
    <div class="flex items-center gap-2">
      <div class="w-3 h-3 rounded bg-ok" />
      <span class="text-muted">Source</span>
    </div>
    <div class="flex items-center gap-2">
      <div class="w-3 h-3 rounded bg-accent" />
      <span class="text-muted">Transform</span>
    </div>
    <div class="flex items-center gap-2">
      <div class="w-3 h-3 rounded bg-warn" />
      <span class="text-muted">Sink</span>
    </div>
  </div>
</div>

<style>
</style>
