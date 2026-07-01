# Switchboard Studio Design System

## Color System

### Palette
```
Background:  #0B0E13  (Deep black with blue tint)
Panel:       #11131A  (Slightly lighter for depth)
Accent:      #4C8BF5  (Primary interactive - bright blue)
Accent 2:    #7EE787  (Secondary - vibrant green)
Text:        #E6E6E6  (Light gray - high contrast)
Muted:       #9AA3B2  (Medium gray - secondary text)
Warning:     #F59E0B  (Amber - alerts)
Success:     #22C55E  (Green - confirmations)
```

### Usage Patterns
- **Primary actions**: Accent (#4C8BF5)
- **Success states**: Accent 2 (#7EE787)
- **Error/warnings**: Warning (#F59E0B)
- **Text hierarchy**: Text → Muted for secondary
- **Backgrounds**: Panel with glass morphism overlay

## Typography

### Font Families
- **UI**: Inter (400, 500, 600, 700)
- **Monospace/Code**: JetBrains Mono (400, 500)

### Scale
```
10px  - Captions, badges
12px  - Helper text, timestamps
14px  - Body text, labels
16px  - Subheadings
18px  - Large titles
20px+ - Major headings
```

### Weight Usage
- 400: Body text
- 500: Subheadings, UI labels
- 600: Emphasized text, button labels
- 700: Major headings

## Spacing

### Base Unit: 4px
```
4px   - xs (tight spacing)
8px   - sm (compact)
12px  - md (comfortable)
16px  - lg (relaxed)
24px  - xl (generous)
32px  - 2xl (very loose)
```

## Sizing

### Common Dimensions
```
Status Bar:      height: 48px
Sidebar:         width: 256px
Right Panel:     width: 288px
Border Radius:   12px (default)
Input Height:    40px (form elements)
Button Height:   36px
Card Padding:    16px (default), 24px (generous)
```

## Motion

### Easing Curves
- **Standard**: cubic-bezier(.2, .9, .3, 1)
- **Smooth**: cubic-bezier(.4, 0, .6, 1)
- **Bounce**: cubic-bezier(.68, -0.55, .265, 1.55)

### Durations
```
Micro:        120ms (state changes)
Short:        180ms (transitions)
Standard:     360ms (animations)
Long:         600ms (page transitions)
```

### Animations

#### Token Stream (Message Arrival)
```css
@keyframes tokenIn {
  0% {
    transform: translateX(12px) scale(.98);
    opacity: 0;
    filter: blur(2px);
  }
  60% {
    transform: translateX(-4px) scale(1.02);
    opacity: 1;
    filter: blur(0);
  }
  100% {
    transform: translateX(0) scale(1);
    opacity: 1;
  }
}
```
Duration: 360ms, Easing: standard

#### Node Pulse (Active Processing)
```css
@keyframes nodePulse {
  0%, 100% {
    box-shadow: 0 8px 24px rgba(76, 139, 245, 0.12);
  }
  50% {
    box-shadow: 0 12px 32px rgba(76, 139, 245, 0.24);
  }
}
```
Duration: 2s, Easing: smooth, Infinite

#### Message Flow (Edge Animation)
```css
@keyframes flowPulse {
  0% { stroke-dashoffset: 20; }
  100% { stroke-dashoffset: 0; }
}
```
Duration: 1.5s, Easing: linear, Infinite

## Shadows

### Elevation System
```
0   - No shadow (bg elements)
1   - 0 2px 4px rgba(0,0,0,0.2)
2   - 0 4px 8px rgba(0,0,0,0.25)
3   - 0 8px 16px rgba(0,0,0,0.3)
4   - 0 12px 24px rgba(0,0,0,0.35)
5   - 0 16px 32px rgba(0,0,0,0.4)
```

Glow effects use accent colors at 0.12-0.24 opacity.

## Component Specifications

### Buttons
```
Height:     36px
Padding:    12px 24px
Border Radius: 8px
Font Weight: 600
Font Size:  14px
Transition: 180ms

States:
  Default:  accent bg, text color, no shadow
  Hover:    accent bg with 90% opacity
  Active:   accent with darker shade
  Disabled: muted bg, muted text
```

### Inputs & Textareas
```
Height:     40px (input), 120px (textarea)
Padding:    12px 16px
Border:     1px solid panel
Border Radius: 8px
Focus:      1px solid accent
Transition: 180ms

Placeholder: muted color
Value:      text color
Background: bg (slightly lighter than main)
```

### Cards & Panels
```
Padding:      16px (compact), 24px (generous)
Border:       1px solid rgba(255,255,255,0.05)
Border Radius: 12px
Background:   panel with glass morphism
Hover Shadow: 6px rgba(76,139,245,0.08)
Transition:   180ms

Glass effect:
  background: rgba(255,255,255,0.03)
  backdrop-filter: blur(10px)
  border: 1px solid rgba(255,255,255,0.05)
```

### Badges & Tags
```
Padding:      4px 12px
Border Radius: 6px
Font Size:    12px
Font Weight:  500
Height:       24px

Colors:
  Accent:     accent bg, accent/30 border
  Success:    ok bg, ok/30 border
  Warning:    warn bg, warn/30 border
```

## Responsive Breakpoints

```
Mobile:   <640px    (single column)
Tablet:   640-1024px (two columns)
Desktop:  >1024px   (three columns)
```

### Behavior
- **Mobile**: Compose dock overlays bottom
- **Tablet**: Right panel becomes floating
- **Desktop**: Full three-column layout

## Accessibility

### WCAG AA Compliance
- Text: 4.5:1 contrast ratio (minimum)
- Large text: 3:1 ratio acceptable
- Focus indicators: 2px accent outline
- Keyboard navigation: Tab order logical

### Screen Reader
- Announce dynamic updates concisely
- Provide semantic HTML structure
- Use `aria-live` regions for streaming text

### Focus States
- Visible outline or highlight
- Keyboard shortcuts clearly labeled
- Tab order follows visual flow

## Implementation

### CSS Variables
```css
:root {
  /* Colors */
  --bg: #0B0E13;
  --panel: #11131A;
  --accent: #4C8BF5;
  --accent-2: #7EE787;
  --text: #E6E6E6;
  --muted: #9AA3B2;
  --warn: #F59E0B;
  --ok: #22C55E;
  
  /* Spacing */
  --radius: 12px;
  --glass: rgba(255,255,255,0.03);
  --glass-hover: rgba(255,255,255,0.06);
  
  /* Timing */
  --transition-fast: 120ms;
  --transition-normal: 180ms;
  --transition-slow: 360ms;
}
```

### Tailwind Extensions
```js
theme: {
  colors: {
    bg: "#0B0E13",
    panel: "#11131A",
    accent: "#4C8BF5",
    "accent-2": "#7EE787",
    text: "#E6E6E6",
    muted: "#9AA3B2",
    warn: "#F59E0B",
    ok: "#22C55E"
  },
  borderRadius: {
    DEFAULT: "12px",
    sm: "8px",
    lg: "16px"
  },
  animation: {
    "token-in": "tokenIn 360ms cubic-bezier(.2,.9,.3,1)",
    "node-pulse": "nodePulse 2s cubic-bezier(.4,0,.6,1) infinite"
  }
}
```

## Examples

### Message Card
```html
<div class="p-4 rounded bg-panel border border-panel/50 hover:border-accent/30 transition-all glass">
  <div class="flex items-center justify-between mb-2">
    <span class="text-xs font-mono text-accent">prompt.in</span>
    <span class="text-xs text-muted">2.4ms</span>
  </div>
  <div class="text-sm text-text">What is machine learning?</div>
  <div class="text-xs text-muted mt-2">14:32:05</div>
</div>
```

### Active Node
```html
<g class="cursor-pointer">
  <rect
    x="40" y="120" width="120" height="60" rx="8"
    fill="rgba(17,19,26,0.8)"
    stroke="#4C8BF5"
    stroke-width="2"
  />
  <text x="100" y="155" text-anchor="middle" fill="#E6E6E6">
    Ollama Transform
  </text>
</g>
```

### Status Badge
```html
<div class="inline-flex items-center gap-1 px-2 py-1 rounded bg-ok/10 border border-ok/30">
  <div class="w-2 h-2 rounded-full bg-ok animate-pulse" />
  <span class="text-xs font-medium text-text">Connected</span>
</div>
```

## Tools

- **Design**: Figma (community file in progress)
- **Icons**: SVG + Tailwind (custom sprites)
- **Colors**: https://www.tints.dev/?color=0B0E13
- **Contrast**: https://www.tinycolor.tools/
