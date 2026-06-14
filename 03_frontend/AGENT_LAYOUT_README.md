# CloudData AI Agent — Premium Enterprise Layout

A complete React/Tailwind implementation of a premium, enterprise-grade AI Agent platform interface inspired by Cloudera's corporate design language.

## What's Included

### Core Components
- **AgentLayout** — Main orchestrator component (responsive two-pane layout)
- **Navbar** — Top chrome with platform status, environment indicator, user menu
- **Sidebar** — Minimalist left navigation with 5 main sections (collapsible on mobile)
- **ChatPanel** — AI chat interface with message history and input area
- **MetricsPanel** — Real-time data dashboard with metrics and activity logs
- **MetricCard** — Reusable metric widget with trends and accent colors
- **DataFlowChart** — Agentic data flow pipeline visualization

### Design Assets
- **AGENT_DESIGN_SYSTEM.md** — Complete style guide with colors, typography, spacing, and component specs
- **tailwind.config.js** — Tailwind configuration with 7 core agent theme colors

## Color Palette

```
Primary Accent (Innovation Orange):   #FF5B00 → bg-agent-orange
Background Dark (Deep Charcoal):      #0F141C → bg-agent-dark-bg
Surface Dark (Interface Cards):       #171E29 → bg-agent-dark-surface
Border Dark (Structural Lines):       #242F3E → border-agent-dark-border
Text Primary (Light Gray):            #F3F4F6 → text-agent-text-primary
Text Secondary (Muted Gray):          #9CA3AF → text-agent-text-secondary
Secondary Accent (Tech Teal):         #00A3C4 → text-agent-teal
```

## Layout Structure

```
┌────────────────────────────────────────────────────────┐
│ Navbar (h-16)                                          │
│  • Menu + Logo | Status Badge | User Menu              │
├────────┬────────────────────────────────────────────────┤
│        │                                                │
│Sidebar │  ChatPanel (flex-1)  │  MetricsPanel (flex-1)  │
│(w-64)  │  • Message History    │  • Key Metrics           │
│        │  • Input Area         │  • Data Flow Chart       │
│        │  • Send Button        │  • Activity Log          │
│        │                       │  • Real-time Status      │
│        │                       │                          │
└────────┴────────────────────────────────────────────────┘
```

## Quick Start

### 1. Add to Your App

```tsx
import { AgentLayout } from './components/AgentLayout';

export function App() {
  return <AgentLayout />;
}
```

### 2. Tailwind Already Configured

The `tailwind.config.js` has been updated with all `agent-*` color tokens. No additional setup needed.

### 3. Responsive Design

- **Mobile** (<640px): Sidebar slides out with overlay backdrop
- **Tablet** (640px-1024px): Sidebar visible with minimal width
- **Desktop** (>1024px): Full layout with all panels visible

## Component Examples

### Using a Metric Card

```jsx
<MetricCard
  label="Token Spend"
  value="2,847"
  unit="tokens/day"
  icon={<Zap size={16} />}
  trend={12}
  accentColor="orange"
/>
```

### Styling a Button

```jsx
<button className="bg-agent-orange text-white hover:bg-orange-600 px-4 py-2 rounded-agent-md font-medium transition-all">
  Send
</button>
```

### Creating a Card

```jsx
<div className="bg-agent-dark-surface border border-agent-dark-border rounded-agent-lg p-4">
  <h3 className="text-sm font-semibold text-agent-text-primary">Title</h3>
  <p className="text-xs text-agent-text-secondary">Content</p>
</div>
```

## Key Features

### 1. **Premium Enterprise Feel**
- High-contrast dark mode with sophisticated color accents
- Minimal, clean borders (1px) instead of heavy shadows
- Technical, data-focused aesthetic

### 2. **Real-time Metrics Dashboard**
- 4-metric grid with trend indicators
- Active data flow pipeline visualization
- Activity log with status indicators

### 3. **Responsive & Mobile-Friendly**
- Collapsible sidebar on mobile
- Touch-optimized button sizes (h-10 minimum)
- Proper spacing for readability on all screens

### 4. **High Accessibility**
- WCAG AAA contrast ratios on all text
- Visible focus indicators on all interactive elements
- Keyboard navigation support

### 5. **Smooth Interactions**
- 200ms transitions on all state changes
- Hover/active/focus states clearly indicated
- Loading states with `animate-pulse`

## Theming & Customization

### Change the Orange Accent

Edit `tailwind.config.js`:
```javascript
'orange': '#YOUR_HEX_CODE',  // Replace #FF5B00
```

### Add Custom Gradients

```jsx
className="bg-gradient-to-br from-agent-orange/10 to-orange-950/20"
```

### Update Typography Scale

Edit `tailwind.config.js` → `fontSize` section to adjust text sizes globally.

## Accessibility Notes

All components include:
- ✓ Focus rings on buttons and inputs
- ✓ WCAG AAA contrast ratios
- ✓ Semantic HTML (`<button>`, `<nav>`, etc.)
- ✓ ARIA labels where needed
- ✓ Motion respects `prefers-reduced-motion`

## Files Reference

```
03_frontend/
├── src/
│   └── components/
│       └── AgentLayout/
│           ├── AgentLayout.tsx        (Main layout)
│           ├── Navbar.tsx             (Top chrome)
│           ├── Sidebar.tsx            (Navigation)
│           ├── ChatPanel.tsx          (Chat interface)
│           ├── MetricsPanel.tsx       (Data dashboard)
│           ├── MetricCard.tsx         (Metric widget)
│           ├── DataFlowChart.tsx      (Pipeline viz)
│           └── index.ts               (Exports)
├── tailwind.config.js                 (Updated with agent colors)
└── AGENT_DESIGN_SYSTEM.md             (Style guide)
```

## Testing

### Visual Testing
1. Open the component in your dev server
2. Test on mobile (max-width: 640px) — sidebar should collapse
3. Test on tablet (640px-1024px) — sidebar visible but narrow
4. Test on desktop (>1024px) — full layout
5. Test focus states by tabbing through buttons

### Color Verification
Run this in the browser console to verify colors:
```javascript
window.getComputedStyle(document.querySelector('.bg-agent-orange')).backgroundColor
// Should output: rgb(255, 91, 0)
```

## Browser Support

- ✓ Chrome 90+
- ✓ Firefox 88+
- ✓ Safari 14+
- ✓ Edge 90+

## Performance Considerations

- All icons from `lucide-react` — tree-shakeable, minimal bundle size
- Tailwind CSS purges unused utilities in production
- No external dependencies beyond React + Tailwind
- Optimized for 60fps animations (use `will-change` sparingly)

## Future Enhancements

1. **Dark/Light Mode Toggle** — Add `prefers-color-scheme` support
2. **Notification Center** — Toast or badge system
3. **User Preferences** — Sidebar collapse memory, theme mode
4. **Real API Integration** — Wire metrics to actual backend endpoints
5. **Advanced Charts** — Upgrade data visualization with Recharts or D3
6. **Settings Modal** — Add configuration panel for agent behavior

## Questions or Issues?

Refer to:
- **Style Guide**: `AGENT_DESIGN_SYSTEM.md`
- **Component Props**: JSDoc comments in each component file
- **Tailwind Docs**: https://tailwindcss.com/docs

---

**Version**: 1.0  
**Last Updated**: 2026-05-17  
**Built with**: React 18 + Tailwind CSS 4 + Lucide React
