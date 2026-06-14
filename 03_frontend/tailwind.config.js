/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Cloudera navy layer system — dark enterprise background
        cdp: {
          950: '#0B1520',  // deepest background
          900: '#102030',  // panel background
          800: '#162840',  // card / elevated surface
          700: '#1e3a55',  // border / divider
          600: '#2a4f72',  // muted border hover
        },
        // Cloudera primary action blue — signature brand color
        cloudera: {
          DEFAULT: '#0088CC',
          hover:   '#0099DD',
          muted:   '#0066AA',
          faint:   '#003355',
        },
        // AI Agent Platform Theme — Premium Enterprise Palette
        agent: {
          'dark-bg': '#0F141C',      // The Deep Charcoal — main page background
          'dark-surface': '#171E29', // The Interface Cards — panels, cards
          'dark-border': '#242F3E',  // The Structural Lines — dividers, borders
          'text-primary': '#F3F4F6',   // Primary text
          'text-secondary': '#9CA3AF', // Secondary/muted text
          'orange': '#FF5B00',  // The Innovation Orange — CTAs, highlights
          'teal': '#00A3C4',    // The Tech Teal — data flows, metrics
        },
      },
      fontSize: {
        // Enforce 12px minimum for readability in enterprise UI
        'xs':  ['0.75rem',  { lineHeight: '1.125rem' }],  // 12px — minimum body text
        'sm':  ['0.8125rem',{ lineHeight: '1.25rem'  }],  // 13px
        'base':['0.875rem', { lineHeight: '1.375rem' }],  // 14px
      },
    },
  },
  plugins: [],
}
