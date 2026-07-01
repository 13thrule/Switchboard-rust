import forms from '@tailwindcss/forms'

export default {
  content: [
    "./index.html",
    "./src/**/*.{svelte,js,ts}"
  ],
  theme: {
    extend: {
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
      fontFamily: {
        ui: ["Inter", "sans-serif"],
        mono: ["JetBrains Mono", "monospace"]
      },
      borderRadius: {
        DEFAULT: "12px"
      },
      spacing: {
        gutter: "24px"
      },
      animation: {
        "token-in": "tokenIn 360ms cubic-bezier(.2,.9,.3,1)",
        "node-pulse": "nodePulse 2s cubic-bezier(.4,0,.6,1) infinite"
      },
      keyframes: {
        tokenIn: {
          "0%": { transform: "translateX(12px) scale(.98)", opacity: "0", filter: "blur(2px)" },
          "60%": { transform: "translateX(-4px) scale(1.02)", opacity: "1", filter: "blur(0)" },
          "100%": { transform: "translateX(0) scale(1)", opacity: "1" }
        },
        nodePulse: {
          "0%, 100%": { boxShadow: "0 8px 24px rgba(76,139,245,0.12)" },
          "50%": { boxShadow: "0 12px 32px rgba(76,139,245,0.24)" }
        }
      },
      backdropBlur: {
        xs: "2px"
      }
    }
  },
  plugins: [forms]
}
