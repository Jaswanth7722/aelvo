/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        // Warm light surfaces — snow white + golden white
        surface: {
          DEFAULT: "#FFFFFF", // snow white
          alt: "#FFF7EC", // golden white
          border: "#F4E2C9", // soft light-orange border
        },
        brand: {
          orange: "#FF9F45", // light orange (primary)
          gold: "#FFC98A", // golden
          cream: "#FFF7EC", // golden white
          snow: "#FFFFFF",
          purple: "#8B5CF6",
          deep: "#7C3AED",
        },
        accent: {
          blue: "#7C3AED", // purple used for primary actions
          green: "#16A34A",
          purple: "#8B5CF6",
          red: "#E11D48",
          amber: "#F59E0B",
          cyan: "#0891B2",
          orange: "#FF9F45",
        },
        ink: {
          DEFAULT: "#1F2937", // primary text
          soft: "#4B5563",
          muted: "#9CA3AF",
        },
      },
      fontFamily: {
        sans: [
          "Inter",
          "ui-sans-serif",
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "sans-serif",
        ],
        mono: ["JetBrains Mono", "Fira Code", "monospace"],
      },
      boxShadow: {
        soft: "0 1px 3px 0 rgba(124, 58, 237, 0.06), 0 1px 2px -1px rgba(124, 58, 237, 0.06)",
        card: "0 4px 16px -4px rgba(255, 159, 69, 0.12), 0 2px 8px -2px rgba(124, 58, 237, 0.08)",
        glow: "0 0 24px -6px rgba(255, 159, 69, 0.45)",
      },
      keyframes: {
        "fade-up": {
          "0%": { opacity: "0", transform: "translateY(10px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        shimmer: {
          "0%": { backgroundPosition: "-200% 0" },
          "100%": { backgroundPosition: "200% 0" },
        },
        "pulse-glow": {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.45" },
        },
        float: {
          "0%, 100%": { transform: "translateY(0)" },
          "50%": { transform: "translateY(-6px)" },
        },
        "spin-slow": {
          from: { transform: "rotate(0deg)" },
          to: { transform: "rotate(360deg)" },
        },
      },
      animation: {
        "fade-up": "fade-up 0.4s ease-out both",
        shimmer: "shimmer 1.6s linear infinite",
        "pulse-glow": "pulse-glow 1.8s ease-in-out infinite",
        float: "float 3s ease-in-out infinite",
        "spin-slow": "spin-slow 8s linear infinite",
      },
    },
  },
  plugins: [],
};
