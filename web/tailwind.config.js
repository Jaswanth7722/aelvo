/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        surface: {
          DEFAULT: "#0d1117",
          alt: "#161b22",
          border: "#21262d",
        },
        accent: {
          blue: "#3b82f6",
          green: "#00e38c",
          purple: "#8c5cff",
          red: "#ff5c7a",
          amber: "#f7b731",
          cyan: "#39c8ff",
        },
      },
      fontFamily: {
        mono: ["JetBrains Mono", "Fira Code", "monospace"],
      },
    },
  },
  plugins: [],
};
