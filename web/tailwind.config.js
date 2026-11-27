/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'control-bg': '#0f172a',
        'control-panel': '#1e293b',
        'control-border': '#334155',
        'control-text': '#e2e8f0',
      },
    },
  },
  plugins: [],
}
