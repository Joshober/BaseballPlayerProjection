/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        scout: { ink: "#0c1426", clay: "#c45c3e", field: "#1a3d2e", chalk: "#f4f1ea" },
      },
      fontFamily: {
        display: ["Georgia", "Cambria", "serif"],
        sans: ["system-ui", "Segoe UI", "sans-serif"],
      },
    },
  },
  plugins: [],
};
