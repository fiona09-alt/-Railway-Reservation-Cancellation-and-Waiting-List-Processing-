/** @type {import('tailwindcss').Config} */
export default {
  // Content globs resolve relative to CWD (repo root) — hence client/ prefix.
  content: ['./client/index.html', './client/src/**/*.{js,jsx}'],
  theme: { extend: {} },
  plugins: [],
};
