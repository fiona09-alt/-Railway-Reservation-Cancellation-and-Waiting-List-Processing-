import tailwindcss from 'tailwindcss';
import autoprefixer from 'autoprefixer';
import path from 'node:path';

export default {
  // Absolute path: postcss runs with CWD = repo root, config lives in client/.
  plugins: [tailwindcss(path.resolve('client/tailwind.config.js')), autoprefixer()],
};
