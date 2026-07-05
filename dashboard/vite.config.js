import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The Docker build serves via nginx (which proxies /api and /ws to the
// gateway). This dev proxy provides the same single-origin behavior for a
// local `npm run dev` loop against a gateway on localhost:8080.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/ws': {
        target: 'ws://localhost:8080',
        ws: true,
      },
    },
  },
});
