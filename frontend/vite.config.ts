import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/database-agent/',
  optimizeDeps: {
    exclude: ['@duckdb/duckdb-wasm'],
  },
})
