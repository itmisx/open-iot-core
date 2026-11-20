import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 产物经 go:embed 打进 openiotd 二进制（PRD LANG-018、UI-001）。
//
// base 用相对路径：控制台可能被挂在反向代理的子路径下
// （UI-001 要求支持），绝对路径会让静态资源 404。
export default defineConfig({
  plugins: [react()],
  base: './',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    // 产物提交进仓库，使无 Node 环境时 Go 侧构建不被阻塞（UI-039）。
    // 因此关掉文件名哈希——每次构建都变的哈希会让 diff 充满噪声，
    // 而嵌入式产物不需要 CDN 缓存失效。
    rollupOptions: {
      output: {
        entryFileNames: 'assets/app.js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name][extname]',
      },
    },
    // UI-038 要求主 bundle（gzip 后）在 400 KB 以内。
    // 设成 500 KB 的未压缩告警线，超了会在构建日志里看到。
    chunkSizeWarningLimit: 500,
    sourcemap: false,
  },
  server: {
    // 开发时把 API 代理到本地 openiotd
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:9090',
        changeOrigin: true,
      },
    },
  },
})
