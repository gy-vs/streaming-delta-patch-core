import { defineConfig } from 'vitest/config';
import { existsSync } from 'node:fs';
import { dirname, resolve as resolvePath } from 'node:path';
import { fileURLToPath } from 'node:url';

// 让 TS 源码中的 ".js" 相对导入（NodeNext ESM 约定）在 vitest 下解析到 ".ts"。
function tsExtensionResolve() {
  return {
    name: 'ts-extension-resolve',
    enforce: 'pre' as const,
    resolveId(source: string, importer: string | undefined) {
      if (
        importer &&
        (source.startsWith('./') || source.startsWith('../')) &&
        source.endsWith('.js')
      ) {
        const importerPath = importer.startsWith('file:')
          ? fileURLToPath(importer)
          : importer;
        const candidate = resolvePath(
          dirname(importerPath),
          source.slice(0, -3) + '.ts',
        );
        if (existsSync(candidate)) return candidate;
      }
      return null;
    },
  };
}

export default defineConfig({
  plugins: [tsExtensionResolve()],
  test: {
    include: ['test/**/*.test.ts'],
    // threads 池（worker_threads）不继承父进程 stdio，
    // 在 npm 脚本/CI 等 stdio 被管道持有的环境里测试结束后能可靠退出。
    pool: 'threads',
    // 大文件内存测试是 I/O 密集型；串行执行输出及时、不互相抢磁盘/内存。
    fileParallelism: false,
  },
});
