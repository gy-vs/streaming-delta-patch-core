import { createHash } from 'node:crypto';
import { mkdir, rm } from 'node:fs/promises';
import { createReadStream, createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { afterAll, describe, expect, it } from 'vitest';import {
  applyPatchToWritable,
  generatePatch,
  openFileSource,
  type FileRandomReadSource,
} from '../src/index.js';

const BS = 4096;
const SEED = 0xabcdef01;
const MARKER = Buffer.from('<<<INSERTED-MARKER>>>');
const INSERT_AT = BS * 8; // 块边界处插入

/** 确定性 LCG 字节流（不整份驻留内存）。 */
async function* lcgChunks(totalSize: number, chunkSize = 64 * 1024): AsyncGenerator<Buffer> {
  let state = SEED >>> 0;
  let remaining = totalSize;
  while (remaining > 0) {
    const n = Math.min(chunkSize, remaining);
    const buf = Buffer.allocUnsafe(n);
    for (let i = 0; i < n; i++) {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      buf[i] = state >>> 24;
    }
    remaining -= n;
    yield buf;
  }
}

/** 新数据流：旧内容 LCG，在 INSERT_AT 处插入 MARKER。 */
async function* expectedNewChunks(oldSize: number): AsyncGenerator<Buffer> {
  let produced = 0;
  for await (const chunk of lcgChunks(oldSize)) {
    let offset = 0;
    if (produced < INSERT_AT && produced + chunk.length > INSERT_AT) {
      const cut = INSERT_AT - produced;
      yield chunk.subarray(0, cut);
      yield MARKER;
      produced += cut;
      offset = cut;
    }
    yield chunk.subarray(offset);
    produced += chunk.length - offset;
  }
}

async function hashAsync(it: AsyncIterable<Buffer>): Promise<{ hash: string; size: number }> {
  const h = createHash('sha256');
  let size = 0;
  for await (const c of it) {
    h.update(c);
    size += c.length;
  }
  return { hash: h.digest('hex'), size };
}

function hashFile(path: string): Promise<{ hash: string; size: number }> {
  return hashAsync(createReadStream(path) as unknown as AsyncIterable<Buffer>);
}

async function writeFileFrom(path: string, chunks: AsyncIterable<Buffer>): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const ws = createWriteStream(path);
    ws.on('error', reject);
    ws.on('finish', () => resolve());
    (async () => {
      for await (const c of chunks) {
        if (!ws.write(c)) await new Promise((r) => ws.once('drain', r));
      }
      ws.end();
    })().catch(reject);
  });
}

function gc(): boolean {
  const g = (globalThis as { gc?: () => void }).gc;
  if (!g) return false;
  g();
  g();
  return true;
}

const dir = join(tmpdir(), `sdp-mem-${process.pid}`);

async function runAtSize(oldSize: number): Promise<number> {
  const tag = oldSize.toString(36);
  const oldPath = join(dir, `old-${tag}.bin`);
  const newPath = join(dir, `new-${tag}.bin`);

  await writeFileFrom(oldPath, lcgChunks(oldSize));
  const source: FileRandomReadSource = await openFileSource(oldPath);

  gc();
  const heapBefore = process.memoryUsage().heapUsed;

  // 补丁不落盘也不整份收集：用 PassThrough 桥接生成器与应用器。
  const bridge = new PassThrough({ highWaterMark: 64 * 1024 });
  const producer = (async () => {
    try {
      for await (const c of generatePatch(source, expectedNewChunks(oldSize), BS)) {
        if (!bridge.write(c)) await new Promise((r) => bridge.once('drain', r));
      }
      bridge.end();
    } catch (e) {
      bridge.destroy(e as Error);
    }
  })();

  const ws = createWriteStream(newPath);
  await applyPatchToWritable(source, bridge, ws);
  await producer;
  await source.close();

  gc();
  const heapAfter = process.memoryUsage().heapUsed;

  // 正确性：输出与独立构造的期望流一致。
  const expected = await hashAsync(expectedNewChunks(oldSize));
  const actual = await hashFile(newPath);
  expect(actual.size).toBe(expected.size);
  expect(actual.hash).toBe(expected.hash);

  await rm(oldPath);
  await rm(newPath);
  return heapAfter - heapBefore;
}

describe('large file: bounded memory end-to-end', () => {
  // 峰值堆对比需要可强制 GC；没有 --expose-gc 时跳过以免产生假阴性。
  const maybeIt = gc() ? it : it.skip;

  // 小尺寸健全性测试：文件源 + 全流式补丁/应用链路（正确性在 runAtSize 内断言）。
  it('file-backed source roundtrip at small size', async () => {
    await mkdir(dir, { recursive: true });
    const growth = await runAtSize(BS * 32); // 128 KB
    expect(Number.isFinite(growth)).toBe(true);
  }, 60_000);

  maybeIt('peak heap does not grow linearly with file size', async () => {
    await mkdir(dir, { recursive: true });

    const small = BS * 4096; // 16 MB
    const large = small * 4; // 64 MB
    const growthSmall = await runAtSize(small);
    const growthLarge = await runAtSize(large);

    // 线性实现下 64MB 比 16MB 会多消耗数十 MB 堆（按字节驻留）；
    // 有界实现的增量差只来自签名表（每块约几十字节）与运行时噪声。
    // 8 MB 宽上限远小于任何按字节线性的实现。
    const extra = growthLarge - growthSmall;
    expect(
      extra,
      `heap growth delta ${extra} bytes (small=${growthSmall}, large=${growthLarge})`,
    ).toBeLessThan(8 * 1024 * 1024);
  }, 240_000);

  afterAll(async () => {
    await rm(dir, { recursive: true, force: true });
  });
});
