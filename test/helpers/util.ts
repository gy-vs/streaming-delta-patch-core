import { createHash } from 'node:crypto';
import {
  FOOTER_SIZE,
  HEADER_SIZE,
  MAGIC,
  OP_COPY,
  OP_END,
  OP_INSERT,
} from '../../src/format.js';

export type DecodedOp =
  | { kind: 'COPY'; offset: number; length: number }
  | { kind: 'INSERT'; data: Buffer }
  | { kind: 'END'; newSize: bigint; digest: Buffer };

export interface DecodedPatch {
  blockSize: number;
  oldSize: bigint;
  ops: DecodedOp[];
}

/** 仅用于测试断言：解码整条补丁。 */
export function decodePatch(buf: Buffer): DecodedPatch {
  if (!buf.subarray(0, 4).equals(MAGIC)) throw new Error('bad magic in test helper');
  const blockSize = buf.readUInt32LE(6);
  const oldSize = buf.readBigUInt64LE(10);
  const ops: DecodedOp[] = [];
  let p = HEADER_SIZE;
  while (p < buf.length) {
    const op = buf.readUInt8(p++);
    if (op === OP_COPY) {
      const offset = Number(buf.readBigUInt64LE(p));
      p += 8;
      const length = buf.readUInt32LE(p);
      p += 4;
      ops.push({ kind: 'COPY', offset, length });
    } else if (op === OP_INSERT) {
      const length = buf.readUInt32LE(p);
      p += 4;
      ops.push({ kind: 'INSERT', data: Buffer.from(buf.subarray(p, p + length)) });
      p += length;
    } else if (op === OP_END) {
      const newSize = buf.readBigUInt64LE(p);
      const digest = Buffer.from(buf.subarray(p + 8, p + 8 + 32));
      p += FOOTER_SIZE - 1;
      ops.push({ kind: 'END', newSize, digest });
    } else {
      throw new Error(`unknown opcode 0x${op.toString(16)} at ${p - 1}`);
    }
  }
  return { blockSize, oldSize, ops };
}

export async function collect(it: AsyncIterable<Buffer>): Promise<Buffer> {
  const parts: Buffer[] = [];
  for await (const c of it) parts.push(c);
  return Buffer.concat(parts);
}

/** 确定性 LCG 伪随机字节，供大文件/往返测试使用。 */
export function pseudoRandomBytes(size: number, seed = 0x12345678): Buffer {
  let state = seed >>> 0;
  const buf = Buffer.allocUnsafe(size);
  for (let i = 0; i < size; i++) {
    // Numerical Recipes LCG
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    buf[i] = state >>> 24;
  }
  return buf;
}

export function sha256(buf: Buffer): Buffer {
  return createHash('sha256').update(buf).digest();
}

export function chunkify(buf: Buffer, sizes: number[] | ((i: number) => number)): Buffer[] {
  const out: Buffer[] = [];
  let pos = 0;
  let i = 0;
  while (pos < buf.length) {
    const size = typeof sizes === 'function' ? sizes(i) : sizes[i % sizes.length]!;
    const n = Math.max(1, Math.min(size, buf.length - pos));
    out.push(buf.subarray(pos, pos + n));
    pos += n;
    i++;
  }
  return out;
}

/**
 * 构造两个等长、不同内容但 Adler-32 弱校验相同的块：
 * 在三个相邻位置施加 (d, -2d, d) 扰动，a 与 b 均保持不变。
 */
export function makeWeakCollisionPair(blockSize: number, d = 1): [Buffer, Buffer] {
  const a = Buffer.alloc(blockSize, 0);
  // 用可预测模式填充，保证被扰动字节有足够余量。
  for (let i = 0; i < blockSize; i++) a[i] = 0x40 + (i % 16);
  const b = Buffer.from(a);
  b[10] = a[10]! + d;
  b[11] = a[11]! - 2 * d;
  b[12] = a[12]! + d;
  return [a, b];
}
