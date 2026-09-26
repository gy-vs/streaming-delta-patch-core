import { createHash } from 'node:crypto';

/**
 * 弱滚动校验：Adler-32（模 65521）。
 *
 * 对字节块 [b0..b_{n-1}] 定义：
 *   a = 1 + Σ (b_i + 1)
 *   b = n + Σ_{i=0..n-1} (n - i) * (b_i + 1)
 * checksum = (b << 16) | a
 *
 * 字节按 (b + 1) 计入，使全零数据仍有随位置变化的 b 分量。
 * 滚动：移出字节 x、移入字节 y 时
 *   a' = a - (x+1) + (y+1)
 *   b' = b - n*(x+1) + (a' - 1)
 */
export const ADLER_MOD = 65521;

export interface Rolling {
  /** 当前窗口内的字节数。 */
  readonly length: number;
  /** 32 位组合校验值，低 16 位 a，高 16 位 b。 */
  value(): number;
  /** 推入一个字节（窗口增长阶段）。 */
  add(byte: number): void;
  /** 移出最左字节并在右侧移入新字节，窗口长度保持不变。 */
  roll(outByte: number, inByte: number): void;
}

export function createRolling(initial?: Buffer): Rolling {
  let a = 1;
  let b = 0;
  let length = 0;

  const r: Rolling = {
    get length() {
      return length;
    },
    value() {
      return ((b & 0xffff) << 16) | (a & 0xffff);
    },
    add(byte: number) {
      const v = (byte & 0xff) + 1;
      a = (a + v) % ADLER_MOD;
      b = (b + a) % ADLER_MOD; // b 累加的是"加入该字节后的 a"
      length++;
    },
    roll(outByte: number, inByte: number) {
      const ov = (outByte & 0xff) + 1;
      const iv = (inByte & 0xff) + 1;
      a = (((a - ov + iv) % ADLER_MOD) + ADLER_MOD) % ADLER_MOD;
      // 旧 b = Σ length 个历史 a_k；去掉 length*ov，再加上新 a-1
      // （新窗口的 a' = 1 + Σ' ，故 Σ' = a' - 1）。
      b = (((b - length * ov + a - 1) % ADLER_MOD) + ADLER_MOD) % ADLER_MOD;
    },
  };

  if (initial) for (const byte of initial) r.add(byte);
  return r;
}

/** 一次性计算一段数据的弱校验值（语义与 {@link createRolling} 相同）。 */
export function weakChecksum(data: Buffer): number {
  const r = createRolling(data);
  return r.value();
}

/** 强摘要：SHA-256，返回 32 字节摘要。 */
export function strongDigest(data: Buffer): Buffer {
  return createHash('sha256').update(data).digest();
}
