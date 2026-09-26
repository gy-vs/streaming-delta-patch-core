import { describe, expect, it } from 'vitest';
import { createRolling, strongDigest, weakChecksum } from '../src/checksums.js';
import { makeWeakCollisionPair, pseudoRandomBytes } from './helpers/util.js';

describe('rolling weak checksum (Adler-32)', () => {
  it('incremental add equals one-shot computation', () => {
    const data = pseudoRandomBytes(5000, 7);
    const r = createRolling();
    for (const byte of data) r.add(byte);
    expect(r.value()).toBe(weakChecksum(data));
  });

  it('roll() keeps the checksum equal to the shifted window', () => {
    const data = pseudoRandomBytes(300, 99);
    const n = 64;
    const r = createRolling(data.subarray(0, n));
    for (let i = 0; i + n < data.length; i++) {
      r.roll(data[i]!, data[i + n]!);
      expect(r.value()).toBe(weakChecksum(data.subarray(i + 1, i + 1 + n)));
    }
  });

  it('distinguishes all-zero blocks by position in b', () => {
    expect(weakChecksum(Buffer.alloc(0))).not.toBe(weakChecksum(Buffer.alloc(1)));
    expect(weakChecksum(Buffer.alloc(1))).not.toBe(weakChecksum(Buffer.alloc(2)));
  });

  it('the constructed collision pair really collides on weak but differs in strong', () => {
    const [a, b] = makeWeakCollisionPair(128);
    expect(weakChecksum(a)).toBe(weakChecksum(b));
    expect(strongDigest(a).equals(strongDigest(b))).toBe(false);
  });
});
