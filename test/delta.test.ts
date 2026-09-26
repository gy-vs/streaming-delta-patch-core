import { describe, expect, it } from 'vitest';
import { Writable } from 'node:stream';
import {
  applyPatch,
  applyPatchToBuffer,
  bufferSource,
  generatePatch,
  type RandomReadSource,
} from '../src/index.js';
import {
  chunkify,
  collect,
  decodePatch,
  pseudoRandomBytes,
  sha256,
} from './helpers/util.js';
import { SignatureTable } from '../src/signatures.js';
import { applyPatchToWritable } from '../src/applier.js';

const BS = 16; // 小块尺寸，便于构造边界场景

async function makePatch(
  oldData: Buffer,
  newData: Buffer | Buffer[],
  blockSize = BS,
  strong?: (data: Buffer) => Buffer,
): Promise<Buffer> {
  const input = Array.isArray(newData) ? newData : [newData];
  return collect(
    generatePatch(bufferSource(oldData), input, blockSize, strong ? { strong } : undefined),
  );
}

async function roundtrip(oldData: Buffer, newData: Buffer, blockSize = BS): Promise<{
  patch: Buffer;
  decoded: ReturnType<typeof decodePatch>;
  result: Buffer;
}> {
  const patch = await makePatch(oldData, newData, blockSize);
  const decoded = decodePatch(patch);
  const result = await applyPatchToBuffer(bufferSource(oldData), [patch]);
  expect(result.equals(newData)).toBe(true);
  return { patch, decoded, result };
}

describe('delta roundtrips', () => {
  it('empty old and empty new produces header + END only', async () => {
    const { decoded, result } = await roundtrip(Buffer.alloc(0), Buffer.alloc(0));
    expect(decoded.ops.map((o) => o.kind)).toEqual(['END']);
    expect(decoded.oldSize).toBe(0n);
    expect(result.length).toBe(0);
  });

  it('empty old, non-empty new becomes INSERTs only', async () => {
    const next = Buffer.from('brand new world');
    const { decoded } = await roundtrip(Buffer.alloc(0), next);
    expect(decoded.ops.filter((o) => o.kind === 'COPY')).toHaveLength(0);
    const inserted = Buffer.concat(
      decoded.ops.filter((o) => o.kind === 'INSERT').map((o) => (o as { data: Buffer }).data),
    );
    expect(inserted.equals(next)).toBe(true);
  });

  it('non-empty old, empty new emits no COPY/INSERT and zero END length', async () => {
    const oldData = Buffer.from('some old bytes here')!;
    const { decoded, result } = await roundtrip(oldData, Buffer.alloc(0));
    expect(decoded.ops.map((o) => o.kind)).toEqual(['END']);
    expect(result.length).toBe(0);
    expect(decoded.ops[0]!.kind).toBe('END');
    expect((decoded.ops[0] as { newSize: bigint }).newSize).toBe(0n);
  });

  it('identical inputs produce a single COPY covering everything', async () => {
    const data = Buffer.concat([pseudoRandomBytes(BS * 3 + 5, 1)]);
    const { decoded } = await roundtrip(data, Buffer.from(data));
    const copies = decoded.ops.filter((o) => o.kind === 'COPY');
    const inserts = decoded.ops.filter((o) => o.kind === 'INSERT');
    expect(inserts).toHaveLength(0);
    const covered = copies.reduce((n, o) => n + (o as { length: number }).length, 0);
    expect(covered).toBe(data.length);
  });

  it('insertion exactly at a block boundary: INSERT between two COPYs', async () => {
    const oldData = Buffer.from('ABCDEFGHIJKLMNOPqrstuvwxyz012345'); // 16 + 16
    expect(oldData.length).toBe(BS * 2);
    const next = Buffer.concat([
      oldData.subarray(0, BS), // 完整第一块，COPY
      Buffer.from('--INSERTED--'), // 边界处插入
      oldData.subarray(BS), // 完整第二块，COPY
    ]);
    const { decoded, result } = await roundtrip(oldData, next);
    const kinds = decoded.ops.map((o) => o.kind);
    expect(kinds).toEqual(['COPY', 'INSERT', 'COPY', 'END']);
    expect((decoded.ops[1] as { data: Buffer }).data.toString()).toBe('--INSERTED--');
    expect(result.equals(next)).toBe(true);
  });

  it('duplicate blocks resolve with a stable left-most tie break', async () => {
    const block = pseudoRandomBytes(BS, 42);
    const oldData = Buffer.concat([
      block, // index 0, offset 0
      pseudoRandomBytes(BS, 43), // index 1
      Buffer.from(block), // index 2, offset 2*BS，内容与 index 0 相同
    ]);
    const next = Buffer.concat([block, block]); // 两个重复块
    const { decoded } = await roundtrip(oldData, next);
    const copies = decoded.ops.filter((o) => o.kind === 'COPY') as {
      offset: number;
      length: number;
    }[];
    expect(copies).toHaveLength(2);
    // 两次都必须决胜到最靠左的 index 0（offset 0），而不是 index 2
    expect(copies[0]!.offset).toBe(0);
    expect(copies[1]!.offset).toBe(0);
  });

  it('completely different data is all INSERT bytes', async () => {
    const oldData = pseudoRandomBytes(BS * 4, 100);
    const next = pseudoRandomBytes(BS * 4, 200);
    const { decoded } = await roundtrip(oldData, next);
    expect(decoded.ops.filter((o) => o.kind === 'COPY')).toHaveLength(0);
    const inserted = Buffer.concat(
      decoded.ops
        .filter((o) => o.kind === 'INSERT')
        .map((o) => (o as { data: Buffer }).data),
    );
    expect(inserted.equals(next)).toBe(true);
  });

  it('handles a short tail block in both old and new', async () => {
    const oldData = pseudoRandomBytes(BS * 2 + 7, 5);
    const next = Buffer.from(oldData);
    const { decoded } = await roundtrip(oldData, next);
    const copies = decoded.ops.filter((o) => o.kind === 'COPY') as {
      offset: number;
      length: number;
    }[];
    const covered = copies.reduce((n, c) => n + c.length, 0);
    expect(covered).toBe(oldData.length);
    // 短块也应命中，而不是作为 INSERT
    expect(copies.some((c) => c.length === 7)).toBe(true);
  });

  it('short new tail that only exists at old EOF is matched even without more input', async () => {
    const oldData = pseudoRandomBytes(BS * 3 + 3, 11);
    const next = oldData.subarray(oldData.length - 3); // 恰好是旧尾部短块
    const { decoded, result } = await roundtrip(oldData, next);
    const copies = decoded.ops.filter((o) => o.kind === 'COPY');
    expect(copies).toHaveLength(1);
    expect((copies[0] as { length: number }).length).toBe(3);
    expect(result.equals(next)).toBe(true);
  });

  it('old size divisible by block size and new shorter than one block', async () => {
    const oldData = pseudoRandomBytes(BS * 2, 67); // 整除，无短块
    const next = oldData.subarray(5, 10); // 5 字节，不可能匹配任何块
    const { decoded, result } = await roundtrip(oldData, next);
    expect(decoded.ops.filter((o) => o.kind === 'COPY')).toHaveLength(0);
    expect(result.equals(next)).toBe(true);
  });

  it('delete everything: new empty against block-aligned old', async () => {
    const oldData = pseudoRandomBytes(BS * 2, 68);
    const { result } = await roundtrip(oldData, Buffer.alloc(0));
    expect(result.length).toBe(0);
  });

  it('END carries the correct total length and digest', async () => {
    const oldData = pseudoRandomBytes(BS * 2, 13);
    const next = Buffer.concat([Buffer.from('x'), oldData.subarray(2, 20), Buffer.from('yy')]);
    const { decoded } = await roundtrip(oldData, next);
    const end = decoded.ops[decoded.ops.length - 1] as {
      newSize: bigint;
      digest: Buffer;
    };
    expect(end.newSize).toBe(BigInt(next.length));
    expect(end.digest.equals(sha256(next))).toBe(true);
  });
});

describe('streaming determinism', () => {
  it('same inputs produce byte-identical patches regardless of input chunking', async () => {
    const oldData = pseudoRandomBytes(BS * 10, 21);
    const next = Buffer.from(
      Buffer.concat([Buffer.from('PRE'), oldData.subarray(5, BS * 8), Buffer.from('POST!!')]),
    );
    const p1 = await makePatch(oldData, [next]);
    const p2 = await makePatch(oldData, chunkify(next, [1, 1, 2, 7, 100, 3, 19]));
    const p3 = await makePatch(oldData, chunkify(next, () => 5));
    expect(p2.equals(p1)).toBe(true);
    expect(p3.equals(p1)).toBe(true);
  });

  it('output is deterministic across repeated runs', async () => {
    const oldData = pseudoRandomBytes(BS * 6, 31);
    const next = pseudoRandomBytes(BS * 6, 32);
    const p1 = await makePatch(oldData, next);
    const p2 = await makePatch(oldData, next);
    expect(p1.equals(p2)).toBe(true);
  });
});

describe('fuzz-style property checks', () => {
  it('random edits roundtrip for many seeds', async () => {
    for (let seed = 0; seed < 20; seed++) {
      const len = BS * (2 + (seed % 5)) + (seed % BS);
      const oldData = pseudoRandomBytes(len, seed * 7 + 1);
      let next: Buffer = Buffer.from(oldData);
      // 随机插入、删除、替换
      const edits = 1 + (seed % 4);
      for (let e = 0; e < edits; e++) {
        const pos = (seed * 13 + e * 7) % Math.max(1, next.length);
        const mode = seed % 3;
        if (mode === 0) {
          const ins = pseudoRandomBytes(1 + (e % BS), seed + e);
          next = Buffer.concat([next.subarray(0, pos), ins, next.subarray(pos)]);
        } else if (mode === 1) {
          const del = Math.min(1 + (e % 3), next.length - pos);
          next = Buffer.concat([next.subarray(0, pos), next.subarray(pos + del)]);
        } else {
          next[pos] = (next[pos]! ^ 0xff) & 0xff;
        }
      }
      const result = await applyPatchToBuffer(bufferSource(oldData), [await makePatch(oldData, next)]);
      expect(result.equals(next), `seed ${seed}`).toBe(true);
    }
  }, 10000);
});

describe('writable destination', () => {
  it('applyPatchToWritable reproduces the new data end-to-end', async () => {
    const oldData = pseudoRandomBytes(BS * 4, 91);
    const next = Buffer.concat([oldData.subarray(0, BS), Buffer.from('++'), oldData.subarray(BS)]);
    const patch = await makePatch(oldData, next);
    const parts: Buffer[] = [];
    const dest = new Writable({
      write(chunk: Buffer, _enc, cb) {
        parts.push(chunk);
        cb();
      },
    });
    await applyPatchToWritable(bufferSource(oldData), [patch], dest);
    expect(Buffer.concat(parts).equals(next)).toBe(true);
  });
});

describe('signature table', () => {
  it('rejects non-positive block sizes', async () => {
    await expect(SignatureTable.build(bufferSource(Buffer.from('x')), 0)).rejects.toThrow();
  });
});

describe('byte-boundary chunking of the patch stream', () => {
  it('applies correctly when every patch byte arrives individually', async () => {
    const oldData = pseudoRandomBytes(BS * 3 + 5, 71);
    const next = Buffer.concat([
      Buffer.from('Z'),
      oldData.subarray(1, BS * 2 + 2),
      Buffer.from('tail'),
    ]);
    const patch = await makePatch(oldData, next);
    const singleBytes = Array.from(patch, (b) => Buffer.from([b]));
    const result = await applyPatchToBuffer(bufferSource(oldData), singleBytes);
    expect(result.equals(next)).toBe(true);
  });
});
