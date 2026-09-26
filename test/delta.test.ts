import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { test } from 'node:test';
import {
  BufferSource,
  DeltaError,
  FORMAT_VERSION,
  OP_COPY,
  OP_END,
  OP_INSERT,
  PATCH_MAGIC,
  applyDelta,
  applyDeltaToBuffer,
  buildSignature,
  createDelta,
  encodeVarint,
  generateDelta,
  inspectPatch,
  parseSignature,
  serializeSignature,
  toAsyncIterable,
} from '../src/index';
import { pattern, toStream } from './helpers';

const sha256 = (buf: Buffer): Buffer => createHash('sha256').update(buf).digest();

function craftPatch(...parts: (Buffer | number)[]): Buffer {
  return Buffer.concat(
    parts.map((p) => (typeof p === 'number' ? Buffer.from([p]) : p)),
  );
}

function patchHeader(oldLength: number, blockSize = 64): Buffer {
  return Buffer.concat([
    PATCH_MAGIC,
    Buffer.from([FORMAT_VERSION]),
    encodeVarint(blockSize),
    encodeVarint(oldLength),
    Buffer.alloc(32), // old digest (not verified by the applier)
  ]);
}

// ---------------------------------------------------------------------------
// Round-trip basics
// ---------------------------------------------------------------------------

test('empty old and empty new produce an empty result', async () => {
  const old = new BufferSource(Buffer.alloc(0));
  const { patch, stats } = await createDelta(old, Buffer.alloc(0), { blockSize: 64 });
  assert.strictEqual(stats.outputBytes, 0);
  const { data, stats: applyStats } = await applyDeltaToBuffer(old, patch);
  assert.strictEqual(data.length, 0);
  assert.strictEqual(applyStats.outputBytes, 0);
});

test('empty old data: everything is INSERTed', async () => {
  const old = new BufferSource(Buffer.alloc(0));
  const fresh = pattern(1, 1000);
  const { patch, stats } = await createDelta(old, fresh, { blockSize: 64 });
  assert.strictEqual(stats.copyOps, 0);
  assert.ok(stats.insertOps >= 1);
  const { data } = await applyDeltaToBuffer(old, patch);
  assert.ok(data.equals(fresh));
});

test('empty new data: no instructions, empty output', async () => {
  const old = new BufferSource(pattern(2, 500));
  const { patch } = await createDelta(old, Buffer.alloc(0), { blockSize: 64 });
  const info = await inspectPatch(patch);
  assert.deepStrictEqual(info.ops, []);
  assert.strictEqual(info.newLength, 0);
  const { data } = await applyDeltaToBuffer(old, patch);
  assert.strictEqual(data.length, 0);
});

test('identical data collapses to COPY instructions', async () => {
  const content = pattern(3, 10_000);
  const old = new BufferSource(content);
  const { patch } = await createDelta(old, content, { blockSize: 512 });
  const info = await inspectPatch(patch);
  assert.ok(info.ops.length >= 1);
  assert.ok(info.ops.every((op) => op.op === 'copy'));
  assert.ok(patch.length < 200, `patch should be tiny, got ${patch.length}`);
  const { data } = await applyDeltaToBuffer(old, patch);
  assert.ok(data.equals(content));
});

test('insert at an exact block boundary', async () => {
  const oldData = pattern(7, 256); // 4 blocks of 64
  const inserted = pattern(99, 50);
  const fresh = Buffer.concat([oldData.subarray(0, 128), inserted, oldData.subarray(128)]);
  const old = new BufferSource(oldData);
  const { patch, stats } = await createDelta(old, fresh, { blockSize: 64 });
  assert.ok(stats.copyOps >= 1, 'expected COPY ops');
  assert.ok(stats.insertOps >= 1, 'expected INSERT ops');
  const { data } = await applyDeltaToBuffer(old, patch);
  assert.ok(data.equals(fresh));
});

test('duplicate blocks: stable tie-break to the lowest index', async () => {
  const block = pattern(5, 64);
  const oldData = Buffer.concat([block, block, block]);
  const old = new BufferSource(oldData);
  const { patch } = await createDelta(old, block, { blockSize: 64 });
  const info = await inspectPatch(patch);
  assert.deepStrictEqual(info.ops, [{ op: 'copy', index: 0, length: 64 }]);
  const { data } = await applyDeltaToBuffer(old, patch);
  assert.ok(data.equals(block));
});

test('completely different data is a pure literal stream', async () => {
  const oldData = pattern(21, 100_000);
  const fresh = pattern(22, 90_000);
  const old = new BufferSource(oldData);
  const { patch, stats } = await createDelta(old, fresh, { blockSize: 256 });
  assert.strictEqual(stats.copyOps, 0);
  assert.ok(patch.length > fresh.length, 'patch carries all literals');
  assert.ok(patch.length < fresh.length + 1024, 'overhead stays small');
  const { data } = await applyDeltaToBuffer(old, patch);
  assert.ok(data.equals(fresh));
});

test('tail short block is matched and copied', async () => {
  const oldData = pattern(11, 2 * 64 + 17); // two full blocks + 17-byte tail
  const old = new BufferSource(oldData);

  // Whole content: the tail merges into one contiguous COPY run.
  const whole = await createDelta(old, oldData, { blockSize: 64 });
  const wholeInfo = await inspectPatch(whole.patch);
  assert.deepStrictEqual(wholeInfo.ops, [{ op: 'copy', index: 0, length: 145 }]);
  assert.ok((await applyDeltaToBuffer(old, whole.patch)).data.equals(oldData));

  // Tail alone: COPY of the short block with its exact short length.
  const tail = oldData.subarray(128);
  const tailOnly = await createDelta(old, tail, { blockSize: 64 });
  const tailInfo = await inspectPatch(tailOnly.patch);
  assert.deepStrictEqual(tailInfo.ops, [{ op: 'copy', index: 2, length: 17 }]);
  assert.ok((await applyDeltaToBuffer(old, tailOnly.patch)).data.equals(tail));
});

test('new data streamed in small chunks round-trips', async () => {
  const oldData = pattern(31, 8192);
  const fresh = Buffer.concat([
    oldData.subarray(0, 2000),
    pattern(32, 333),
    oldData.subarray(2000, 7000),
    pattern(33, 100),
  ]);
  const old = new BufferSource(oldData);
  const signature = await buildSignature(old, { blockSize: 128 });
  const gen = generateDelta(signature, toStream(fresh, 137)); // odd chunk size
  const parts: Buffer[] = [];
  for (;;) {
    const next = await gen.next();
    if (next.done) break;
    parts.push(next.value);
  }
  const { data } = await applyDeltaToBuffer(old, Buffer.concat(parts));
  assert.ok(data.equals(fresh));
});

test('old data smaller than one block still matches', async () => {
  const oldData = pattern(13, 10); // single 10-byte tail block, blockSize 64
  const old = new BufferSource(oldData);
  const { patch } = await createDelta(old, oldData, { blockSize: 64 });
  const info = await inspectPatch(patch);
  assert.deepStrictEqual(info.ops, [{ op: 'copy', index: 0, length: 10 }]);
  assert.ok((await applyDeltaToBuffer(old, patch)).data.equals(oldData));
});

test('new data shorter than the block size with no matching tail', async () => {
  const old = new BufferSource(pattern(14, 256)); // full blocks only
  const fresh = pattern(15, 20); // 20 < 64, no 20-byte tail block exists
  const { patch, stats } = await createDelta(old, fresh, { blockSize: 64 });
  assert.strictEqual(stats.copyOps, 0);
  assert.ok((await applyDeltaToBuffer(old, patch)).data.equals(fresh));
});

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

test('same inputs produce byte-identical patches', async () => {
  const oldData = pattern(41, 8192);
  const fresh = Buffer.concat([
    oldData.subarray(0, 1000),
    pattern(42, 200),
    oldData.subarray(1000, 5000),
    pattern(43, 64),
    oldData.subarray(5000),
  ]);

  const make = async (): Promise<Buffer> => {
    const old = new BufferSource(oldData);
    const { patch } = await createDelta(old, toStream(fresh, 1024), { blockSize: 128 });
    return patch;
  };

  const first = await make();
  const second = await make();
  assert.ok(first.equals(second), 'patches must be byte-identical');

  // A signature round-trip through its serialized form must not change output.
  const old = new BufferSource(oldData);
  const signature = await buildSignature(old, { blockSize: 128 });
  const parsed = await parseSignature(serializeSignature(signature));
  const viaParsed = await (async () => {
    const parts: Buffer[] = [];
    const gen = generateDelta(parsed, toStream(fresh, 1024));
    for (;;) {
      const next = await gen.next();
      if (next.done) break;
      parts.push(next.value);
    }
    return Buffer.concat(parts);
  })();
  assert.ok(first.equals(viaParsed));
});

// ---------------------------------------------------------------------------
// Collisions
// ---------------------------------------------------------------------------

/** Weak checksum where every window collides into one bucket. */
const constantWeak = () => ({
  reset() {},
  update() {},
  roll() {},
  pop() {},
  value: () => 0x1234,
});

test('weak collisions are resolved by the strong digest', async () => {
  // 8 distinct blocks plus duplicates; every window weak-collides with
  // every block, so only the strong digest can disambiguate.
  const blocks = Array.from({ length: 8 }, (_, i) => pattern(100 + i, 64));
  const oldData = Buffer.concat([...blocks, blocks[2], blocks[5]]);
  const fresh = Buffer.concat([
    blocks[5],
    pattern(200, 30), // novel bytes
    blocks[2],
    blocks[7],
    pattern(201, 70), // novel bytes
  ]);

  const old = new BufferSource(oldData);
  const { patch, stats } = await createDelta(old, fresh, {
    blockSize: 64,
    weakChecksumFactory: constantWeak,
  });
  assert.ok(stats.copyOps >= 1, 'real blocks still matched');
  assert.ok(stats.insertOps >= 1, 'novel bytes still inserted');
  const { data } = await applyDeltaToBuffer(old, patch);
  assert.ok(data.equals(fresh));
});

test('simulated strong-digest collisions are caught by the final digest', async () => {
  // A "broken" strong digest that maps everything to the same value makes
  // the generator emit false COPYs. The applier must reject the result via
  // the trailer digest instead of returning corrupt data.
  const constantStrong = () => Buffer.alloc(16, 0xab);
  const oldData = Buffer.concat([pattern(1, 64), pattern(2, 64), pattern(3, 64)]);
  const fresh = pattern(9, 64); // differs from every old block
  assert.ok(!fresh.equals(oldData.subarray(0, 64)));

  const old = new BufferSource(oldData);
  const { patch } = await createDelta(old, fresh, {
    blockSize: 64,
    weakChecksumFactory: constantWeak,
    strongDigest: constantStrong,
  });
  const info = await inspectPatch(patch);
  assert.deepStrictEqual(info.ops, [{ op: 'copy', index: 0, length: 64 }]);

  await assert.rejects(
    applyDeltaToBuffer(old, patch),
    (err: unknown) => err instanceof DeltaError && err.code === 'DIGEST_MISMATCH',
  );
});

// ---------------------------------------------------------------------------
// Truncation and corruption
// ---------------------------------------------------------------------------

test('truncated patches never produce a result', async () => {
  const oldData = pattern(51, 512);
  const fresh = pattern(52, 600);
  const old = new BufferSource(oldData);
  const { patch } = await createDelta(old, fresh, { blockSize: 64 });

  // Control: the full patch applies cleanly.
  assert.ok((await applyDeltaToBuffer(old, patch)).data.equals(fresh));

  const cuts = new Set([
    0, 1, 2, 5, 20, 39, 40, 41, 60,
    patch.length - 34,
    patch.length - 33,
    patch.length - 20,
    patch.length - 1,
    Math.floor(patch.length / 2),
  ]);
  for (const cut of cuts) {
    if (cut < 0 || cut >= patch.length) continue;
    await assert.rejects(
      applyDeltaToBuffer(old, patch.subarray(0, cut)),
      (err: unknown) => err instanceof DeltaError,
      `cut at ${cut}`,
    );
  }
});

test('corrupt patches are rejected with specific errors', async () => {
  const oldData = pattern(61, 256);
  const fresh = pattern(62, 300);
  const old = new BufferSource(oldData);
  const { patch } = await createDelta(old, fresh, { blockSize: 64 });

  const expectCode = async (mutated: Buffer, code: string): Promise<void> => {
    await assert.rejects(
      applyDeltaToBuffer(old, mutated),
      (err: unknown) => err instanceof DeltaError && err.code === code,
      `expected ${code}`,
    );
  };

  const badMagic = Buffer.from(patch);
  badMagic[0] ^= 0xff;
  await expectCode(badMagic, 'BAD_MAGIC');

  const badVersion = Buffer.from(patch);
  badVersion[4] = 0x7e;
  await expectCode(badVersion, 'BAD_VERSION');

  const trailing = Buffer.concat([patch, Buffer.from([0x00])]);
  await expectCode(trailing, 'TRAILING_DATA');

  // Hand-crafted structural failures.
  const empty = new BufferSource(Buffer.alloc(0));
  const reject = async (crafted: Buffer, code: string): Promise<void> => {
    await assert.rejects(
      applyDeltaToBuffer(empty, crafted),
      (err: unknown) => err instanceof DeltaError && err.code === code,
      `expected ${code}`,
    );
  };

  await reject(craftPatch(patchHeader(0), 0x7f), 'BAD_OPCODE');
  await reject(
    craftPatch(patchHeader(0), OP_COPY, encodeVarint(3), encodeVarint(64)),
    'RANGE',
  );
  await reject(
    craftPatch(
      patchHeader(0),
      OP_INSERT, encodeVarint(3), Buffer.from('abc'),
      OP_END, encodeVarint(4), sha256(Buffer.from('abc')),
    ),
    'LENGTH_MISMATCH',
  );
  await reject(
    craftPatch(
      patchHeader(0),
      OP_INSERT, encodeVarint(3), Buffer.from('abc'),
      OP_END, encodeVarint(3), sha256(Buffer.from('abd')),
    ),
    'DIGEST_MISMATCH',
  );
});

test('old data length mismatch is rejected up front', async () => {
  const old = new BufferSource(pattern(71, 256));
  const { patch } = await createDelta(old, pattern(72, 100), { blockSize: 64 });
  const wrongOld = new BufferSource(pattern(71, 200));
  await assert.rejects(
    applyDeltaToBuffer(wrongOld, patch),
    (err: unknown) => err instanceof DeltaError && err.code === 'OLD_LENGTH_MISMATCH',
  );
});

test('maxOutputLength bounds the applier', async () => {
  const old = new BufferSource(Buffer.alloc(0));
  const fresh = pattern(81, 1000);
  const { patch } = await createDelta(old, fresh, { blockSize: 64 });
  await assert.rejects(
    applyDeltaToBuffer(old, patch, { maxOutputLength: 999 }),
    (err: unknown) => err instanceof DeltaError && err.code === 'LIMIT_EXCEEDED',
  );
  assert.ok(
    (await applyDeltaToBuffer(old, patch, { maxOutputLength: 1000 })).data.equals(fresh),
  );
});

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

test('generation honours a pre-aborted signal', async () => {
  const old = new BufferSource(pattern(91, 4096));
  const signature = await buildSignature(old, { blockSize: 128 });
  const controller = new AbortController();
  controller.abort();
  const gen = generateDelta(signature, toStream(pattern(92, 4096)), {
    signal: controller.signal,
  });
  await assert.rejects(
    gen.next(),
    (err: unknown) => err instanceof DeltaError && err.code === 'CANCELLED',
  );
});

test('generation can be cancelled mid-stream', async () => {
  const old = new BufferSource(pattern(93, 64 * 1024));
  const signature = await buildSignature(old, { blockSize: 512 });
  const controller = new AbortController();
  const gen = generateDelta(signature, toStream(pattern(94, 512 * 1024), 4096), {
    signal: controller.signal,
  });
  const first = await gen.next();
  assert.strictEqual(first.done, false); // header chunk
  controller.abort();
  await assert.rejects(
    gen.next(),
    (err: unknown) => err instanceof DeltaError && err.code === 'CANCELLED',
  );
});

test('application honours a pre-aborted signal', async () => {
  const old = new BufferSource(Buffer.alloc(0));
  const { patch } = await createDelta(old, pattern(95, 1024), { blockSize: 64 });
  const controller = new AbortController();
  controller.abort();
  const gen = applyDelta(old, toAsyncIterable(patch), { signal: controller.signal });
  await assert.rejects(
    gen.next(),
    (err: unknown) => err instanceof DeltaError && err.code === 'CANCELLED',
  );
});

test('application can be cancelled mid-stream', async () => {
  const old = new BufferSource(Buffer.alloc(0));
  const { patch } = await createDelta(old, pattern(96, 200 * 1024), { blockSize: 1024 });
  const controller = new AbortController();
  const gen = applyDelta(old, toAsyncIterable(patch), { signal: controller.signal });
  const first = await gen.next();
  assert.strictEqual(first.done, false);
  controller.abort();
  await assert.rejects(
    gen.next(),
    (err: unknown) => err instanceof DeltaError && err.code === 'CANCELLED',
  );
});
