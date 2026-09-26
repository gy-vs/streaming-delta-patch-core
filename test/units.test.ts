import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  BufferSource,
  StreamReader,
  buildSignature,
  chooseBlockSize,
  createRsyncChecksum,
  encodeVarint,
  parseSignature,
  serializeSignature,
} from '../src/index';
import { pattern } from './helpers';

async function* single(buf: Buffer): AsyncIterable<Buffer> {
  yield buf;
}

test('varint round-trips across the safe-integer range', async () => {
  const values = [0, 1, 2, 127, 128, 255, 300, 16383, 16384, 2 ** 31 - 1, 2 ** 32, 2 ** 53 - 1];
  for (const value of values) {
    const reader = new StreamReader(single(encodeVarint(value)));
    assert.strictEqual(await reader.readVarint(), value, `value ${value}`);
    assert.strictEqual(await reader.finished(), true);
  }
});

test('varint rejects overlong encodings', async () => {
  const reader = new StreamReader(single(Buffer.alloc(16, 0xff)));
  await assert.rejects(reader.readVarint(), /varint too long/);
});

test('rolling checksum matches fresh computation after roll and pop', () => {
  const data = [...pattern(1, 64)];
  const fresh = (bytes: number[]): number => {
    const c = createRsyncChecksum();
    c.reset();
    c.update(Buffer.from(bytes));
    return c.value();
  };

  const window = data.slice(0, 8);
  const rolling = createRsyncChecksum();
  rolling.reset();
  rolling.update(Buffer.from(window));
  assert.strictEqual(rolling.value(), fresh(window));

  // Slide the window one byte at a time.
  for (let i = 8; i < 40; i++) {
    const outByte = window.shift()!;
    const inByte = data[i];
    window.push(inByte);
    rolling.roll(outByte, inByte);
    assert.strictEqual(rolling.value(), fresh(window), `roll at ${i}`);
  }

  // Shrink the window to nothing (EOF draining).
  while (window.length > 0) {
    const outByte = window.shift()!;
    rolling.pop(outByte);
    assert.strictEqual(rolling.value(), fresh(window), `pop to ${window.length}`);
  }
});

test('chooseBlockSize keeps the block count bounded', () => {
  assert.strictEqual(chooseBlockSize(0), 2048);
  assert.strictEqual(chooseBlockSize(1), 2048);
  assert.strictEqual(chooseBlockSize(8192 * 2048), 2048);
  assert.strictEqual(chooseBlockSize(8192 * 2048 + 1), 2049);
  for (const size of [1e6, 1e8, 1e10, 1e12]) {
    const bs = chooseBlockSize(size);
    assert.ok(Math.ceil(size / bs) <= 8192, `size ${size}`);
  }
});

test('signature serialization round-trips byte-for-byte', async () => {
  const block = pattern(5, 64);
  const old = new BufferSource(Buffer.concat([block, block, pattern(6, 64), block]));
  const signature = await buildSignature(old, { blockSize: 64 });

  const serialized = serializeSignature(signature);
  const parsed = await parseSignature(serialized);
  const reserialized = serializeSignature(parsed);
  assert.ok(serialized.equals(reserialized));

  assert.strictEqual(parsed.blockCount, 4);
  assert.strictEqual(parsed.header.oldLength, 256);
  assert.ok(parsed.header.oldDigest.equals(signature.header.oldDigest));
});

test('duplicate blocks resolve to the lowest index (stable tie-break)', async () => {
  const block = pattern(5, 64);
  const old = new BufferSource(Buffer.concat([block, block, pattern(6, 64), block]));
  const signature = await buildSignature(old, { blockSize: 64 });

  const weak = createRsyncChecksum();
  weak.reset();
  weak.update(block);
  const strong = signature.strongDigest(block);

  const hit = signature.find(weak.value(), 64, strong);
  assert.ok(hit);
  assert.strictEqual(hit.index, 0);

  // Unknown strong digest under a known weak sum must not match.
  const miss = signature.find(weak.value(), 64, Buffer.alloc(16, 0xee));
  assert.strictEqual(miss, undefined);

  // Length mismatch (e.g. tail-sized window vs full block) must not match.
  const wrongLength = signature.find(weak.value(), 32, strong);
  assert.strictEqual(wrongLength, undefined);
});
