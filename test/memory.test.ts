import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { promises as fsp } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { test } from 'node:test';
import {
  FileSource,
  applyDelta,
  buildSignature,
  generateDelta,
} from '../src/index';
import { pattern } from './helpers';

const FILE_SIZE = 48 * 1024 * 1024; // 48 MiB
const CHUNK = 1 << 20; // 1 MiB
const HEAP_BUDGET = 32 * 1024 * 1024; // must stay far below the file size

function heapUsed(): number {
  global.gc!();
  return process.memoryUsage().heapUsed;
}

test('large file: peak memory does not scale with file size', async (t) => {
  if (typeof global.gc !== 'function') {
    t.skip('requires node --expose-gc');
    return;
  }

  const dir = await fsp.mkdtemp(join(tmpdir(), 'bindelta-'));
  const oldPath = join(dir, 'old.bin');
  const patchPath = join(dir, 'patch.bin');

  try {
    // Write a 48 MiB deterministic old file, 1 MiB at a time.
    const ws = createWriteStream(oldPath);
    for (let i = 0; i < FILE_SIZE / CHUNK; i++) {
      if (!ws.write(pattern(i + 1, CHUNK))) await once(ws, 'drain');
    }
    ws.end();
    await once(ws, 'finish');

    // New data: the old file streamed from disk, with the first 100 bytes
    // of every third 1 MiB chunk replaced — mostly COPY, some INSERT.
    const newDataChunks = async function* (): AsyncGenerator<Buffer, void, void> {
      let index = 0;
      for await (const chunk of createReadStream(oldPath, { highWaterMark: CHUNK })) {
        const copy = Buffer.from(chunk);
        if (index % 3 === 0) copy.fill(0xab, 0, 100);
        index += 1;
        yield copy;
      }
    };

    // Expected digest of the new data, computed by streaming.
    const expected = createHash('sha256');
    let expectedLength = 0;
    for await (const chunk of newDataChunks()) {
      expected.update(chunk);
      expectedLength += chunk.length;
    }
    const expectedDigest = expected.digest();

    const old = await FileSource.open(oldPath);
    try {
      // ---- generate: signature + delta, patch streamed to disk ----
      const beforeGenerate = heapUsed();
      const signature = await buildSignature(old);
      const patchStream = createWriteStream(patchPath);
      const gen = generateDelta(signature, newDataChunks());
      let stats;
      for (;;) {
        const next = await gen.next();
        if (next.done) {
          stats = next.value;
          break;
        }
        if (!patchStream.write(next.value)) await once(patchStream, 'drain');
      }
      patchStream.end();
      await once(patchStream, 'finish');
      const generateGrowth = heapUsed() - beforeGenerate;

      assert.ok(stats!.outputBytes === expectedLength);
      assert.ok(
        generateGrowth < HEAP_BUDGET,
        `generation heap grew by ${generateGrowth} bytes for a ${FILE_SIZE}-byte file`,
      );
      const patchSize = (await fsp.stat(patchPath)).size;
      assert.ok(
        patchSize < FILE_SIZE / 8,
        `mostly-COPY patch should be small, got ${patchSize}`,
      );

      // ---- apply: patch streamed from disk, output streamed to a sink ----
      const beforeApply = heapUsed();
      const actual = createHash('sha256');
      let actualLength = 0;
      const out = applyDelta(
        old,
        createReadStream(patchPath, { highWaterMark: CHUNK }) as AsyncIterable<Buffer>,
      );
      for (;;) {
        const next = await out.next();
        if (next.done) {
          assert.strictEqual(next.value.outputBytes, expectedLength);
          break;
        }
        actual.update(next.value);
        actualLength += next.value.length;
      }
      const applyGrowth = heapUsed() - beforeApply;

      assert.strictEqual(actualLength, expectedLength);
      assert.ok(actual.digest().equals(expectedDigest), 'reconstructed data matches');
      assert.ok(
        applyGrowth < HEAP_BUDGET,
        `application heap grew by ${applyGrowth} bytes for a ${FILE_SIZE}-byte file`,
      );
    } finally {
      await old.close();
    }
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
});
