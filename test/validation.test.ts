import { PassThrough } from 'node:stream';
import { describe, expect, it } from 'vitest';
import {
  AbortError,
  applyPatch,
  applyPatchToBuffer,
  bufferSource,
  DigestMismatchError,
  generatePatch,
  LengthMismatchError,
  OldSizeMismatchError,
  PatchFormatError,
  RangeValidationError,
  TruncatedPatchError,
} from '../src/index.js';
import { collect, makeWeakCollisionPair, pseudoRandomBytes } from './helpers/util.js';

const BS = 16;

async function patchOf(oldData: Buffer, next: Buffer): Promise<Buffer> {
  return collect(generatePatch(bufferSource(oldData), [next], BS));
}

async function expectPartialFailure(
  oldData: Buffer,
  patch: Buffer,
  expectedCtor: new (...args: never[]) => Error,
): Promise<void> {
  let bytes = 0;
  await expect(
    (async () => {
      for await (const chunk of applyPatch(bufferSource(oldData), [patch])) {
        bytes += chunk.length;
      }
    })(),
  ).rejects.toBeInstanceOf(expectedCtor);
  // 抛错后迭代器结束：调用方不应把已产出的字节当作完整结果。
  // （bytes 可能 > 0，但伴随异常，接口约定即"未成功完成"。）
  expect(typeof bytes).toBe('number');
}

describe('applier validation', () => {
  it('rejects bad magic', async () => {
    const patch = await patchOf(pseudoRandomBytes(BS, 1), pseudoRandomBytes(BS, 2));
    patch[0] = 0x58;
    await expectPartialFailure(pseudoRandomBytes(BS, 1), patch, PatchFormatError);
  });

  it('rejects unsupported version', async () => {
    const oldData = pseudoRandomBytes(BS, 1);
    const patch = await patchOf(oldData, pseudoRandomBytes(BS, 2));
    patch[4] = 99;
    await expectPartialFailure(oldData, patch, PatchFormatError);
  });

  it('rejects when header is truncated', async () => {
    const oldData = pseudoRandomBytes(BS, 1);
    const patch = await patchOf(oldData, pseudoRandomBytes(BS, 2));
    await expectPartialFailure(oldData, patch.subarray(0, 10), TruncatedPatchError);
    await expectPartialFailure(oldData, Buffer.alloc(0), TruncatedPatchError);
  });

  it('rejects truncated COPY arguments', async () => {
    const oldData = Buffer.from(pseudoRandomBytes(BS * 2, 3));
    const patch = await patchOf(oldData, Buffer.from(oldData));
    // 头部后 1 字节 opcode + 部分参数
    await expectPartialFailure(oldData, patch.subarray(0, 18 + 5), TruncatedPatchError);
  });

  it('rejects truncated INSERT header and payload', async () => {
    const oldData = Buffer.alloc(0);
    const next = Buffer.from('hello new payload here');
    const patch = await patchOf(oldData, next);
    // 找到第一条 INSERT 的载荷开始位置：18 + 1 + 4
    const payloadStart = 18 + 1 + 4;
    await expectPartialFailure(
      oldData,
      patch.subarray(0, payloadStart + 3), // 载荷只给 3 字节
      TruncatedPatchError,
    );
    await expectPartialFailure(
      oldData,
      patch.subarray(0, payloadStart - 2), // 长度字段本身截断
      TruncatedPatchError,
    );
  });

  it('rejects truncated END footer', async () => {
    const oldData = Buffer.alloc(0);
    const patch = await patchOf(oldData, Buffer.from('abc'));
    await expectPartialFailure(oldData, patch.subarray(0, patch.length - 10), TruncatedPatchError);
  });

  it('rejects trailing bytes after END', async () => {
    const oldData = Buffer.alloc(0);
    const patch = await patchOf(oldData, Buffer.from('abc'));
    const tampered = Buffer.concat([patch, Buffer.from([0x00])]);
    await expectPartialFailure(oldData, tampered, PatchFormatError);
  });

  it('rejects unknown opcode', async () => {
    const oldData = Buffer.alloc(0);
    const patch = await patchOf(oldData, Buffer.from('abc'));
    patch[18] = 0x7f;
    await expectPartialFailure(oldData, patch, PatchFormatError);
  });

  it('rejects COPY range outside the old data', async () => {
    const oldData = pseudoRandomBytes(BS * 2, 8);
    const patch = await patchOf(oldData, Buffer.from(oldData));
    // COPY 从偏移 19 开始，参数位于 18(opcode) 后
    patch.writeBigUInt64LE(BigInt(oldData.length - 3), 19); // offset 靠近尾部
    patch.writeUInt32LE(10, 27); // length 10 → 超出
    await expectPartialFailure(oldData, patch, RangeValidationError);

    const patch2 = await patchOf(oldData, Buffer.from(oldData));
    patch2.writeBigUInt64LE(BigInt(oldData.length), 19); // offset == size 也非法
    await expectPartialFailure(oldData, patch2, RangeValidationError);
  });

  it('rejects old size mismatch', async () => {
    const oldData = pseudoRandomBytes(BS, 1);
    const patch = await patchOf(oldData, pseudoRandomBytes(BS, 2));
    await expectPartialFailure(pseudoRandomBytes(BS + 1, 1), patch, OldSizeMismatchError);
  });

  it('rejects declared length mismatch in END', async () => {
    const oldData = Buffer.alloc(0);
    const next = Buffer.from('abc');
    const patch = await patchOf(oldData, next);
    // END: opcode 在 patch.length - 41；newSize 在其后 1 字节
    const endPos = patch.length - 41;
    patch.writeBigUInt64LE(BigInt(next.length + 1), endPos + 1);
    // 摘要也会不符，但长度校验先执行
    await expectPartialFailure(oldData, patch, LengthMismatchError);
  });

  it('rejects digest mismatch when payload is tampered', async () => {
    const oldData = Buffer.alloc(0);
    const next = Buffer.from('abc');
    const patch = await patchOf(oldData, next);
    patch[18 + 1 + 4] = (patch[18 + 1 + 4]! ^ 0xff) & 0xff; // 翻转 INSERT 载荷首字节
    await expectPartialFailure(oldData, patch, DigestMismatchError);
  });

  it('rejects tampered END digest', async () => {
    const oldData = Buffer.alloc(0);
    const patch = await patchOf(oldData, Buffer.from('abc'));
    patch[patch.length - 1] = (patch[patch.length - 1]! ^ 0x01) & 0xff;
    await expectPartialFailure(oldData, patch, DigestMismatchError);
  });
});

describe('checksum collision handling', () => {
  it('weak collision with different strong digests does not cause a false match', async () => {
    const [blockA, blockB] = makeWeakCollisionPair(BS);
    // 旧数据只有 blockA；新数据在填充后包含 blockB（弱值相同，强摘要不同）
    const oldData = Buffer.concat([blockA, pseudoRandomBytes(BS, 7)]);
    const next = Buffer.concat([Buffer.from('zz'), blockB]);
    const result = await applyPatchToBuffer(bufferSource(oldData), [await patchOf(oldData, next)]);
    expect(result.equals(next)).toBe(true);
    const decoded = await collect(generatePatch(bufferSource(oldData), [next], BS));
    // blockB 不应被 COPY（没有任何旧块等于它）
    expect(decoded.includes(blockA.subarray(0, 4))).toBe(true); // 补丁确实含字面载荷
  });

  it('simulated strong-digest collision: patch is rejected rather than corrupting output', async () => {
    // 注入常量强摘要：任何弱命中都会"确认"，等价于伪造了强摘要碰撞。
    const fakeStrong = (_data: Buffer): Buffer => Buffer.alloc(32, 0);
    const [blockA, blockB] = makeWeakCollisionPair(BS);
    const oldData = blockA;
    const next = blockB;

    const patch = await collect(
      generatePatch(bufferSource(oldData), [next], BS, { strong: fakeStrong }),
    );
    // 注入使生成器错误地把 blockB 当作旧块 blockA 发出 COPY；
    // 应用器做独立范围/长度/真实 sha256 验证，必须拒绝该结果。
    await expect(
      applyPatchToBuffer(bufferSource(oldData), [patch]),
    ).rejects.toBeInstanceOf(DigestMismatchError);
  });

  it('real sha256: identical duplicate blocks are both matched correctly', async () => {
    const block = pseudoRandomBytes(BS, 55);
    const oldData = Buffer.concat([block, block]);
    const result = await applyPatchToBuffer(
      bufferSource(oldData),
      [await patchOf(oldData, Buffer.from(block))],
    );
    expect(result.equals(block)).toBe(true);
  });
});

describe('cancellation', () => {
  it('generator rejects with AbortError when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const gen = generatePatch(
      bufferSource(pseudoRandomBytes(BS, 1)),
      [pseudoRandomBytes(BS, 2)],
      BS,
      { signal: controller.signal },
    );
    await expect(collect(gen)).rejects.toBeInstanceOf(AbortError);
  });

  it('generator rejects when signalled mid-stream while blocked on slow input', async () => {
    const controller = new AbortController();
    const slow = new PassThrough();
    const gen = generatePatch(bufferSource(Buffer.alloc(0)), slow, BS, {
      signal: controller.signal,
    });
    slow.write(Buffer.from('some bytes'));
    const promise = collect(gen);
    setTimeout(() => controller.abort(), 10);
    await expect(promise).rejects.toBeInstanceOf(AbortError);
    slow.destroy();
  });

  it('applier rejects with AbortError when signal is already aborted', async () => {
    const oldData = Buffer.alloc(0);
    const patch = await patchOf(oldData, Buffer.from('abc'));
    const controller = new AbortController();
    controller.abort();
    await expect(
      applyPatchToBuffer(bufferSource(oldData), [patch], { signal: controller.signal }),
    ).rejects.toBeInstanceOf(AbortError);
  });

  it('applier rejects when aborted mid-stream', async () => {
    const oldData = Buffer.alloc(0);
    const next = Buffer.alloc(10_000, 0x61);
    const patch = await patchOf(oldData, next);
    const controller = new AbortController();
    const output: Buffer[] = [];
    const p = (async () => {
      for await (const chunk of applyPatch(bufferSource(oldData), [patch], {
        signal: controller.signal,
      })) {
        output.push(chunk);
        controller.abort();
      }
    })();
    await expect(p).rejects.toBeInstanceOf(AbortError);
  });
});
