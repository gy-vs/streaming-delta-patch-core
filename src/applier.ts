import { createHash } from 'node:crypto';
import type { Writable } from 'node:stream';
import {
  AbortError,
  DigestMismatchError,
  LengthMismatchError,
  OldSizeMismatchError,
  PatchFormatError,
  RangeValidationError,
  TruncatedPatchError,
  throwIfAborted,
} from './errors.js';
import {
  FOOTER_SIZE,
  HEADER_SIZE,
  MAX_INSERT_LENGTH,
  OP_COPY,
  OP_END,
  OP_INSERT,
  parseHeader,
} from './format.js';
import type { RandomReadSource } from './sources.js';

/** 从旧数据源 COPY 时单次读取的上限，保证应用侧读取缓冲有界。 */
export const COPY_READ_CHUNK = 64 * 1024;

/**
 * 字节流阅读器：从补丁块序列中顺序取出定长数据。
 *
 * 只保留"尚未满足读取需求"的尾部，且至多为当前读取长度
 * （指令头最大 13 字节、END 41 字节），INSERT 载荷用 {@link drain}
 * 边到边转发而不落进阅读器，因此阅读器内存与文件大小无关。
 */
class ByteReader {
  #iterator: AsyncIterator<Buffer>;
  #leftover: Buffer = Buffer.alloc(0);
  #ended = false;

  constructor(iterable: AsyncIterable<Buffer> | Iterable<Buffer>) {
    const stream = iterable as {
      [Symbol.asyncIterator]?: () => AsyncIterator<Buffer>;
      [Symbol.iterator]?: () => Iterator<Buffer>;
    };
    this.#iterator =
      typeof stream[Symbol.asyncIterator] === 'function'
        ? stream[Symbol.asyncIterator]!()
        : (() => {
            const sync = stream[Symbol.iterator]!();
            return {
              next: () => Promise.resolve(sync.next()),
            } as AsyncIterator<Buffer>;
          })();
  }

  #pull(signal?: AbortSignal): Promise<IteratorResult<Buffer>> {
    const p = Promise.resolve(this.#iterator.next());
    if (!signal) return p;
    if (signal.aborted) return Promise.reject(new AbortError());
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        signal.removeEventListener('abort', onAbort);
        reject(new AbortError());
      };
      signal.addEventListener('abort', onAbort, { once: true });
      p.then(
        (v) => {
          signal.removeEventListener('abort', onAbort);
          resolve(v);
        },
        (e) => {
          signal.removeEventListener('abort', onAbort);
          reject(e);
        },
      );
    });
  }

  /** 精确读取 length 字节；流提前结束时抛 {@link TruncatedPatchError}。 */
  async readExact(length: number, what: string, signal?: AbortSignal): Promise<Buffer> {
    if (this.#leftover.length >= length) {
      const out = this.#leftover.subarray(0, length);
      this.#leftover = this.#leftover.subarray(length);
      return Buffer.from(out); // 复制，避免与残留块共享底层内存
    }
    const parts: Buffer[] = [];
    if (this.#leftover.length > 0) {
      parts.push(this.#leftover);
      this.#leftover = Buffer.alloc(0);
    }
    let have = parts.reduce((n, p) => n + p.length, 0);
    while (have < length) {
      throwIfAborted(signal);
      const next = await this.#pull(signal);
      if (next.done) throw new TruncatedPatchError(what);
      const chunk = next.value;
      if (!Buffer.isBuffer(chunk)) {
        throw new PatchFormatError('patch input must be an iterable of Buffer');
      }
      const need = length - have;
      if (chunk.length <= need) {
        parts.push(chunk);
        have += chunk.length;
      } else {
        parts.push(chunk.subarray(0, need));
        this.#leftover = Buffer.from(chunk.subarray(need));
        have = length;
      }
    }
    return Buffer.concat(parts, length);
  }

  /**
   * 把接下来 length 个载荷字节逐个产出（不缓冲整条 INSERT）。
   * 结束后保证恰好消费 length 字节；不足则抛截断错误。
   */
  async *drain(
    length: number,
    what: string,
    signal?: AbortSignal,
  ): AsyncGenerator<Buffer, void, void> {
    let remaining = length;
    if (this.#leftover.length > 0) {
      const take = Math.min(this.#leftover.length, remaining);
      yield Buffer.from(this.#leftover.subarray(0, take));
      this.#leftover = this.#leftover.subarray(take);
      remaining -= take;
    }
    while (remaining > 0) {
      throwIfAborted(signal);
      const next = await this.#pull(signal);
      if (next.done) throw new TruncatedPatchError(what);
      const chunk = next.value;
      if (!Buffer.isBuffer(chunk)) {
        throw new PatchFormatError('patch input must be an iterable of Buffer');
      }
      if (chunk.length <= remaining) {
        yield chunk;
        remaining -= chunk.length;
      } else {
        yield chunk.subarray(0, remaining);
        this.#leftover = Buffer.from(chunk.subarray(remaining));
        remaining = 0;
      }
    }
  }

  /** END 之后不得再有任何字节。 */
  async assertFullyConsumed(signal?: AbortSignal): Promise<void> {
    if (this.#leftover.length > 0) {
      throw new PatchFormatError('unexpected trailing bytes after END opcode');
    }
    if (this.#ended) return;
    const next = await this.#pull(signal);
    if (!next.done) {
      throw new PatchFormatError('unexpected trailing bytes after END opcode');
    }
    this.#ended = true;
  }
}

/** 分块从旧数据源读取 COPY 区间，单次至多持有 COPY_READ_CHUNK 字节。 */
async function* copyFromOld(
  source: RandomReadSource,
  offset: number,
  length: number,
  signal?: AbortSignal,
): AsyncGenerator<Buffer, void, void> {
  let pos = 0;
  while (pos < length) {
    throwIfAborted(signal);
    const n = Math.min(COPY_READ_CHUNK, length - pos);
    yield source.read(offset + pos, n, signal);
    pos += n;
  }
}

export interface ApplyOptions {
  signal?: AbortSignal;
}

/**
 * 应用 SDP1 补丁，流式产出重建后的新数据字节。
 *
 * 只有当迭代正常结束（而非抛错）时，结果才完整可信；应用器依次验证：
 *  1. 头部魔数/版本，以及头部声明的旧数据大小 == 实际旧数据源大小；
 *  2. 每条 COPY 的 [offset, offset+length) 完全落在旧数据范围内；
 *  3. 输出总长度不超过 END 声明长度，且最终严格相等；
 *  4. 全部输出字节的 sha256 == END 声明摘要；
 *  5. END 之后无尾随字节，指令流无截断、无未知操作码。
 *
 * 任何一项失败都会抛出 {@link DeltaError} 子类，且不会"宣称"得到完整结果。
 */
export async function* applyPatch(
  oldSource: RandomReadSource,
  patch: AsyncIterable<Buffer> | Iterable<Buffer>,
  options: ApplyOptions = {},
): AsyncGenerator<Buffer, void, void> {
  const reader = new ByteReader(patch);
  const signal = options.signal;

  const headerBuf = await reader.readExact(HEADER_SIZE, 'the patch header', signal);
  const header = parseHeader(headerBuf);
  if (header.oldSize !== BigInt(oldSource.size)) {
    throw new OldSizeMismatchError(
      `patch was built for old size ${header.oldSize} but source has size ${oldSource.size}`,
    );
  }

  const outHash = createHash('sha256');
  let outLength = 0;
  let ended = false;

  // declaredNewSize 在读到 END 前未知；期间只防安全整数溢出。
  let declaredNewSize: bigint | null = null;
  const emitChecked = async function* (data: Buffer | Promise<Buffer>): AsyncGenerator<Buffer> {
    const buf = await data;
    outHash.update(buf);
    const next = outLength + buf.length;
    if (!Number.isSafeInteger(next)) {
      throw new RangeValidationError('reconstructed output length exceeds safe integer range');
    }
    if (declaredNewSize !== null && BigInt(next) > declaredNewSize) {
      throw new LengthMismatchError(
        `output exceeds declared new size (${next} > ${declaredNewSize})`,
      );
    }
    outLength = next;
    yield buf;
  };

  while (!ended) {
    throwIfAborted(signal);
    const opcodeBuf = await reader.readExact(1, 'an opcode', signal);
    const opcode = opcodeBuf.readUInt8(0);

    if (opcode === OP_COPY) {
      const args = await reader.readExact(12, 'COPY arguments', signal);
      const offset = Number(args.readBigUInt64LE(0));
      const length = args.readUInt32LE(8);
      if (length === 0) throw new PatchFormatError('COPY with zero length');
      if (!Number.isSafeInteger(offset)) {
        throw new RangeValidationError('COPY offset exceeds safe integer range');
      }
      if (offset < 0 || offset >= oldSource.size || offset + length > oldSource.size) {
        throw new RangeValidationError(
          `COPY range out of bounds: offset=${offset} length=${length} oldSize=${oldSource.size}`,
        );
      }
      for await (const chunk of copyFromOld(oldSource, offset, length, signal)) {
        for await (const out of emitChecked(chunk)) yield out;
      }
      continue;
    }

    if (opcode === OP_INSERT) {
      const lenBuf = await reader.readExact(4, 'INSERT length', signal);
      const length = lenBuf.readUInt32LE(0);
      if (length === 0) throw new PatchFormatError('INSERT with zero length');
      if (length > MAX_INSERT_LENGTH) {
        // uint32 本身不可能超过，保留为防御性检查。
        throw new PatchFormatError(`INSERT length too large: ${length}`);
      }
      for await (const chunk of reader.drain(length, `INSERT payload of ${length} bytes`, signal)) {
        for await (const out of emitChecked(chunk)) yield out;
      }
      continue;
    }

    if (opcode === OP_END) {
      const footer = await reader.readExact(FOOTER_SIZE - 1, 'END footer', signal);
      declaredNewSize = footer.readBigUInt64LE(0);
      const expectedDigest = footer.subarray(8, 40);
      if (BigInt(outLength) !== declaredNewSize) {
        throw new LengthMismatchError(
          `declared new size ${declaredNewSize} does not match emitted length ${outLength}`,
        );
      }
      const actual = outHash.digest();
      if (!actual.equals(expectedDigest)) {
        throw new DigestMismatchError(
          'reconstructed data sha256 does not match the digest in END',
        );
      }
      ended = true;
      await reader.assertFullyConsumed(signal);
      continue;
    }

    throw new PatchFormatError(`unknown opcode: 0x${opcode.toString(16).padStart(2, '0')}`);
  }
}

/** 便捷封装：应用补丁并收集为单个 Buffer（仅在全部验证通过后才返回）。 */
export async function applyPatchToBuffer(
  oldSource: RandomReadSource,
  patch: AsyncIterable<Buffer> | Iterable<Buffer>,
  options?: ApplyOptions,
): Promise<Buffer> {
  const parts: Buffer[] = [];
  for await (const chunk of applyPatch(oldSource, patch, options)) {
    parts.push(chunk);
  }
  return Buffer.concat(parts);
}

/** 便捷封装：应用补丁并写入 Writable（验证全部通过后才正常 resolve）。 */
export async function applyPatchToWritable(
  oldSource: RandomReadSource,
  patch: AsyncIterable<Buffer> | Iterable<Buffer>,
  destination: Writable,
  options?: ApplyOptions,
): Promise<void> {
  for await (const chunk of applyPatch(oldSource, patch, options)) {
    if (!destination.write(chunk)) {
      await onceDrain(destination, options?.signal);
    }
  }
  await new Promise<void>((resolve, reject) => destination.end((err?: Error | null) => (err ? reject(err) : resolve())));
}

function onceDrain(stream: Writable, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new AbortError());
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      stream.off('drain', onDrain);
      reject(new AbortError());
    };
    const onDrain = () => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    };
    signal?.addEventListener('abort', onAbort, { once: true });
    stream.once('drain', onDrain);
  });
}
