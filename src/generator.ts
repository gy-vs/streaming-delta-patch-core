import { createHash } from 'node:crypto';
import { strongDigest as defaultStrongDigest } from './checksums.js';
import { AbortError, DeltaError, RangeValidationError, throwIfAborted } from './errors.js';
import { writeCopyOp, writeEndOp, writeHeader, writeInsertOp } from './format.js';
import { SignatureTable } from './signatures.js';
import type { RandomReadSource } from './sources.js';

/** INSERT 指令在内存中聚合的上限；超出即刷出，保证字面缓冲有界。 */
export const INSERT_FLUSH_SIZE = 64 * 1024;
/** 允许的最大块大小，避免单次环形窗口分配失控。 */
export const MAX_BLOCK_SIZE = 16 * 1024 * 1024;
export const DEFAULT_BLOCK_SIZE = 2048;
const ADLER_MOD = 65521;

export interface GenerateOptions {
  signal?: AbortSignal;
  /**
   * 强摘要函数（默认 sha256）。签名表构建与滑窗确认使用同一函数，
   * 因此可注入碰撞摘要来测试碰撞处理路径。END 中的最终摘要始终用真实 sha256。
   */
  strong?: (data: Buffer) => Buffer;
}

/** 把一个 Promise 与中止信号竞速：信号先触发则拒绝为 {@link AbortError}。 */
function raceAbort<T>(p: Promise<T>, signal?: AbortSignal): Promise<T> {
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

/**
 * 根据旧数据签名，把新数据流编码为 SDP1 补丁流（async iterable，拉取驱动、背压友好）。
 *
 * 内存特征（与新文件大小无关，只与块大小相关）：
 *  - 签名表 O(旧文件大小 / blockSize) 条记录；
 *  - 扫描侧常驻：一个 blockSize 的环形窗口 + INSERT_FLUSH_SIZE 的字面聚合缓冲；
 *  - 逐字节滑动滚动校验；字节离开窗口时才作为字面量输出——
 *    此时覆盖该字节的所有可能匹配窗口都已检查完毕，输出决策不可回退。
 *
 * 决胜：弱校验命中后计算强摘要；摘要确认时签名桶按块序号升序，
 * 重复块永远选择旧数据中最靠左的那一个。
 */
export async function* generatePatch(
  oldSource: RandomReadSource,
  newData: AsyncIterable<Buffer> | Iterable<Buffer>,
  blockSize: number = DEFAULT_BLOCK_SIZE,
  options: GenerateOptions = {},
): AsyncGenerator<Buffer, void, void> {
  if (!Number.isInteger(blockSize) || blockSize <= 0) {
    throw new RangeValidationError(`blockSize must be a positive integer, got ${blockSize}`);
  }
  if (blockSize > MAX_BLOCK_SIZE) {
    throw new RangeValidationError(`blockSize ${blockSize} exceeds maximum ${MAX_BLOCK_SIZE}`);
  }
  const strong = options.strong ?? defaultStrongDigest;

  const table = await SignatureTable.build(oldSource, blockSize, {
    signal: options.signal,
    strong,
  });
  // 旧数据至多有一个短块（最后一块），长度唯一。
  const shortSigs =
    oldSource.size > 0 && oldSource.size % blockSize !== 0
      ? table.candidatesByLength(oldSource.size % blockSize)
      : [];
  const shortSig = shortSigs[0];

  const headerChunks: Buffer[] = [];
  writeHeader(headerChunks, blockSize, BigInt(oldSource.size));
  for (const c of headerChunks) yield c;

  // —— 滑动窗口状态（环形缓冲，容量恰为 blockSize）——
  const ring = Buffer.allocUnsafe(blockSize);
  let head = 0; // 窗口首字节在 ring 中的下标
  let winLen = 0; // 当前窗口长度 [0, blockSize]
  let a = 1;
  let b = 0;

  // —— 字面量聚合 ——
  const insertBuf = Buffer.allocUnsafe(INSERT_FLUSH_SIZE);
  let insertLen = 0;
  const takeInsert = (): Buffer[] => {
    if (insertLen === 0) return [];
    const out: Buffer[] = [];
    writeInsertOp(out, insertLen);
    // 复制：insertBuf 之后会被复用，不能把旧视图交给下游。
    out.push(Buffer.from(insertBuf.subarray(0, insertLen)));
    insertLen = 0;
    return out;
  };

  // END 中的最终摘要始终使用真实 sha256，与可能注入的碰撞摘要无关。
  const finalHash = createHash('sha256');
  let newSize = 0;

  /** 窗口内容按逻辑顺序拷贝（仅弱命中时调用，次数很少）。 */
  const windowView = (len: number): Buffer => {
    const view = Buffer.allocUnsafe(len);
    if (head + len <= blockSize) {
      ring.copy(view, 0, head, head + len);
    } else {
      const first = blockSize - head;
      ring.copy(view, 0, head, blockSize);
      ring.copy(view, first, 0, len - first);
    }
    return view;
  };

  const resetWindow = () => {
    head = 0;
    winLen = 0;
    a = 1;
    b = 0;
  };

  /** 已确认字面量 + 一条 COPY（范围不变量在此兜底，正常路径不会触发）。 */
  const emitCopy = (offset: number, length: number): Buffer[] => {
    if (offset < 0 || length <= 0 || !Number.isSafeInteger(offset + length) ||
        offset + length > oldSource.size) {
      throw new RangeValidationError(
        `refusing to emit out-of-range COPY offset=${offset} length=${length}`,
      );
    }
    const out = takeInsert();
    writeCopyOp(out, offset, length);
    return out;
  };

  /** 处理一个新字节，返回需要立即输出的补丁片段。 */
  const processByte = (byte: number): Buffer[] => {
    const v = (byte & 0xff) + 1;

    if (winLen < blockSize) {
      ring[(head + winLen) % blockSize] = byte;
      winLen++;
      a = (a + v) % ADLER_MOD;
      b = (b + a) % ADLER_MOD;

      // 旧数据尾部短块只可能在"填充长度恰好等于其长度"时对齐匹配。
      if (shortSig && winLen === shortSig.length) {
        const weakVal = ((b & 0xffff) << 16) | (a & 0xffff);
        if (weakVal === shortSig.weak) {
          const view = windowView(winLen);
          if (strong(view).equals(shortSig.strong)) {
            resetWindow();
            return emitCopy(shortSig.offset, shortSig.length);
          }
        }
      }

      if (winLen === blockSize) {
        const weakVal = ((b & 0xffff) << 16) | (a & 0xffff);
        if (table.candidates(weakVal)) {
          const view = windowView(blockSize);
          const sig = table.confirm(weakVal, strong(view));
          if (sig) {
            resetWindow();
            return emitCopy(sig.offset, sig.length);
          }
        }
      }
      return [];
    }

    // 窗口已满 → 滚动：最左字节离开窗口，从此不可能再被任何匹配覆盖。
    const outByte = ring[head]!;
    const ov = (outByte & 0xff) + 1;
    ring[head] = byte;
    head = (head + 1) % blockSize;
    a = (((a - ov + v) % ADLER_MOD) + ADLER_MOD) % ADLER_MOD;
    b = (((b - blockSize * ov + a - 1) % ADLER_MOD) + ADLER_MOD) % ADLER_MOD;

    insertBuf[insertLen++] = outByte;
    let flushed: Buffer[] = [];
    if (insertLen === INSERT_FLUSH_SIZE) flushed = takeInsert();

    const weakVal = ((b & 0xffff) << 16) | (a & 0xffff);
    if (table.candidates(weakVal)) {
      const view = windowView(blockSize);
      const sig = table.confirm(weakVal, strong(view));
      if (sig) {
        resetWindow();
        return [...flushed, ...emitCopy(sig.offset, sig.length)];
      }
    }
    return flushed;
  };

  const stream = newData as {
    [Symbol.asyncIterator]?: () => AsyncIterator<Buffer>;
    [Symbol.iterator]?: () => Iterator<Buffer>;
  };
  const isAsync = typeof stream[Symbol.asyncIterator] === 'function';
  const iterator = isAsync
    ? stream[Symbol.asyncIterator]!()
    : stream[Symbol.iterator]!();

  while (true) {
    throwIfAborted(options.signal);
    const next = await raceAbort(Promise.resolve(iterator.next()), options.signal);
    if (next.done) break;
    const chunk = next.value;
    if (!Buffer.isBuffer(chunk)) {
      throw new DeltaError('new data input must be an iterable of Buffer');
    }
    finalHash.update(chunk);
    newSize += chunk.length;
    if (!Number.isSafeInteger(newSize)) {
      throw new RangeValidationError('new data length exceeds safe integer range');
    }
    for (let i = 0; i < chunk.length; i++) {
      for (const out of processByte(chunk[i]!)) yield out;
    }
  }

  // 流结束：窗口残留字节不可能再匹配（所有相关窗口均已检查过），作为字面量。
  if (winLen > 0) {
    const view = windowView(winLen);
    for (const byte of view) {
      insertBuf[insertLen++] = byte;
      if (insertLen === INSERT_FLUSH_SIZE) {
        for (const c of takeInsert()) yield c;
      }
    }
  }
  for (const c of takeInsert()) yield c;

  const footer: Buffer[] = [];
  writeEndOp(footer, BigInt(newSize), finalHash.digest());
  for (const c of footer) yield c;
}
