import { createFileDigest, StrongDigest } from './digest';
import { checkAborted, DeltaError } from './errors';
import {
  FILE_DIGEST_SIZE,
  FORMAT_VERSION,
  OP_COPY,
  OP_END,
  OP_INSERT,
  PATCH_MAGIC,
} from './format';
import { Signature } from './signature';
import { ChunkWriter } from './varint';

export interface GenerateOptions {
  /** Abort to cancel generation; the iterator throws a CANCELLED error. */
  signal?: AbortSignal;
  /** Max literal bytes buffered before an INSERT is emitted (default 64 KiB). */
  insertFlushThreshold?: number;
  /** Patch output chunk size (default 64 KiB). */
  outputChunkSize?: number;
}

export interface GenerateStats {
  outputBytes: number;
  copiedBytes: number;
  literalBytes: number;
  copyOps: number;
  insertOps: number;
}

const DEFAULT_FLUSH = 64 * 1024;

/**
 * Generate a binary patch from `signature` (of the old data) and a stream of
 * new data. Yields patch chunks; the returned stats resolve when the stream
 * is fully consumed.
 *
 * Memory is bounded by O(blockSize + insertFlushThreshold + outputChunkSize)
 * plus the signature table, independent of the new data size.
 *
 * The patch is byte-for-byte deterministic for identical inputs and options.
 */
export async function* generateDelta(
  signature: Signature,
  newData: AsyncIterable<Buffer | Uint8Array>,
  options: GenerateOptions = {},
): AsyncGenerator<Buffer, GenerateStats, void> {
  const { signal } = options;
  const flushThreshold = options.insertFlushThreshold ?? DEFAULT_FLUSH;
  const out = new ChunkWriter(options.outputChunkSize ?? DEFAULT_FLUSH);
  const { blockSize, strongSize } = signature.header;
  const strongDigest: StrongDigest = signature.strongDigest;

  checkAborted(signal);
  if (strongDigest(Buffer.alloc(0)).length !== strongSize) {
    throw new DeltaError(
      'strong digest size does not match the signature',
      'INVALID_OPTION',
    );
  }

  // ---- header -----------------------------------------------------------
  out.bytes(PATCH_MAGIC);
  out.byte(FORMAT_VERSION);
  out.varint(blockSize);
  out.varint(signature.header.oldLength);
  out.bytes(signature.header.oldDigest);
  const headerChunk = out.flush();
  if (headerChunk) yield headerChunk;

  // ---- streaming state ----------------------------------------------------
  const stats: GenerateStats = {
    outputBytes: 0,
    copiedBytes: 0,
    literalBytes: 0,
    copyOps: 0,
    insertOps: 0,
  };
  const newHash = createFileDigest();
  const it = newData[Symbol.asyncIterator]();
  let chunk: Buffer | null = null;
  let chunkPos = 0;
  let eof = false;

  // Circular sliding window of up to blockSize bytes.
  const win = Buffer.allocUnsafe(blockSize);
  let wStart = 0;
  let wLen = 0;
  let weakValid = false;
  const weak = signature.weakChecksumFactory();

  // Literal run accumulation (bounded by flushThreshold).
  const lit = Buffer.allocUnsafe(flushThreshold);
  let litLen = 0;

  // Pending COPY that may still be extended by the next contiguous block.
  let pendingCopy: { index: number; offset: number; length: number } | null = null;

  const emitPendingCopy = (): void => {
    if (!pendingCopy) return;
    out.byte(OP_COPY);
    out.varint(pendingCopy.index);
    out.varint(pendingCopy.length);
    stats.copyOps += 1;
    pendingCopy = null;
  };

  const flushLiteral = (): void => {
    if (litLen === 0) return;
    const bytes = lit.subarray(0, litLen);
    newHash.update(bytes);
    out.byte(OP_INSERT);
    out.varint(litLen);
    out.bytes(Buffer.from(bytes));
    stats.insertOps += 1;
    stats.literalBytes += litLen;
    litLen = 0;
  };

  const appendLiteral = (b: number): void => {
    emitPendingCopy(); // a literal breaks any contiguous COPY run
    if (litLen === lit.length) flushLiteral();
    lit[litLen++] = b;
  };

  const pullChunk = async (): Promise<boolean> => {
    while (chunk === null || chunkPos >= chunk.length) {
      if (eof) return false;
      const next = await it.next();
      if (next.done) {
        eof = true;
        return false;
      }
      const value = next.value;
      chunk = toBuffer(value);
      chunkPos = 0;
      if (chunk.length === 0) {
        chunk = null;
        continue;
      }
    }
    return true;
  };

  const fillWindow = async (): Promise<void> => {
    while (wLen < blockSize && (await pullChunk())) {
      checkAborted(signal);
      const take = Math.min(blockSize - wLen, chunk!.length - chunkPos);
      const writePos = (wStart + wLen) % blockSize;
      const first = Math.min(take, blockSize - writePos);
      chunk!.copy(win, writePos, chunkPos, chunkPos + first);
      if (take > first) chunk!.copy(win, 0, chunkPos + first, chunkPos + take);
      chunkPos += take;
      wLen += take;
    }
  };

  const windowSegments = (): Buffer[] => {
    const end = wStart + wLen;
    if (end <= blockSize) return [win.subarray(wStart, end)];
    return [win.subarray(wStart, blockSize), win.subarray(0, end - blockSize)];
  };

  // ---- main sliding-window loop -------------------------------------------
  await fillWindow();
  while (wLen > 0) {
    checkAborted(signal);

    if (!weakValid) {
      weak.reset();
      for (const seg of windowSegments()) weak.update(seg);
      weakValid = true;
    }

    let matched;
    if (signature.hasWeak(weak.value())) {
      const windowBuf =
        wStart + wLen <= blockSize
          ? win.subarray(wStart, wStart + wLen)
          : Buffer.concat(windowSegments(), wLen);
      const strong = toBuffer(strongDigest(windowBuf));
      matched = signature.find(weak.value(), wLen, strong);
    }

    if (matched) {
      flushLiteral();
      if (pendingCopy && pendingCopy.offset + pendingCopy.length === matched.offset) {
        pendingCopy.length += matched.length;
      } else {
        emitPendingCopy();
        pendingCopy = {
          index: matched.index,
          offset: matched.offset,
          length: matched.length,
        };
      }
      for (const seg of windowSegments()) newHash.update(seg);
      stats.outputBytes += wLen;
      stats.copiedBytes += wLen;
      wStart = 0;
      wLen = 0;
      weakValid = false;
      await fillWindow();
    } else {
      const outByte = win[wStart];
      appendLiteral(outByte);
      stats.outputBytes += 1;
      if (!eof) {
        // Slide by one: pull a fresh byte into the window.
        if (await pullChunk()) {
          const inByte = chunk![chunkPos++];
          win[(wStart + wLen) % blockSize] = inByte;
          weak.roll(outByte, inByte);
          wStart = (wStart + 1) % blockSize;
        } else {
          weak.pop(outByte);
          wStart = (wStart + 1) % blockSize;
          wLen -= 1;
        }
      } else {
        weak.pop(outByte);
        wStart = (wStart + 1) % blockSize;
        wLen -= 1;
      }
    }

    const full = out.flushIfFull();
    if (full) yield full;
  }

  // ---- trailer -------------------------------------------------------------
  flushLiteral();
  emitPendingCopy();
  out.byte(OP_END);
  out.varint(stats.outputBytes);
  const digest = newHash.digest();
  if (digest.length !== FILE_DIGEST_SIZE) {
    throw new DeltaError('internal digest size error', 'SOURCE');
  }
  out.bytes(digest);
  const tail = out.flush();
  if (tail) yield tail;
  return stats;
}

function toBuffer(value: Buffer | Uint8Array): Buffer {
  if (Buffer.isBuffer(value)) return value;
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}
