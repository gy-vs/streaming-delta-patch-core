import { createFileDigest } from './digest';
import { checkAborted, DeltaError } from './errors';
import {
  FILE_DIGEST_SIZE,
  FORMAT_VERSION,
  OP_COPY,
  OP_END,
  OP_INSERT,
  PATCH_MAGIC,
} from './format';
import { OldDataSource } from './source';
import { StreamReader } from './varint';

export interface ApplyOptions {
  /** Abort to cancel; the iterator throws a CANCELLED error. */
  signal?: AbortSignal;
  /** Optional upper bound on total output bytes; exceeded → error. */
  maxOutputLength?: number;
  /** Slice size for COPY reads from the old data (default 1 MiB). */
  copySliceSize?: number;
}

export interface ApplyStats {
  outputBytes: number;
  copiedBytes: number;
  literalBytes: number;
  copyOps: number;
  insertOps: number;
}

const COPY_SLICE = 1 << 20;
const INSERT_SLICE = 64 * 1024;

/**
 * Apply a patch stream to the old data, yielding the reconstructed new data.
 *
 * Validation performed:
 *  - patch magic/version and declared old length vs. the actual source
 *  - every COPY range must lie inside the old data
 *  - total output must equal the length declared in the patch trailer
 *  - the SHA-256 of the output must equal the trailer digest
 *  - no trailing bytes after the trailer
 *
 * If the iterator throws (validation failure, truncation, cancellation),
 * any bytes already yielded are incomplete and MUST be discarded — a
 * complete result exists only when iteration finishes without error.
 */
export async function* applyDelta(
  old: OldDataSource,
  patch: AsyncIterable<Buffer>,
  options: ApplyOptions = {},
): AsyncGenerator<Buffer, ApplyStats, void> {
  const { signal, maxOutputLength } = options;
  const copySlice = options.copySliceSize ?? COPY_SLICE;
  checkAborted(signal);

  const reader = new StreamReader(patch);

  // ---- header -------------------------------------------------------------
  const magic = await reader.read(PATCH_MAGIC.length);
  if (!magic.equals(PATCH_MAGIC)) {
    throw new DeltaError('bad patch magic', 'BAD_MAGIC');
  }
  const version = await reader.readByte();
  if (version !== FORMAT_VERSION) {
    throw new DeltaError(`unsupported patch version ${version}`, 'BAD_VERSION');
  }
  const blockSize = await reader.readVarint();
  const oldLength = await reader.readVarint();
  if (blockSize < 1) {
    throw new DeltaError('invalid block size in patch header', 'BAD_HEADER');
  }
  if (oldLength !== old.length) {
    throw new DeltaError(
      `old data length mismatch: patch expects ${oldLength}, source has ${old.length}`,
      'OLD_LENGTH_MISMATCH',
    );
  }
  await reader.read(FILE_DIGEST_SIZE); // old digest (informational)

  // ---- instructions ---------------------------------------------------------
  const stats: ApplyStats = {
    outputBytes: 0,
    copiedBytes: 0,
    literalBytes: 0,
    copyOps: 0,
    insertOps: 0,
  };
  const outHash = createFileDigest();

  const guardLength = (additional: number): void => {
    if (
      maxOutputLength !== undefined &&
      stats.outputBytes + additional > maxOutputLength
    ) {
      throw new DeltaError(
        `output exceeds the allowed maximum of ${maxOutputLength} bytes`,
        'LIMIT_EXCEEDED',
      );
    }
  };

  for (;;) {
    checkAborted(signal);
    const op = await reader.readByte(); // TRUNCATED when the stream ends early
    if (op === OP_END) break;

    if (op === OP_COPY) {
      const index = await reader.readVarint();
      const length = await reader.readVarint();
      const offset = index * blockSize;
      if (
        length < 1 ||
        !Number.isSafeInteger(offset) ||
        offset + length > oldLength
      ) {
        throw new DeltaError(
          `COPY out of range: index=${index} length=${length} oldLength=${oldLength}`,
          'RANGE',
        );
      }
      guardLength(length);
      stats.copyOps += 1;
      stats.copiedBytes += length;
      let remaining = length;
      let pos = offset;
      while (remaining > 0) {
        checkAborted(signal);
        const take = Math.min(copySlice, remaining);
        const data = await old.readAt(pos, take);
        if (data.length !== take) {
          throw new DeltaError('short read from old data source', 'SOURCE');
        }
        outHash.update(data);
        stats.outputBytes += take;
        yield data;
        pos += take;
        remaining -= take;
      }
    } else if (op === OP_INSERT) {
      const length = await reader.readVarint();
      guardLength(length);
      stats.insertOps += 1;
      stats.literalBytes += length;
      let remaining = length;
      while (remaining > 0) {
        checkAborted(signal);
        const take = Math.min(INSERT_SLICE, remaining);
        const data = await reader.read(take); // TRUNCATED when cut short
        outHash.update(data);
        stats.outputBytes += take;
        yield data;
        remaining -= take;
      }
    } else {
      throw new DeltaError(`unknown opcode 0x${op.toString(16)}`, 'BAD_OPCODE');
    }
  }

  // ---- trailer --------------------------------------------------------------
  const declaredLength = await reader.readVarint();
  const declaredDigest = await reader.read(FILE_DIGEST_SIZE);
  if (stats.outputBytes !== declaredLength) {
    throw new DeltaError(
      `output length mismatch: produced ${stats.outputBytes}, declared ${declaredLength}`,
      'LENGTH_MISMATCH',
    );
  }
  const actualDigest = outHash.digest();
  if (!actualDigest.equals(declaredDigest)) {
    throw new DeltaError('output digest mismatch', 'DIGEST_MISMATCH');
  }
  if (!(await reader.finished())) {
    throw new DeltaError('trailing data after patch trailer', 'TRAILING_DATA');
  }
  return stats;
}
