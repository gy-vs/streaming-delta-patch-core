import { ApplyOptions, ApplyStats, applyDelta } from './apply';
import { GenerateOptions, GenerateStats, generateDelta } from './generate';
import { Signature, SignatureOptions, buildSignature } from './signature';
import { OldDataSource } from './source';

export { ApplyOptions, ApplyStats, applyDelta } from './apply';
export { RollingChecksum, RollingChecksumFactory, createRsyncChecksum } from './checksum';
export { StrongDigest, createFileDigest, sha256Strong } from './digest';
export { DeltaError, DeltaErrorCode } from './errors';
export {
  DEFAULT_MAX_BLOCKS,
  DEFAULT_MIN_BLOCK_SIZE,
  DEFAULT_STRONG_SIZE,
  FILE_DIGEST_SIZE,
  FORMAT_VERSION,
  OP_COPY,
  OP_END,
  OP_INSERT,
  PATCH_MAGIC,
  SIGNATURE_MAGIC,
} from './format';
export { GenerateOptions, GenerateStats, generateDelta } from './generate';
export { PatchInfo, PatchOp, inspectPatch } from './inspect';
export {
  BlockEntry,
  ParseSignatureOptions,
  Signature,
  SignatureHeader,
  SignatureOptions,
  buildSignature,
  chooseBlockSize,
  parseSignature,
  serializeSignature,
} from './signature';
export { BufferSource, FileSource, OldDataSource } from './source';
export { ChunkWriter, StreamReader, encodeVarint } from './varint';

/** Wrap a buffer (or async iterable) as an async iterable of buffers. */
export async function* toAsyncIterable(
  data: Buffer | Uint8Array | AsyncIterable<Buffer | Uint8Array>,
  chunkSize = 64 * 1024,
): AsyncGenerator<Buffer, void, void> {
  if (isAsyncIterable(data)) {
    for await (const chunk of data) {
      yield Buffer.isBuffer(chunk)
        ? chunk
        : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
    }
    return;
  }
  const buf = Buffer.isBuffer(data)
    ? data
    : Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  for (let pos = 0; pos < buf.length; pos += chunkSize) {
    yield buf.subarray(pos, Math.min(pos + chunkSize, buf.length));
  }
}

function isAsyncIterable(
  value: unknown,
): value is AsyncIterable<Buffer | Uint8Array> {
  return (
    typeof value === 'object' &&
    value !== null &&
    Symbol.asyncIterator in (value as object)
  );
}

/** Collect a stream of buffers into one buffer (use for bounded sizes). */
export async function collect(stream: AsyncIterable<Buffer>): Promise<Buffer> {
  const parts: Buffer[] = [];
  for await (const chunk of stream) parts.push(chunk);
  return Buffer.concat(parts);
}

export interface CreateDeltaResult {
  patch: Buffer;
  signature: Signature;
  stats: GenerateStats;
}

/**
 * One-shot helper: build a signature of `old` and produce the complete
 * patch that transforms it into `newData`. Buffers the whole patch, so use
 * the streaming API (`buildSignature` + `generateDelta`) for large outputs.
 */
export async function createDelta(
  old: OldDataSource,
  newData: Buffer | Uint8Array | AsyncIterable<Buffer | Uint8Array>,
  options: SignatureOptions & GenerateOptions = {},
): Promise<CreateDeltaResult> {
  const signature = await buildSignature(old, options);
  const gen = generateDelta(signature, toAsyncIterable(newData), options);
  const parts: Buffer[] = [];
  let stats: GenerateStats | undefined;
  for (;;) {
    const next = await gen.next();
    if (next.done) {
      stats = next.value;
      break;
    }
    parts.push(next.value);
  }
  return { patch: Buffer.concat(parts), signature, stats: stats! };
}

export interface ApplyDeltaResult {
  data: Buffer;
  stats: ApplyStats;
}

/**
 * One-shot helper: apply `patch` to `old` and return the reconstructed data.
 * Throws (never returns a partial result) when validation fails.
 */
export async function applyDeltaToBuffer(
  old: OldDataSource,
  patch: Buffer | AsyncIterable<Buffer>,
  options: ApplyOptions = {},
): Promise<ApplyDeltaResult> {
  const gen = applyDelta(old, toAsyncIterable(patch), options);
  const parts: Buffer[] = [];
  for (;;) {
    const next = await gen.next();
    if (next.done) {
      return { data: Buffer.concat(parts), stats: next.value };
    }
    parts.push(next.value);
  }
}
