import { createRsyncChecksum, RollingChecksumFactory } from './checksum';
import { createFileDigest, sha256Strong, StrongDigest } from './digest';
import { checkAborted, DeltaError } from './errors';
import {
  DEFAULT_MAX_BLOCKS,
  DEFAULT_MIN_BLOCK_SIZE,
  DEFAULT_STRONG_SIZE,
  FILE_DIGEST_SIZE,
  FORMAT_VERSION,
  SIGNATURE_MAGIC,
} from './format';
import { OldDataSource } from './source';
import { encodeVarint, StreamReader } from './varint';

/** One block of the old data, with its weak and strong checksums. */
export interface BlockEntry {
  /** Block ordinal; offset = index * blockSize. */
  readonly index: number;
  /** Byte offset in the old data. */
  readonly offset: number;
  /** Block length; shorter than blockSize only for the tail block. */
  readonly length: number;
  /** 32-bit weak rolling checksum of the block content. */
  readonly weak: number;
  /** Strong digest of the block content. */
  readonly strong: Buffer;
}

export interface SignatureHeader {
  readonly blockSize: number;
  readonly strongSize: number;
  readonly oldLength: number;
  readonly oldDigest: Buffer;
}

/**
 * Block table over the old data. Lookup requires weak + strong + length to
 * all match; when several blocks share the same digests (duplicates), the
 * lowest index wins, which keeps output deterministic.
 */
export class Signature {
  readonly entries: BlockEntry[] = [];
  private readonly buckets = new Map<number, BlockEntry[]>();
  /** Weak checksum used for the sliding window; must match how the entries
   *  were computed. Not serialized — supplied again on parse. */
  readonly weakChecksumFactory: RollingChecksumFactory;

  constructor(
    readonly header: SignatureHeader,
    readonly strongDigest: StrongDigest,
    weakChecksumFactory: RollingChecksumFactory = createRsyncChecksum,
  ) {
    this.weakChecksumFactory = weakChecksumFactory;
  }

  get blockCount(): number {
    return this.entries.length;
  }

  addEntry(entry: BlockEntry): void {
    this.entries.push(entry);
    let bucket = this.buckets.get(entry.weak);
    if (!bucket) {
      bucket = [];
      this.buckets.set(entry.weak, bucket);
    }
    bucket.push(entry);
  }

  hasWeak(weak: number): boolean {
    return this.buckets.has(weak);
  }

  /**
   * Find a block matching weak + strong + length. Entries are stored in
   * ascending index order, so the first match is the stable lowest index.
   */
  find(weak: number, length: number, strong: Buffer): BlockEntry | undefined {
    const bucket = this.buckets.get(weak);
    if (!bucket) return undefined;
    for (const entry of bucket) {
      if (entry.length === length && entry.strong.equals(strong)) {
        return entry;
      }
    }
    return undefined;
  }
}

export interface SignatureOptions {
  /** Explicit block size; when omitted it is derived from the old length. */
  blockSize?: number;
  /** Adaptive sizing lower bound (default 2048). */
  minBlockSize?: number;
  /** Adaptive sizing cap on block count; bounds table memory (default 8192). */
  maxBlocks?: number;
  /** Strong digest per block (default: SHA-256 truncated to 16 bytes). */
  strongDigest?: StrongDigest;
  /** Rolling weak checksum factory (default: rsync-style). */
  weakChecksumFactory?: RollingChecksumFactory;
  /** Read size for streaming the old data (default 1 MiB). */
  ioChunkSize?: number;
  signal?: AbortSignal;
}

/**
 * Choose a block size such that the number of blocks stays below
 * `maxBlocks`; this keeps signature memory bounded for arbitrarily
 * large inputs.
 */
export function chooseBlockSize(
  oldLength: number,
  minBlockSize: number = DEFAULT_MIN_BLOCK_SIZE,
  maxBlocks: number = DEFAULT_MAX_BLOCKS,
): number {
  if (!Number.isSafeInteger(oldLength) || oldLength < 0) {
    throw new DeltaError(`invalid old length ${oldLength}`, 'INVALID_OPTION');
  }
  if (minBlockSize < 1 || maxBlocks < 1) {
    throw new DeltaError('invalid block sizing options', 'INVALID_OPTION');
  }
  return Math.max(minBlockSize, Math.ceil(oldLength / maxBlocks));
}

/**
 * Build a signature by streaming the old data exactly once. Memory usage is
 * O(number of blocks), which is bounded by `maxBlocks` under adaptive sizing.
 */
export async function buildSignature(
  old: OldDataSource,
  options: SignatureOptions = {},
): Promise<Signature> {
  const {
    signal,
    ioChunkSize = 1 << 20,
    minBlockSize = DEFAULT_MIN_BLOCK_SIZE,
    maxBlocks = DEFAULT_MAX_BLOCKS,
    weakChecksumFactory = createRsyncChecksum,
  } = options;
  const strongDigest = options.strongDigest ?? sha256Strong();
  const strongSize = strongDigest(Buffer.alloc(0)).length;
  if (strongSize < 1 || strongSize > FILE_DIGEST_SIZE) {
    throw new DeltaError(`invalid strong digest size ${strongSize}`, 'INVALID_OPTION');
  }
  const blockSize =
    options.blockSize ?? chooseBlockSize(old.length, minBlockSize, maxBlocks);
  if (!Number.isSafeInteger(blockSize) || blockSize < 1) {
    throw new DeltaError(`invalid block size ${blockSize}`, 'INVALID_OPTION');
  }

  const header: SignatureHeader = {
    blockSize,
    strongSize,
    oldLength: old.length,
    oldDigest: Buffer.alloc(0), // filled in below
  };
  const signature = new Signature(header, strongDigest, weakChecksumFactory);
  const fileHash = createFileDigest();

  let offset = 0;
  let carry = Buffer.alloc(0);
  let index = 0;

  const addBlock = (block: Buffer): void => {
    const weak = weakChecksumFactory();
    weak.reset();
    weak.update(block);
    signature.addEntry({
      index: index++,
      offset: header.blockSize * (index - 1),
      length: block.length,
      weak: weak.value(),
      strong: strongDigest(block),
    });
  };

  while (offset < old.length) {
    checkAborted(signal);
    const take = Math.min(ioChunkSize, old.length - offset);
    const chunk = await old.readAt(offset, take);
    if (chunk.length !== take) {
      throw new DeltaError('short read from old data source', 'SOURCE');
    }
    offset += take;
    fileHash.update(chunk);

    let buf = carry.length > 0 ? Buffer.concat([carry, chunk]) : chunk;
    let pos = 0;
    while (buf.length - pos >= blockSize) {
      addBlock(buf.subarray(pos, pos + blockSize));
      pos += blockSize;
    }
    // Copy the remainder so `carry` does not pin a large io chunk.
    carry = pos < buf.length ? Buffer.from(buf.subarray(pos)) : Buffer.alloc(0);
    buf = Buffer.alloc(0);
  }
  if (carry.length > 0) {
    addBlock(carry); // short tail block
  }

  (header as { oldDigest: Buffer }).oldDigest = fileHash.digest();
  return signature;
}

/** Serialize a signature deterministically (entries in index order). */
export function serializeSignature(signature: Signature): Buffer {
  const { header } = signature;
  if (header.oldDigest.length !== FILE_DIGEST_SIZE) {
    throw new DeltaError('signature header has invalid digest', 'BAD_HEADER');
  }
  const parts: Buffer[] = [
    SIGNATURE_MAGIC,
    Buffer.from([FORMAT_VERSION]),
    encodeVarint(header.blockSize),
    encodeVarint(header.strongSize),
    encodeVarint(header.oldLength),
    header.oldDigest,
    encodeVarint(signature.entries.length),
  ];
  for (const entry of signature.entries) {
    const weak = Buffer.allocUnsafe(4);
    weak.writeUInt32LE(entry.weak >>> 0, 0);
    parts.push(weak, encodeVarint(entry.length), entry.strong);
  }
  return Buffer.concat(parts);
}

export interface ParseSignatureOptions {
  /** Strong digest used later by the delta generator; must match the one
   *  the signature was built with. Defaults to truncated SHA-256. */
  strongDigest?: StrongDigest;
  /** Weak checksum factory used later by the delta generator; must match
   *  the one the signature was built with. Defaults to rsync-style. */
  weakChecksumFactory?: RollingChecksumFactory;
}

/** Parse a serialized signature (from a buffer or a stream of chunks). */
export async function parseSignature(
  input: Buffer | AsyncIterable<Buffer>,
  options: ParseSignatureOptions = {},
): Promise<Signature> {
  const reader = new StreamReader(toIterable(input));
  const magic = await reader.read(SIGNATURE_MAGIC.length);
  if (!magic.equals(SIGNATURE_MAGIC)) {
    throw new DeltaError('bad signature magic', 'BAD_MAGIC');
  }
  const version = await reader.readByte();
  if (version !== FORMAT_VERSION) {
    throw new DeltaError(`unsupported signature version ${version}`, 'BAD_VERSION');
  }
  const blockSize = await reader.readVarint();
  const strongSize = await reader.readVarint();
  const oldLength = await reader.readVarint();
  if (blockSize < 1 || strongSize < 1 || strongSize > FILE_DIGEST_SIZE) {
    throw new DeltaError('invalid signature header', 'BAD_HEADER');
  }
  const oldDigest = await reader.read(FILE_DIGEST_SIZE);
  const count = await reader.readVarint();

  const strongDigest = options.strongDigest ?? sha256Strong();
  const signature = new Signature(
    { blockSize, strongSize, oldLength, oldDigest },
    strongDigest,
    options.weakChecksumFactory ?? createRsyncChecksum,
  );
  for (let i = 0; i < count; i++) {
    const weakBuf = await reader.read(4);
    const weak = weakBuf.readUInt32LE(0);
    const length = await reader.readVarint();
    if (length < 1 || length > blockSize) {
      throw new DeltaError('invalid block length in signature', 'BAD_HEADER');
    }
    const strong = await reader.read(strongSize);
    signature.addEntry({ index: i, offset: i * blockSize, length, weak, strong });
  }
  if (!(await reader.finished())) {
    throw new DeltaError('trailing data after signature', 'TRAILING_DATA');
  }
  return signature;
}

function toIterable(input: Buffer | AsyncIterable<Buffer>): AsyncIterable<Buffer> {
  if (Buffer.isBuffer(input)) {
    return (async function* () {
      yield input;
    })();
  }
  return input;
}
