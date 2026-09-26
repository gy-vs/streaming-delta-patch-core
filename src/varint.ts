import { DeltaError } from './errors';

/** Encode a non-negative safe integer as unsigned LEB128. */
export function encodeVarint(value: number): Buffer {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new DeltaError(`cannot varint-encode ${value}`, 'BAD_VARINT');
  }
  const bytes: number[] = [];
  let n = value;
  do {
    let b = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) b |= 0x80;
    bytes.push(b);
  } while (n > 0);
  return Buffer.from(bytes);
}

const MAX_VARINT_BYTES = 8; // 8 * 7 = 56 bits, covers all safe integers

/**
 * Incremental byte reader over an async iterable of chunks.
 * Never buffers more than the not-yet-consumed remainder of one chunk.
 */
export class StreamReader {
  private readonly it: AsyncIterator<Buffer>;
  private buf: Buffer = Buffer.alloc(0);
  private pos = 0;
  private done = false;

  constructor(stream: AsyncIterable<Buffer>) {
    this.it = stream[Symbol.asyncIterator]();
  }

  private async ensure(): Promise<boolean> {
    while (this.pos >= this.buf.length) {
      if (this.done) return false;
      const next = await this.it.next();
      if (next.done) {
        this.done = true;
        return false;
      }
      const value = next.value;
      if (value && value.length > 0) {
        this.buf = value;
        this.pos = 0;
      }
    }
    return true;
  }

  async readByte(): Promise<number> {
    if (!(await this.ensure())) {
      throw new DeltaError('unexpected end of stream', 'TRUNCATED');
    }
    return this.buf[this.pos++];
  }

  /** Read exactly `n` bytes; throws TRUNCATED if the stream ends early. */
  async read(n: number): Promise<Buffer> {
    if (n === 0) return Buffer.alloc(0);
    const parts: Buffer[] = [];
    let remaining = n;
    while (remaining > 0) {
      if (!(await this.ensure())) {
        throw new DeltaError(
          `unexpected end of stream: wanted ${remaining} more byte(s)`,
          'TRUNCATED',
        );
      }
      const available = this.buf.length - this.pos;
      const take = Math.min(available, remaining);
      parts.push(this.buf.subarray(this.pos, this.pos + take));
      this.pos += take;
      remaining -= take;
    }
    return parts.length === 1 ? parts[0] : Buffer.concat(parts, n);
  }

  async readVarint(): Promise<number> {
    let result = 0;
    let shift = 0;
    for (let i = 0; i < MAX_VARINT_BYTES; i++) {
      const b = await this.readByte();
      result += (b & 0x7f) * 2 ** shift;
      if ((b & 0x80) === 0) {
        if (!Number.isSafeInteger(result)) {
          throw new DeltaError('varint out of range', 'BAD_VARINT');
        }
        return result;
      }
      shift += 7;
    }
    throw new DeltaError('varint too long', 'BAD_VARINT');
  }

  /** True when the underlying stream is exhausted and fully consumed. */
  async finished(): Promise<boolean> {
    return !(await this.ensure());
  }
}

/**
 * Buffered writer that emits fixed-size chunks. Keeps patch generation
 * streaming: memory stays bounded regardless of patch size.
 */
export class ChunkWriter {
  private parts: Buffer[] = [];
  private size = 0;

  constructor(private readonly threshold: number = 64 * 1024) {}

  byte(b: number): void {
    this.bytes(Buffer.from([b]));
  }

  bytes(b: Buffer): void {
    if (b.length === 0) return;
    this.parts.push(b);
    this.size += b.length;
  }

  varint(n: number): void {
    this.bytes(encodeVarint(n));
  }

  /** Flush and return a chunk when the threshold is reached, else null. */
  flushIfFull(): Buffer | null {
    return this.size >= this.threshold ? this.flush() : null;
  }

  /** Flush all buffered bytes, or null when empty. */
  flush(): Buffer | null {
    if (this.size === 0) return null;
    const out =
      this.parts.length === 1 ? this.parts[0] : Buffer.concat(this.parts, this.size);
    this.parts = [];
    this.size = 0;
    return out;
  }
}
