import { promises as fsp } from 'node:fs';
import { DeltaError } from './errors';

/**
 * Random-access view of the old data. Only `readAt` is required by the
 * delta generator (indirectly, via signature building) and the applier.
 */
export interface OldDataSource {
  /** Total length in bytes. */
  readonly length: number;
  /** Read exactly `length` bytes starting at `offset`. */
  readAt(offset: number, length: number): Promise<Buffer>;
  /** Release any underlying resources. */
  close?(): Promise<void>;
}

function validateRange(total: number, offset: number, length: number): void {
  if (
    !Number.isSafeInteger(offset) ||
    !Number.isSafeInteger(length) ||
    offset < 0 ||
    length < 0 ||
    offset + length > total
  ) {
    throw new DeltaError(
      `read out of range: offset=${offset} length=${length} total=${total}`,
      'RANGE',
    );
  }
}

/** In-memory old data (tests, small inputs). */
export class BufferSource implements OldDataSource {
  private readonly buf: Buffer;
  constructor(data: Buffer | Uint8Array) {
    this.buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
  }
  get length(): number {
    return this.buf.length;
  }
  readAt(offset: number, length: number): Promise<Buffer> {
    validateRange(this.buf.length, offset, length);
    return Promise.resolve(this.buf.subarray(offset, offset + length));
  }
}

/** File-backed old data; memory usage is independent of file size. */
export class FileSource implements OldDataSource {
  private handle: fsp.FileHandle;
  readonly length: number;

  private constructor(handle: fsp.FileHandle, length: number) {
    this.handle = handle;
    this.length = length;
  }

  static async open(path: string): Promise<FileSource> {
    const handle = await fsp.open(path, 'r');
    try {
      const stat = await handle.stat();
      return new FileSource(handle, stat.size);
    } catch (err) {
      await handle.close();
      throw err;
    }
  }

  async readAt(offset: number, length: number): Promise<Buffer> {
    validateRange(this.length, offset, length);
    if (length === 0) return Buffer.alloc(0);
    const buf = Buffer.allocUnsafe(length);
    const { bytesRead } = await this.handle.read(buf, 0, length, offset);
    if (bytesRead !== length) {
      throw new DeltaError(
        `short read: wanted ${length} bytes at ${offset}, got ${bytesRead}`,
        'SOURCE',
      );
    }
    return buf;
  }

  async close(): Promise<void> {
    await this.handle.close();
  }
}
