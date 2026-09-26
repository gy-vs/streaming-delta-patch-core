/** Deterministic byte pattern via a small LCG (stable across runs). */
export function pattern(seed: number, length: number): Buffer {
  const buf = Buffer.allocUnsafe(length);
  let x = (seed >>> 0) || 1;
  for (let i = 0; i < length; i++) {
    x = (x * 1664525 + 1013904223) >>> 0;
    buf[i] = (x >>> 16) & 0xff;
  }
  return buf;
}

/** Wrap a buffer as an async iterable of fixed-size chunks. */
export async function* toStream(
  data: Buffer,
  chunkSize = 4096,
): AsyncGenerator<Buffer, void, void> {
  for (let pos = 0; pos < data.length; pos += chunkSize) {
    yield data.subarray(pos, Math.min(pos + chunkSize, data.length));
  }
}
