import { createHash } from 'node:crypto';
import { DEFAULT_STRONG_SIZE, FILE_DIGEST_SIZE } from './format';

/** Strong digest of a single block/window. Must be deterministic. */
export type StrongDigest = (chunk: Uint8Array) => Buffer;

/** Default strong digest: SHA-256 truncated to `size` bytes. */
export function sha256Strong(size: number = DEFAULT_STRONG_SIZE): StrongDigest {
  return (chunk) => createHash('sha256').update(chunk).digest().subarray(0, size);
}

/** Incremental whole-file digest (SHA-256, 32 bytes). */
export function createFileDigest(): { update(chunk: Uint8Array): void; digest(): Buffer } {
  const hash = createHash('sha256');
  return {
    update(chunk) {
      hash.update(chunk);
    },
    digest() {
      return hash.digest();
    },
  };
}

export { FILE_DIGEST_SIZE };
