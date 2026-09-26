/**
 * Rolling weak checksum over a sliding window.
 *
 * The implementation follows the classic rsync checksum:
 *   a = sum(x[i])                    (mod 2^16)
 *   b = sum((len - i) * x[i])        (mod 2^16)
 *   value = (b << 16) | a
 *
 * State transitions are O(1):
 *  - update: append bytes to the window
 *  - roll:   remove the front byte, append one byte (window length unchanged)
 *  - pop:    remove the front byte (window shrinks by one, used at EOF)
 */
export interface RollingChecksum {
  /** Reset to an empty window. */
  reset(): void;
  /** Append bytes to the window. */
  update(chunk: Uint8Array, offset?: number, length?: number): void;
  /** Slide the window: drop `outByte` at the front, append `inByte`. */
  roll(outByte: number, inByte: number): void;
  /** Drop the front byte without appending (window shrinks). */
  pop(outByte: number): void;
  /** Current 32-bit unsigned checksum value. */
  value(): number;
}

/** Factory for the default rsync-style rolling checksum. */
export function createRsyncChecksum(): RollingChecksum {
  let a = 0;
  let b = 0;
  let n = 0; // current window length
  return {
    reset() {
      a = 0;
      b = 0;
      n = 0;
    },
    update(chunk, offset = 0, length = chunk.length - offset) {
      let s1 = a;
      let s2 = b;
      // Appending byte x at the end: a += x; b += a_before_append.
      for (let i = 0; i < length; i++) {
        s1 = (s1 + chunk[offset + i]) & 0xffff;
        s2 = (s2 + s1) & 0xffff;
      }
      a = s1;
      b = s2;
      n += length;
    },
    roll(outByte, inByte) {
      a = (a - outByte + inByte) & 0xffff;
      b = (b - n * outByte + a) & 0xffff;
    },
    pop(outByte) {
      a = (a - outByte) & 0xffff;
      b = (b - n * outByte) & 0xffff;
      n -= 1;
    },
    value() {
      return (((b & 0xffff) << 16) | (a & 0xffff)) >>> 0;
    },
  };
}

/** Factory used to plug in alternative weak checksums (mainly for tests). */
export type RollingChecksumFactory = () => RollingChecksum;
