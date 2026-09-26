/** Error codes produced by signature/delta/patch operations. */
export type DeltaErrorCode =
  | 'BAD_MAGIC'
  | 'BAD_VERSION'
  | 'BAD_HEADER'
  | 'BAD_OPCODE'
  | 'BAD_VARINT'
  | 'TRUNCATED'
  | 'RANGE'
  | 'OLD_LENGTH_MISMATCH'
  | 'LENGTH_MISMATCH'
  | 'DIGEST_MISMATCH'
  | 'TRAILING_DATA'
  | 'LIMIT_EXCEEDED'
  | 'SOURCE'
  | 'INVALID_OPTION'
  | 'CANCELLED';

/** Single error type for all failures in this library. */
export class DeltaError extends Error {
  readonly code: DeltaErrorCode;
  constructor(message: string, code: DeltaErrorCode) {
    super(message);
    this.name = 'DeltaError';
    this.code = code;
  }
}

/** Throw a CANCELLED error when the signal is aborted. */
export function checkAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new DeltaError('operation cancelled', 'CANCELLED');
  }
}
