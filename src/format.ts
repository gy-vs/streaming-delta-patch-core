/** Binary format constants shared by the generator, applier and inspector. */

/** Magic bytes of a signature file ("BDS1"). */
export const SIGNATURE_MAGIC = Buffer.from([0x42, 0x44, 0x53, 0x31]);
/** Magic bytes of a patch stream ("BDP1"). */
export const PATCH_MAGIC = Buffer.from([0x42, 0x44, 0x50, 0x31]);

export const FORMAT_VERSION = 1;

/** Instruction opcodes in the patch stream. */
export const OP_END = 0x00;
export const OP_COPY = 0x01;
export const OP_INSERT = 0x02;

/** Length in bytes of the whole-file digest (SHA-256). */
export const FILE_DIGEST_SIZE = 32;

/** Default length in bytes of the per-block strong digest. */
export const DEFAULT_STRONG_SIZE = 16;

/** Default minimum block size used by adaptive sizing. */
export const DEFAULT_MIN_BLOCK_SIZE = 2048;

/** Default upper bound on the number of signature blocks (bounds memory). */
export const DEFAULT_MAX_BLOCKS = 8192;
