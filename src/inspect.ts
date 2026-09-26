import { DeltaError } from './errors';
import {
  FILE_DIGEST_SIZE,
  FORMAT_VERSION,
  OP_COPY,
  OP_END,
  OP_INSERT,
  PATCH_MAGIC,
} from './format';
import { StreamReader } from './varint';

export type PatchOp =
  | { op: 'copy'; index: number; length: number }
  | { op: 'insert'; length: number };

export interface PatchInfo {
  blockSize: number;
  oldLength: number;
  oldDigest: Buffer;
  newLength: number;
  newDigest: Buffer;
  ops: PatchOp[];
}

/**
 * Parse a patch stream into its header, instruction list and trailer.
 * INSERT payloads are skipped, not retained. Intended for tests, debugging
 * and tooling; it does not need the old data.
 */
export async function inspectPatch(
  patch: Buffer | AsyncIterable<Buffer>,
): Promise<PatchInfo> {
  const reader = new StreamReader(toIterable(patch));
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
  const oldDigest = await reader.read(FILE_DIGEST_SIZE);

  const ops: PatchOp[] = [];
  for (;;) {
    const op = await reader.readByte();
    if (op === OP_END) break;
    if (op === OP_COPY) {
      ops.push({ op: 'copy', index: await reader.readVarint(), length: await reader.readVarint() });
    } else if (op === OP_INSERT) {
      const length = await reader.readVarint();
      await reader.read(length);
      ops.push({ op: 'insert', length });
    } else {
      throw new DeltaError(`unknown opcode 0x${op.toString(16)}`, 'BAD_OPCODE');
    }
  }
  const newLength = await reader.readVarint();
  const newDigest = await reader.read(FILE_DIGEST_SIZE);
  if (!(await reader.finished())) {
    throw new DeltaError('trailing data after patch trailer', 'TRAILING_DATA');
  }
  return { blockSize, oldLength, oldDigest, newLength, newDigest, ops };
}

function toIterable(input: Buffer | AsyncIterable<Buffer>): AsyncIterable<Buffer> {
  if (Buffer.isBuffer(input)) {
    return (async function* () {
      yield input;
    })();
  }
  return input;
}
