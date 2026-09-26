import {
  LengthMismatchError,
  PatchFormatError,
  RangeValidationError,
  TruncatedPatchError,
} from './errors.js';

/**
 * 补丁二进制格式（小端编码）。
 *
 * 头部（18 字节）：
 *   magic   4B  = "SDP1"
 *   version 1B  = 1
 *   flags   1B  = 0（保留）
 *   blockSize     uint32 LE
 *   oldSize       uint64 LE
 *
 * 指令流：
 *   COPY   opcode=0x01, offset uint64 LE, length uint32 LE
 *   INSERT opcode=0x02, length uint32 LE, 紧跟 length 个原始字节
 *   END    opcode=0x03, newSize uint64 LE, newSha256 32B
 */
export const MAGIC = Buffer.from('SDP1');
export const FORMAT_VERSION = 1;
export const HEADER_SIZE = 18;
export const FOOTER_SIZE = 1 + 8 + 32; // opcode + newSize + sha256

export const OP_COPY = 0x01;
export const OP_INSERT = 0x02;
export const OP_END = 0x03;

/** 单条 INSERT 允许携带的最大字节数（length 字段为 uint32）。 */
export const MAX_INSERT_LENGTH = 0xffffffff;

export interface PatchHeader {
  blockSize: number;
  oldSize: bigint;
}

export function writeHeader(out: Buffer[], blockSize: number, oldSize: bigint): void {
  const buf = Buffer.allocUnsafe(HEADER_SIZE);
  MAGIC.copy(buf, 0);
  buf.writeUInt8(FORMAT_VERSION, 4);
  buf.writeUInt8(0, 5);
  buf.writeUInt32LE(blockSize, 6);
  buf.writeBigUInt64LE(oldSize, 10);
  out.push(buf);
}

export function parseHeader(buf: Buffer): PatchHeader {
  if (buf.length !== HEADER_SIZE) {
    throw new TruncatedPatchError('the 18-byte patch header');
  }
  if (!buf.subarray(0, 4).equals(MAGIC)) {
    throw new PatchFormatError('bad patch magic: not an SDP1 stream');
  }
  const version = buf.readUInt8(4);
  if (version !== FORMAT_VERSION) {
    throw new PatchFormatError(`unsupported patch version: ${version}`);
  }
  if (buf.readUInt8(5) !== 0) {
    throw new PatchFormatError('unknown flags set in patch header');
  }
  const blockSize = buf.readUInt32LE(6);
  if (blockSize === 0) {
    throw new PatchFormatError('blockSize in patch header must be non-zero');
  }
  return { blockSize, oldSize: buf.readBigUInt64LE(10) };
}

/** 追加单字节操作码。 */
export function writeOpcode(out: Buffer[], opcode: number): void {
  out.push(Buffer.from([opcode]));
}

/** 追加一条 COPY 指令：opcode + uint64 offset + uint32 length。 */
export function writeCopyOp(out: Buffer[], offset: number, length: number): void {
  const buf = Buffer.allocUnsafe(13);
  buf.writeUInt8(OP_COPY, 0);
  buf.writeBigUInt64LE(BigInt(offset), 1);
  buf.writeUInt32LE(length, 9);
  out.push(buf);
}

/** 追加 INSERT 指令头：opcode + uint32 length（载荷由调用方另行写出）。 */
export function writeInsertOp(out: Buffer[], length: number): void {
  if (length < 0 || length > MAX_INSERT_LENGTH) {
    throw new RangeValidationError(`INSERT length out of range: ${length}`);
  }
  const buf = Buffer.allocUnsafe(5);
  buf.writeUInt8(OP_INSERT, 0);
  buf.writeUInt32LE(length, 1);
  out.push(buf);
}

/** 追加 END 指令：opcode + uint64 newSize + 32B sha256。 */
export function writeEndOp(out: Buffer[], newSize: bigint, newSha256: Buffer): void {
  if (newSha256.length !== 32) {
    throw new PatchFormatError('END digest must be exactly 32 bytes (sha256)');
  }
  const buf = Buffer.allocUnsafe(FOOTER_SIZE);
  buf.writeUInt8(OP_END, 0);
  buf.writeBigUInt64LE(newSize, 1);
  newSha256.copy(buf, 9);
  out.push(buf);
}

/**
 * 校验"输出总长度"相关的不变量。
 * 所有总长度以 number 保存，必须始终处于安全整数范围内。
 */
export function checkTotalLength(nextTotal: number, declaredNewSize: bigint): number {
  if (!Number.isSafeInteger(nextTotal)) {
    throw new RangeValidationError('reconstructed output length exceeds safe integer range');
  }
  if (BigInt(nextTotal) > declaredNewSize) {
    throw new LengthMismatchError(
      `output exceeds declared new size (${nextTotal} > ${declaredNewSize})`,
    );
  }
  return nextTotal;
}
