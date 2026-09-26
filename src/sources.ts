import { open, stat } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { DeltaError } from './errors.js';

/**
 * 支持随机读取的旧数据源。生成器与应用器都只依赖这一接口，
 * 因此旧数据不必整体驻留内存（例如直接基于文件句柄实现）。
 */
export interface RandomReadSource {
  /** 数据总字节数。 */
  readonly size: number;
  /**
   * 读取 [offset, offset+length) 区间，返回的 Buffer 长度必须恰为 length。
   * 读取越界或短读都应拒绝（reject）。
   */
  read(offset: number, length: number, signal?: AbortSignal): Promise<Buffer>;
}

/** 基于内存 Buffer 的随机读取源（测试与小数据使用）。 */
export function bufferSource(data: Buffer): RandomReadSource {
  return {
    size: data.length,
    read(offset, length) {
      if (offset < 0 || length < 0 || offset + length > data.length) {
        return Promise.reject(
          new DeltaError(`read out of range: offset=${offset} length=${length} size=${data.length}`),
        );
      }
      // 复制一份，避免调用方修改影响底层数据。
      return Promise.resolve(Buffer.from(data.subarray(offset, offset + length)));
    },
  };
}

export interface FileRandomReadSource extends RandomReadSource {
  close(): Promise<void>;
}

/** 打开一个文件作为随机读取源；使用完必须调用 close()。 */
export async function openFileSource(path: string): Promise<FileRandomReadSource> {
  const handle: FileHandle = await open(path, 'r');
  const s = await stat(path);
  const size = s.size;

  return {
    size,
    async read(offset, length, signal) {
      if (offset < 0 || length < 0 || offset + length > size) {
        throw new DeltaError(`read out of range: offset=${offset} length=${length} size=${size}`);
      }
      const buf = Buffer.allocUnsafe(length);
      let pos = 0;
      const abortCheck = () => {
        if (signal?.aborted) {
          const err = new Error('aborted') as Error & { code?: string };
          err.code = 'ABORT_ERR';
          throw err;
        }
      };
      while (pos < length) {
        abortCheck();
        const { bytesRead } = await handle.read(buf, pos, length - pos, offset + pos);
        if (bytesRead === 0) {
          throw new DeltaError(`unexpected EOF from file at offset ${offset + pos}`);
        }
        pos += bytesRead;
      }
      return buf;
    },
    close() {
      return handle.close();
    },
  };
}
