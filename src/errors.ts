/**
 * 库内所有错误的基类；`name` 固定为具体错误类名，便于调用方按类型分支。
 */
export class DeltaError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** 补丁格式不合法（魔数、版本、操作码、长度字段等）。 */
export class PatchFormatError extends DeltaError {}

/** 补丁在读取完整指令或载荷前结束。 */
export class TruncatedPatchError extends PatchFormatError {
  constructor(what: string) {
    super(`patch stream ended before ${what} could be read`);
    this.name = 'TruncatedPatchError';
  }
}

/** 指令越界：COPY 超出旧文件，或输出长度超出补丁头部声明/安全整数范围。 */
export class RangeValidationError extends DeltaError {}

/** 输出总长度与补丁 END 指令声明的长度不一致。 */
export class LengthMismatchError extends DeltaError {}

/** 输出全部字节的最终摘要与补丁 END 指令声明的摘要不一致。 */
export class DigestMismatchError extends DeltaError {}

/** 补丁头部声明的旧数据大小与实际旧数据源大小不一致。 */
export class OldSizeMismatchError extends DeltaError {}

/** 操作（生成或应用）通过 AbortSignal 被取消。 */
export class AbortError extends DeltaError {
  constructor(message = 'operation aborted') {
    super(message);
    this.name = 'AbortError';
  }
}

/**
 * 若信号已经中止则抛出 {@link AbortError}。
 * 不使用 DOMException，保证跨运行时可 instanceof 判定。
 */
export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw new AbortError();
}
