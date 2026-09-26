import { strongDigest, weakChecksum } from './checksums.js';
import { RangeValidationError, throwIfAborted } from './errors.js';
import type { RandomReadSource } from './sources.js';

/** 旧数据一个分块的签名。 */
export interface BlockSignature {
  /** 块在旧数据中的序号（从 0 开始），也是稳定决胜的次序。 */
  readonly index: number;
  /** 块在旧数据中的字节偏移。 */
  readonly offset: number;
  /** 块长度（最后一块可能小于 blockSize）。 */
  readonly length: number;
  /** 弱滚动校验值（uint32）。 */
  readonly weak: number;
  /** 强摘要（sha256，32 字节）。 */
  readonly strong: Buffer;
}

/**
 * 签名哈希表：弱校验值 -> 候选块列表。
 *
 * 弱校验只用于过滤：命中弱校验后必须逐字节比较/计算强摘要确认。
 * 同一弱值下的候选按块序号升序保存 —— 这是"重复块稳定决胜"的基础：
 * 查找永远返回第一个（序号最小、即最靠左的）确认匹配的候选。
 *
 * 表大小为 O(旧数据块数)，与文件大小成常数比例（每块约 40 字节），
 * 这是算法本身要求的有界状态；扫描器滑动窗口只保留 blockSize 字节。
 */
export class SignatureTable {
  /** key 为弱校验值的无符号十进制字符串（避免 JS Map 的负数键问题）。 */
  readonly #byWeak = new Map<number, BlockSignature[]>();
  readonly blockSize: number;
  readonly totalSize: number;

  private constructor(blockSize: number, totalSize: number, entries: BlockSignature[]) {
    this.blockSize = blockSize;
    this.totalSize = totalSize;
    for (const sig of entries) {
      let bucket = this.#byWeak.get(sig.weak);
      if (!bucket) {
        bucket = [];
        this.#byWeak.set(sig.weak, bucket);
      }
      bucket.push(sig);
    }
  }

  /** 分块流式读取旧数据构建签名表（每次至多持有一个块）。 */
  static async build(
    source: RandomReadSource,
    blockSize: number,
    options: { signal?: AbortSignal; strong?: (data: Buffer) => Buffer } = {},
  ): Promise<SignatureTable> {
    if (!Number.isInteger(blockSize) || blockSize <= 0) {
      throw new RangeValidationError(`blockSize must be a positive integer, got ${blockSize}`);
    }
    const strong = options.strong ?? strongDigest;
    const entries: BlockSignature[] = [];
    const total = source.size;
    let offset = 0;
    let index = 0;
    while (offset < total) {
      throwIfAborted(options.signal);
      const length = Math.min(blockSize, total - offset);
      const chunk = await source.read(offset, length, options.signal);
      entries.push({
        index: index++,
        offset,
        length,
        weak: weakChecksum(chunk),
        strong: strong(chunk),
      });
      offset += length;
    }
    return new SignatureTable(blockSize, total, entries);
  }

  /**
   * 先用弱校验筛候选；命中后由调用方提供强摘要做确定性确认。
   * 返回弱值桶（按 index 升序），无候选时返回 undefined。
   */
  candidates(weak: number): readonly BlockSignature[] | undefined {
    return this.#byWeak.get(weak);
  }

  /**
   * 强摘要确认：在弱值桶中找到第一个 strong 相等的候选。
   * 候选按 index 升序遍历，因此重复块永远决胜到最靠左的同内容块。
   */
  confirm(weak: number, strong: Buffer): BlockSignature | undefined {
    const bucket = this.#byWeak.get(weak);
    if (!bucket) return undefined;
    for (const sig of bucket) {
      if (sig.strong.equals(strong)) return sig;
    }
    return undefined;
  }

  /** 返回指定长度的所有签名（按 index 升序）。旧数据至多有一个非满块。 */
  candidatesByLength(length: number): BlockSignature[] {
    const out: BlockSignature[] = [];
    for (const bucket of this.#byWeak.values()) {
      for (const sig of bucket) {
        if (sig.length === length) out.push(sig);
      }
    }
    out.sort((x, y) => x.index - y.index);
    return out;
  }

  /** 旧数据块总数（含尾部短块）。 */
  blockCount(): number {
    let n = 0;
    for (const bucket of this.#byWeak.values()) n += bucket.length;
    return n;
  }
}
