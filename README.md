# streaming-delta-patch

零依赖的二进制增量补丁库（TypeScript / Node.js），rsync 风格：

- 根据**旧数据的分块签名**（Adler-32 弱滚动校验 + SHA-256 强摘要）把新数据编码为
  `COPY` / `INSERT` 指令流；
- 应用器读取指令流，从支持**随机读取**的旧数据源重建新数据；
- 新数据与补丁均为**流式**处理（async iterable，拉取驱动、背压友好）；
- 生成器常驻内存只与块大小相关，**不随文件大小增长**；
- 应用器验证指令范围、输出总长度与最终摘要，任何失败都会抛错，
  **绝不把不完整的结果宣称为成功**。

## 快速开始

```ts
import {
  bufferSource,
  generatePatch,
  applyPatchToBuffer,
} from 'streaming-delta-patch';

const oldData = Buffer.from('...旧数据...');
const newData = Buffer.from('...新数据...');

// 生成（newData 也可以是任意 AsyncIterable<Buffer>，如文件流）
const patchChunks: Buffer[] = [];
for await (const chunk of generatePatch(bufferSource(oldData), [newData], 2048)) {
  patchChunks.push(chunk);
}
const patch = Buffer.concat(patchChunks);

// 应用：成功完成（不抛错）时返回的内容才是完整、可信的
const rebuilt = await applyPatchToBuffer(bufferSource(oldData), [patch]);
```

大文件场景下旧数据不必进内存，直接使用文件源，补丁也可以边生成边应用：

```ts
import { openFileSource, applyPatchToWritable } from 'streaming-delta-patch';
import { createWriteStream } from 'node:fs';

const oldSource = await openFileSource('/data/old.bin');
try {
  await applyPatchToWritable(
    oldSource,
    generatePatch(oldSource, newDataReadable, 4096),
    createWriteStream('/data/new.bin'),
  );
} finally {
  await oldSource.close();
}
```

## API 概览

| 导出 | 说明 |
| --- | --- |
| `generatePatch(oldSource, newData, blockSize?, opts?)` | 返回补丁字节的 `AsyncGenerator<Buffer>` |
| `applyPatch(oldSource, patch, opts?)` | 返回重建字节的 `AsyncGenerator<Buffer>` |
| `applyPatchToBuffer(oldSource, patch, opts?)` | 便捷封装：收集为单个 Buffer |
| `applyPatchToWritable(oldSource, patch, dest, opts?)` | 便捷封装：写入 Writable |
| `SignatureTable.build(source, blockSize)` | 单独构建分块签名表 |
| `bufferSource(buf)` / `openFileSource(path)` | 随机读取源 |
| `weakChecksum` / `createRolling` / `strongDigest` | Adler-32 与 SHA-256 原语 |

`AbortSignal`：两个方向都接受 `{ signal }`，包括生成器阻塞在慢速输入流上时也会被中断。

## 补丁格式（SDP1）

全部小端编码：

```
头部   magic "SDP1" | version=1 | flags=0 | blockSize uint32 | oldSize uint64
指令   COPY   0x01 | offset uint64 | length uint32
       INSERT 0x02 | length uint32 | length 个原始字节
       END    0x03 | newSize uint64 | newSha256 32B
```

## 设计要点

**两级签名与碰撞处理。** 弱校验只用于快速过滤滑窗候选；弱值命中后必须计算
SHA-256 并与签名逐字节比对，强摘要确认成功才发出 COPY。弱碰撞（不同内容、相同
Adler-32）不会造成误匹配。

**稳定决胜。** 同一弱值桶中的候选按旧数据块序号升序保存，重复块永远决胜到旧数据
中**最靠左**的那一块。

**有界内存。** 扫描器只保留一个 `blockSize` 的环形窗口、一个 64 KiB 的字面量聚合
缓冲，以及与旧文件块数成正比的签名表（每块约几十字节）。字节离开滑窗时才被
"宣判"为字面量——此时覆盖它的所有候选窗口都已检查完毕，输出决策不可回退。
实测 16 MiB 与 64 MiB 输入下生成+应用全程的 V8 堆增量都只有约 100–250 KiB。

**尾部短块。** 旧数据最后一块可以短于 `blockSize`；它只可能在窗口填充阶段（或
一次 COPY 之后的重新填充阶段）对齐匹配。

**应用端验证（任一失败即拒绝整个结果）：**

1. 魔数 / 版本 / 标志位，以及头部 `oldSize` 与实际旧数据源大小一致；
2. 每条 COPY 的 `[offset, offset+length)` 完全落在旧数据范围内；
3. 输出总长度不超过 END 声明长度且最终严格相等；
4. 全部输出字节的 SHA-256 与 END 摘要一致；
5. END 之后无尾随字节；指令或载荷截断、未知操作码一律报错。

> 注意：流处理语义下，部分结果字节可能在最终校验前已经到达下游。调用方只有在
> 迭代**正常结束（未抛异常）**时才能接受结果；使用 Writable 封装时若抛错，
> 目标文件应视为不完整并丢弃。

**确定性。** 给定相同的旧数据、新数据与块大小，补丁字节逐字节相同，与输入/输出
被切成多大的 Buffer 无关。

## 开发

```bash
npm install
npm run typecheck
npm test        # 含大文件有界内存集成测试（需要 --expose-gc）
npm run build
```
