# bindelta

流式二进制增量补丁库（TypeScript / Node.js）：对旧数据分块生成签名（弱滚动校验和 + 强摘要），
据此把新数据编码为 **COPY / INSERT** 指令流，并能将补丁流应用到旧数据上重建新数据。

- 旧数据随机访问（`BufferSource` / `FileSource`），新数据与补丁全程流式处理
- 生成器滑窗 + 有界缓冲，峰值内存不随文件大小线性增长（自适应块大小限制签名表规模）
- 匹配必须同时通过弱校验、强摘要与长度校验；重复块稳定决胜（最低块索引）
- 应用器校验指令范围、输出总长度与最终 SHA-256 摘要；任何失败都抛错，绝不返回部分结果
- 相同输入生成字节级确定的补丁；支持 `AbortSignal` 取消

## 构建与测试

```bash
npm install
npm test        # tsc + node --test（含 48 MiB 大文件内存测试）
```

## 快速上手

```ts
import {
  BufferSource, FileSource,
  buildSignature, generateDelta, applyDelta,
  createDelta, applyDeltaToBuffer,
} from './src';

// 一次性（小数据）
const old = new BufferSource(oldBytes);
const { patch } = await createDelta(old, newBytes, { blockSize: 4096 });
const { data } = await applyDeltaToBuffer(old, patch);   // 校验失败会抛 DeltaError

// 流式（大文件，内存有界）
const oldFile = await FileSource.open('old.bin');
const signature = await buildSignature(oldFile);          // 自适应块大小
for await (const chunk of generateDelta(signature, newDataStream)) {
  patchSink.write(chunk);                                 // 补丁可边生成边写出
}
const out = applyDelta(oldFile, patchStream);
for await (const chunk of out) { /* 重建的新数据分块产出 */ }
// 迭代正常结束才代表结果完整；中途抛错必须丢弃已产出字节
```

## 补丁格式（确定性编码）

```
header   = "BDP1" u8(version) varint(blockSize) varint(oldLength) byte[32](oldDigest)
instr    = 0x01 varint(blockIndex) varint(length)        # COPY：old[index*blockSize, +length)
         | 0x02 varint(length) byte[length]              # INSERT：字面量
         | 0x00                                          # END
trailer  = varint(newLength) byte[32](sha256(newData))
```

所有整数为 LEB128 变长编码。签名（`"BDS1"`）含每块的 32 位弱校验和与强摘要，
可 `serializeSignature` / `parseSignature` 往返，字节级确定。

## 正确性保证

- **匹配验证**：弱校验命中后必须强摘要与块长度同时相等才发 COPY；
  弱碰撞而强不同 → 视为未命中（字面量）；强摘要相同 → 取最低块索引（稳定决胜）。
- **应用校验**：魔数/版本、旧数据长度一致性、COPY 范围越界、输出总长 = 声明值、
  输出 SHA-256 = 尾部摘要、尾部之后无多余字节。任一失败抛出 `DeltaError`（含 `code`）。
- **取消**：生成与应用在每个迭代点检查 `AbortSignal`，中止时抛出 `CANCELLED`。

## 测试覆盖（30 项）

空输入（空旧/空新/全空）、块边界插入、重复块决胜、全量不同、尾部短块、
弱碰撞（注入常数弱校验）、强摘要碰撞模拟（最终摘要捕获错误 COPY）、
截断指令流（多个截断点）、损坏补丁（魔数/版本/操作码/范围/长度/摘要/尾随字节）、
取消（预中止与流中中止）、字节级确定性（含签名序列化往返）、
48 MiB 大文件峰值内存（生成/应用堆增长 ≪ 文件大小）。
