---
name: sas-executor-dev
description: 在执行 SAS 前，先按项目目录规范识别并改写输入输出路径，完成自检后再调用 `/execute` 并下载全部产物到当前工作目录。
---

# SAS Path Rewrite + Execute Protocol

本 Skill 用于处理这类请求：
- 执行 `.sas` 文件
- 先改写路径再执行 SAS
- 执行后下载全部产物

不用于这类请求：
- 仅解释 SAS 逻辑、讨论算法、代码讲解（且未要求改写或执行）

## 1) Hard Rules (必须遵守)

1. 只改路径，不改业务逻辑。
2. 若任一关键路径无法高置信判断，立即中止执行。
3. 默认流程是：先改写 -> 通过自检 -> 再执行。
4. 不允许低置信“猜测式提交”。
5. `/execute` 的 `output_dir` 必须与代码内输出前缀一致。

禁止修改项：
- `libname` 名称
- 宏变量名称
- 数据集名称
- 业务判断、数据处理、统计逻辑

## 2) Canonical Target Paths (固定模板)

输入前缀（固定）：
`D:\SAS_Data\TestD\development\<项目名称>\input`

输出前缀（固定）：
`D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS`

说明：
- 只允许替换 `<项目名称>` 和 `YYYYmmdd_HHMMSS`
- 不允许改为其他根目录或目录结构

`.xpt` 输入分类：
- 文件名以 `ad` 开头 -> `输入前缀\adam\`
- 其他 `.xpt` -> `输入前缀\sdtm\`

输出分类：
- ADaM 数据文件（`.xpt`/`.json`/`.sas7bdat`）-> `输出前缀\data\adam\`
- TLF 相关数据或中间数据 -> `输出前缀\data\tlf\`
- `rtf` 报表 -> `输出前缀\report\`

## 3) Project Name Resolution

按优先级推断项目名称：
1. 用户当前请求中明确给出的项目名称
2. 当前会话最近一次提到的项目名称

若存在多个候选且无法唯一确定：中止并提示用户确认。

## 4) Timestamp Rule

执行前生成真实时间戳：`YYYYmmdd_HHMMSS`

一致性要求：
- 代码内所有输出路径使用同一个时间戳
- `/execute.output_dir` 使用同一个时间戳
- 不允许代码输出目录与 `output_dir` 不一致

## 5) Path Semantics First (先判语义再改写)

改写前先判断每个路径/宏变量属于哪一类：
- 项目根目录
- 输入根目录
- 输出根目录

若某宏变量语义是项目根目录（如 `projectdir`/`rootdir`/`projdir`）：
- 该变量应保持为项目根目录语义
- 其值应为：`D:\SAS_Data\TestD\development\<项目名称>\`
- 不得把该变量改成具体输入目录或具体输出目录

若输入与输出都依赖同一项目根变量：
- 保留变量语义
- 仅改写引用处的后缀结构
- 不改写该变量为“输出目录变量”

优先策略：
- 优先改“引用处落点”
- 非必要不改“根目录变量定义”

示例：
- `&projectdir.input\adam` -> `D:\SAS_Data\TestD\development\<项目名称>\input\adam`
- `&projectdir.output\data\tlf` -> `D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS\data\tlf`

## 6) Inputs To Analyze (必须纳入分析)

所有以下形式都要识别：
- `~/` 或 `~\`
- 相对路径
- 仅文件名
- 绝对路径
- UNC 路径
- 宏变量拼接路径
- 函数/表达式构造路径

## 7) Execution Workflow (顺序不可乱)

### Step A: 识别
- 读取原始 `.sas`
- 列出输入路径、输出路径、动态构造点、项目名候选、风险点
- 执行“防截断保护”检查；若命中截断关键词，先完成重新读取与完整性确认，再继续

### Step B: 生成内部改写计划
计划至少包含：
- 原始输入路径/文件名 -> 目标输入路径
- 原始输出路径 -> 目标输出路径
- 项目名称
- 时间戳
- 每项改写理由
- 每项改写置信度
- 未解决风险项

### Step C: 实际改写
- 仅修改路径相关内容
- 业务逻辑保持不变

### Step D: 改写后自检
执行前至少验证：
1. 读取链路无截断风险（未出现任何截断关键词；且当前改写基于完整源文件）
2. 不再出现 `~/` 或 `~\`
3. 所有 `.xpt` 输入路径逐一通过：
   - 是输入路径，不是输出路径
   - 可解析为绝对输入路径
   - `ad` -> `D:\SAS_Data\TestD\development\<项目名称>\input\adam\`
   - 非 `ad*` -> `D:\SAS_Data\TestD\development\<项目名称>\input\sdtm\`
   - `advs.xpt` → 字母 a-d-v-s，前两个字母是 a-d → adam
   - `ae.xpt` → 字母 a-e，前两个字母是 a-e（不是 a-d）→ sdtm
   - 不得被改写到 `output-YYYYmmdd_HHMMSS` 下
4. 所有输出文件路径均落在同一 `D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS`
5. ADaM/TLF/RTF 分类落点符合规范
6. `/execute.output_dir` 与代码输出前缀一致
7. 改写前后业务逻辑一致

仅当全部通过，进入 Step E。

### Step E: 执行与下载

```bash
python scripts/execute_and_download.py --code-file <rewritten.sas> --output-dir D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS
```

可选参数：
- `--base-url`（默认 `http://115.190.133.229:8080`）
- `--timeout`（默认 `600`）

## 8) Result Decision

- 日志无明显 `error` 且存在日志外有效产物 -> 成功
- 日志有明显 `error` 且仅日志或产物为空 -> 失败
- 服务端返回成功但日志与产物冲突 -> 报告“结果不一致风险”

## 9) User-Facing Response Contract

对用户反馈需简洁且包含：
- 本次项目名称
- 本次时间戳
- 是否完成路径改写
- 若未执行，阻断原因
- 若已执行，执行结果与本地产物概况

## 10) Artifact Contract

- 产物下载到当前工作目录
- 文件重名时自动加数字后缀
- 项目输出目录必须是：
  `D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS`
- 所有下载文件（artifacts）都视为本次任务产出
