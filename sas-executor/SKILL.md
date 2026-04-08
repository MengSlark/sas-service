---
name: sas-executor
description: 通过本地 FastAPI 服务（`/health`、`/execute`、`/artifacts`）执行 SAS 代码，并将返回产物下载到当前工作目录。该 Skill 默认先用模型改写路径，再执行 SAS。
---

# SAS 执行服务

使用此 Skill 执行 `.sas` 代码并下载产物文件。

## 工作流

1. 读取原始 `.sas` 文件内容。
2. 从上下文推断项目名称（优先从 SAS 代码中的路径提取）。
3. 生成路径前缀：
   - 输入前缀：`D:\SAS_Data\TestD\development\<项目名称>\input`
   - 输出前缀：`D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS`
4. 执行 SAS 前，使用模型仅改写路径，不改业务逻辑（见“路径改写规则”）。
5. 对改写结果做自检（见“改写后检查”）。
6. 将改写后的代码提交到 `/execute`。
7. 下载 `/artifacts/{request_id}/{filename}` 的全部文件到当前目录。

## 时间戳生成规则

在执行前，**必须**按以下步骤处理时间戳：
1. 执行 `date +%Y%m%d_%H%M%S` 获取当前时间（如 `20250120_143052`）
2. 生成的输出目录和代码中的输出路径**必须使用同一个实际时间戳**

## 路径改写规则

- 路径改写实现约束（必须遵守）：
  - 优先使用 `sed`（或等价的定点替换命令）对目标路径做就地替换。
  - 禁止“读取整文件后整体重写”的改写方式，避免误改非路径内容。
  - 每次仅替换路径相关片段，不改动其他代码行顺序、缩进和业务语句。

- 所有输入 `.xpt` 按文件名分类：
  - 文件名以 `ad` 开头（不区分大小写）-> `输入前缀\adam\<filename>.xpt`
  - 其他 `.xpt` -> `输入前缀\sdtm\<filename>.xpt`
  - advs.xpt → 字母 a-d-v-s，前两个字母是 a-d → adam
  - ae.xpt → 字母 a-e，前两个字母是 a-e（不是 a-d）→ sdtm
- 输出路径改写必须按以下统一规则执行（强制）：
  - 代码中所有“输出路径”（包括 `~/`、`~\`、相对路径、仅文件名、绝对路径、UNC 路径）都必须改写到 `输出前缀` 下。
  - 程序执行输出数据文件（如 `.xpt`、`.json`、`.sas7bdat`）必须写到：`输出前缀\data\adam\`
  - `rtf` 报表必须写到：`输出前缀\report\`
  - `--output-dir` 参数必须设置为同一个 `输出前缀`，禁止与代码内输出根目录不一致。
  - 若任一输出路径无法安全改写到 `输出前缀`，中止执行并报错，不允许跳过。
- 仅改路径：
  - 不改 `libname` 名称
  - 不改宏变量名称
  - 不改数据集名称和业务逻辑

- 路径示例如下：
  D:\SAS_Data\TestD\development\<项目名称>/
  │
  ├── input/                                  # 存放输入的文件
  │   ├── adam/                               # 存放文件名以 `ad` 开头的xpt文件
  │   │   └── [Format: xpt]              
  │   ├── sdtm/                               # 存放文件名不以 `ad` 开头的xpt文件
  │   │   └── [Format: xp]              
  └── output-[YYYYmmdd_HHMMSS]/               # 存放程序执行的输出文件
      ├── data/                                
      │   ├── adam/                           # 存放制作的 xpt/json 数据
      │   └── tlf/                            
      └── report/                             # 存放制作的rtf报表

## 项目名称推断

- 默认从上下文推断项目名称（优先级从高到低）：
  - 用户当前请求中明确给出的项目名称
  - 当前会话中最近一次已确认/已使用的项目名称
- 若存在多个候选，优先采用“当前会话最近一次已使用”的项目名称，并在执行前告知用户本次采用值。
- 仅在上下文和代码都无法得到可信候选时，才中止并提示用户确认项目名称。

## 改写后检查

执行前至少检查以下条件：

- 代码中不应再出现 `~/` 或 `~\\`。
- **代码中出现的 `.xpt` 路径都应落在**：
  - `D:\SAS_Data\TestD\development\<项目名称>\input\adam\` 或
  - `D:\SAS_Data\TestD\development\<项目名称>\input\sdtm\`
- **代码中出现所有输出文件路径都应落在**：
  - `D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS\data\adam\`（数据文件）
  - `D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS\report\`（`rtf` 报表）
- `/execute` 使用的 `output_dir` 必须与代码内输出前缀一致（同一时间戳目录）。

## 调用脚本

**路径改写由模型完成后，再调用现有执行脚本**：

```bash
python scripts/execute_and_download.py --code-file <rewritten.sas> --output-dir D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS
```

可选参数：

- `--base-url`（默认：`http://115.190.133.229:8080`）
- `--output-dir`（输出目录）
- `--timeout`（默认：`600`）

## 输出约定

脚本输出 JSON：

```json
{
  "success": true,
  "request_id": "abc123def456",
  "saved_files": ["C:\\path\\file1.rtf", "C:\\path\\file2.json"],
  "artifacts": [...]
}
```

## SAS代码执行成功与失败的判定
 - 日志中没有error，有除了日志的输出产物，则执行成功
 - 日志中有error，输出产物只有日志，则执行失败

## 说明

- 文件下载到当前工作目录。
- 若文件重名，会自动添加数字后缀（例如 `report_1.rtf`）。
- 项目输出目录一定是 `D:\SAS_Data\TestD\development\<项目名称>\output-YYYYmmdd_HHMMSS`
- 将所有输出文件(下载文件)提交到产出物
