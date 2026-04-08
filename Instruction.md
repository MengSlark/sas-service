# Folder Struction
交互界面连接的后台项目文件夹

## Pan-EN-xxxxx/Pan-CN-xxxxx
project folder的命名应当是“Pan-EN-”（英文试验）或“Pan-CN-”（中文试验）的前缀，后缀是原试验的数字部分，例如：MDV3800→Pan-EN-3800；

### 1. input  
存放制作adam和报表时使用到的文件

#### 1.1 sdtm
存放制作adam时的源数据。
格式：xpt/json。执行sas code时应当在同folder下转存一份同名的sas7bdat格式的数据，如果原先不存在的话

#### 1.2 adam
存放制作报表时的源数据。
格式：xpt/json。执行sas code时应当在同folder下转存一份同名的sas7bdat格式的数据，如果原先不存在的话

#### 1.3 docs
存放制作adam和报表时使用到的输入文档

##### 1.3.1 protocol
试验方案。
标准文件名：Pan-CN-xxxxx-protocol.docx/doc/pdf
有多个文件时采用后缀来区分

##### 1.3.2 sap
统计分析计划。
标准文件名：Pan-CN-xxxxx-sap.docx/doc/pdf
有多个文件时采用后缀来区分

##### 1.3.3 shell
报表样式文件。
标准文件名：Pan-CN-xxxxx-mockup.docx/doc/pdf
有多个文件时采用后缀来区分

##### 1.3.4 spec
数据映射说明。
标准文件名：Pan-CN-xxxxx-adam-mapping-spec.xlsx/Pan-CN-xxxxx-sdtm-mapping-spec.xlsx/Pan-CN-xxxxx-adtm-define.xml/Pan-CN-xxxxx-sdtm-define.xml

### 2. output  
存放验证集和报表

#### 2.1 data
存放验证集

##### 2.1.1 adam
存放adam验证集

##### 2.1.2 tlf
存放报表验证集

#### 2.2 report
存放示例报表

#### 2.3 program
存放示例code

### 3. output-[YYYYMMDD-HHMMSS] 或其他方式的唯一工作流识别符
存放制作adam和报表时的输出文件

#### 3.1 data
存放制作adam和报表时的输出数据

##### 3.1.1 adam
存放制作的adam输出数据

##### 3.1.2 tlf
存放制作的报表输出数据

#### 3.2 report
存放制作的报表

#### 3.3 program
存放生成的sas code



```sql
# Project Folder Structure (Pan-EN-xxxxx / Pan-CN-xxxxx)
# Note: Project folder naming convention: "Pan-EN-" (English) or "Pan-CN-" (Chinese) + Trial Number (e.g., Pan-EN-3800)

Development/Pan-EN-xxxxx/
│
├── input/                                  # 存放制作 adam 和报表时使用到的文件
│   ├── sdtm/                               # 存放制作 adam 时的源数据
│   │   └── [Format: xpt/json]              # 执行 SAS code 时需在同目录下转存同名 .sas7bdat
│   ├── adam/                               # 存放制作报表时的源数据
│   │   └── [Format: xpt/json]              # 执行 SAS code 时需在同目录下转存同名 .sas7bdat
│   └── docs/                               # 存放制作 adam 和报表时使用到的输入文档
│       ├── protocol/                       # 试验方案
│       │   └── [File: Pan-EN-xxxx-protocol.docx/pdf]   # 多文件时采用后缀区分
│       ├── sap/                            # 统计分析计划
│       │   └── [File: Pan-EN-xxxx-sap.docx/pdf]        # 多文件时采用后缀区分
│       ├── shell/                          # 报表样式文件
│       │   └── [File: Pan-EN-xxxx-mockup.docx/pdf]     # 多文件时采用后缀区分
│       └── spec/                           # 数据映射说明
│           └── [File: Pan-EN-xxxx-*-mapping-spec.xlsx, -define.xml]
├── output                                  # 存放sample验证集和示例报表和程序
│   ├── data/                               # 存放验证集
│   │   ├── adam/                           # 存放 adam 验证集：JSON 格式
│   │   └── tlf/                            # 存放示例报表的验证集
│   ├── program/                            # 存放示例程序
│   └── report/                             # 存放示例报表rtf
└── output-[YYYYMMDD_HHMMSS]/               # 存放制作 adam 和报表时的输出文件
    ├── data/                               # 存放制作 adam 和报表时的输出数据
    │   ├── adam/                           # 存放制作的 adam 输出数据
    │   └── tlf/                            # 存放制作的报表输出数据
    ├── program/                            # 存放生成的sas code（暂时不支持）
    └── report/                             # 存放制作的报表

```