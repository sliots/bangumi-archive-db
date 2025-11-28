# Bangumi Archive 数据处理工具

一个用于处理 Bangumi Archive 数据的Python工具，支持从 jsonlines 文件读取数据并批量插入 PostgreSQL 数据库。

**Release 把 [bangumi/Archive] 的部分指标转为可查询的 SQL 数据表，并按快照日期（`data_date`）发布 `.sql` 备份。可便于后续分析与查询。**  

## 表结构
当前发布的 `.sql` 中包含以下三张表（PostgreSQL）：

```sql
create table character_stats
(
    id        integer not null,
    comments  integer default 0,
    collects  integer default 0,
    data_date date    not null,
    primary key (id, data_date)
);

create table person_stats
(
    id        integer not null,
    comments  integer default 0,
    collects  integer default 0,
    data_date date    not null,
    primary key (id, data_date)
);

create table subject_stats
(
    id            integer not null,
    score         numeric(3, 1),
    score_details jsonb,
    rank          integer,
    favorite      jsonb,
    data_date     date    not null,
    primary key (id, data_date)
);
```
## 数据范围

- dump-2025-11-25.210310Z.zip
- ......
- dump-2024-10-04.184615Z.zip

## 项目结构

```
bangumiArchive/
├── __init__.py                 # 包初始化文件
├── main.py                     # 主程序入口
├── config.py                   # 配置文件
├── requirements.txt            # 项目依赖
├── README.md                   # 项目说明
├── processors/                 # 数据处理器模块
│   ├── __init__.py
│   ├── base.py                # 基础抽象类
│   ├── character_person.py    # 角色和人物处理器
│   ├── subject.py             # 作品处理器
│   └── factory.py             # 处理器工厂
├── utils/                      # 工具模块
│   ├── __init__.py
│   └── logger.py              # 日志配置
└── bangumiArchive/            # 数据文件目录
    ├── character.jsonlines
    ├── person.jsonlines
    └── subject.jsonlines
```

## 功能特性

- **模块化设计**: 采用现代Python项目结构，代码组织清晰
- **多数据类型支持**: 支持character、person、subject三种数据类型
- **批量处理**: 高效的批量数据插入和更新
- **数据验证**: 完整的数据验证和错误处理机制
- **进度显示**: 可选的进度条显示（需要tqdm）
- **统计信息**: 处理完成后自动生成统计报告
- **异常处理**: 完善的异常处理和日志记录

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置

在 `config.py` 中配置数据库连接信息：

```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'bangumi',
    'user': 'postgres',
    'password': 'your_password'
}
```

数据日期由 `bangumiArchive` 子目录的最新 `master` 提交消息自动解析（形如 `dump-YYYY-MM-DD.*.zip`）。

可选：通过环境变量设置迭代起始日期，从该日期对应的提交开始依次处理到最新：

- PowerShell
  ```powershell
  setx DATA_START_DATE "2025-09-02"
  # 新开终端生效后运行
  python main.py
  ```
- Bash
  ```bash
  export DATA_START_DATE="2025-09-02"
  python main.py
  ```

## 使用方法

### 命令行使用

```bash
# 处理单一数据类型
python main.py character
python main.py person
python main.py subject

# 处理所有数据类型
python main.py all

# 限制处理记录数
python main.py 1000           # 所有类型各读取前1000条
python main.py subject 1000   # 指定类型读取前1000条
```

### 按提交迭代处理

开启迭代模式后，程序将：删除当前 jsonlines → 切换到对应提交 → 按该提交的日期处理 → 再删除 jsonlines → 进入下一提交，直到最新。

示例：

```powershell
$env:DATA_START_DATE="2025-09-02"
python main.py
```

注意：迭代过程中在 `bangumiArchive` 子目录中执行 `git checkout -f` 切换提交，会覆盖未提交改动，请确保该子目录工作区干净。

### 编程接口使用

```python
from processors import ProcessorFactory
from config import DATABASE_CONFIG

# 创建处理器
processor = ProcessorFactory.create_processor('character', DATABASE_CONFIG)

# 连接数据库
processor.connect_db()

# 创建表
processor.create_tables()

# 处理文件
processor.process_file('bangumiArchive/character.jsonlines')

# 关闭连接
processor.close_connection()
```

## 日志

程序运行时会生成详细的日志信息，包括：
- 处理进度
- 错误信息
- 统计数据
- 性能指标

日志文件位置：`bangumi_data_processor.log`
