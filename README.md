# D2D

[![CI](https://github.com/qinwenhui/D2D/actions/workflows/ci.yml/badge.svg)](https://github.com/qinwenhui/D2D/actions/workflows/ci.yml)

**A bounded-memory, Arrow-native data migration engine.** It reads a source database in
parallel partitions, applies an optional transform pipeline, and writes to the target; for
pure move jobs the bytes can pass through without being parsed into Arrow.

**基于 Arrow 的有界内存数据迁移引擎。** 从源库分片并行读取，经过可选的转换流水线，
写入目标库；无转换的纯搬运场景可以让字节不经过 Arrow 解析。

> Documentation is currently in Chinese.

---

## 目录

- [D2D 是什么](#d2d-是什么)
- [连接器与方法](#连接器与方法)
- [安装](#安装)
- [快速开始](#快速开始)
- [配置参考](#配置参考)
- [连接器详解](#连接器详解)
- [部署到 CentOS 7](#部署到-centos-7)
- [排错](#排错)
- [开发与测试](#开发与测试)
- [许可证](#许可证)

## D2D 是什么

一次迁移任务由五个部分组成：

```text
source  ──► [source_pool] ──► pipeline ──► [sink_pool] ──► sink
 ×N        容量 C1            转换算子链         容量 C2        ×M
```

- **分片并行**：分片切法由内核统一决定（`source.partition`），各连接器只负责渲染
  本数据库的 SQL / 请求，不各写一套切分逻辑。
- **两条路径**：无转换算子时，源端与目标端可以协商走**物理直通**（源端产出的字节
  直接交给目标端）；否则走 **Arrow 逻辑面**。协商失败自动回落，不会静默改值。
- **有界内存**：连接器内部不攒批；攒批由内核按 `flow.sink.batch_bytes` 完成，
  配合内存账本、背压与 spill。
- **失败要大声**：类型无法表达、列序漂移、目标端只确认了部分行，都会让任务失败
  并指出位置；不静默丢行或改值。
- **方法路由**：同一个数据库的不同访问方式（ODBC / 原生快载 / 原生 API）是
  `method` 参数，不是新连接器。

不适用：需要跨表关联、流式 CDC、事务级一致性快照的场景。D2D 做的是**表到表的批量搬运**。

## 连接器与方法

| connector | source | sink | methods |
|---|---|---|---|
| `dameng` | ✓ | ✓ | `odbc`（缺省）/ `dmfldr` / `dpi` |
| `doris` | ✓ | ✓ | 读：`mysql`（缺省）/ `flight`；写：Stream Load |
| `mysql` | ✓ | ✓ | MySQL 协议 |
| `postgres` | ✓ | ✓ | PostgreSQL 协议 |
| `qsdb` | ✓ | ✓ | HTTP（`qsdb server`） |

运行机上的额外依赖：

| method | 需要什么 |
|---|---|
| 所有方法 | `libodbc.so.2`（二进制的链接期依赖，见 [部署](#部署到-centos-7)） |
| 达梦 `dpi` | 64 位达梦客户端提供的 `libdmdpi.so` |
| 达梦 `dmfldr` | 达梦客户端提供的 `dmfldr` 可执行文件 |
| 达梦 `odbc` | unixODBC + 达梦 ODBC 驱动 + `odbc.ini` / `odbcinst.ini` |
| 其余连接器 | 无额外原生依赖 |

> QSDB 统一以 **server 模式**运行：先启动 `qsdb server`，再在 `connection.url` 里填
> `http://host:port`。连接器通过官方 `qsdb-client` 读写（写走 Arrow IPC），
> 不支持进程内 embedded 模式。

## 安装

### 1. 用发布包

从 [Releases](https://github.com/qinwenhui/D2D/releases) 按目标平台选择：

| 产物 | 平台 | 系统要求 |
|---|---|---|
| `d2d-<version>-linux-x86_64-glibc217.tar.gz` | Linux x86_64 | glibc ≥ 2.17（CentOS 7 / RHEL 7，也适用于新发行版） |
| `d2d-<version>-linux-aarch64-glibc217.tar.gz` | Linux aarch64 | glibc ≥ 2.17 |
| `d2d-<version>-windows-x86_64.zip` | Windows x64 | Windows 10 / 11 |
| `d2d-<version>-windows-aarch64.zip` | Windows on ARM | Windows 11 ARM64 |

Linux 包都在 CentOS 7 兼容环境里构建，x86_64 / aarch64 都能运行在
CentOS 7 及以上（glibc ≥ 2.17）。每个包解压后包含：

```text
d2d（Windows 上是 d2d.exe）
README.md
LICENSE / LICENSE-MIT / LICENSE-APACHE
examples/*.yaml
```

Linux 运行前先装 unixODBC 运行库（二进制的链接期依赖）：

```bash
# CentOS 7 / RHEL 7
yum install -y unixODBC
# Debian / Ubuntu
apt install -y unixodbc

ldd ./d2d | grep 'not found'   # 应当没有输出
```

> **为什么不提供 musl 静态包**：`dpi` / `odbc` 需要在运行期 `dlopen` 达梦客户端库，
> 而静态 musl 二进制没有动态加载器，`dlopen` 会直接失败；同时 `odbc-sys` 还要求在
> 链接期找到 unixODBC。老平台请用上面的 **glibc217** 包，它保留了动态加载能力，
> 并且把 glibc 基线压到了 2.17。

### 2. 从源码构建

要求 Rust **1.89+**。

Linux 构建需要 unixODBC 开发包（达梦 `odbc` 路径在链接期就要 `-lodbc`，
即便本次任务不用它）：

```bash
# Debian / Ubuntu
apt install unixodbc-dev
# CentOS 7 / RHEL 7
yum install unixODBC-devel

cargo build --release
./target/release/d2d --version
```

HTTPS 走 rustls，不依赖系统 OpenSSL。

## 快速开始

下面这份配置把达梦的一张表用 **DPI** 读出来、用 **Doris Stream Load** 写进去。
把它存成 `job.yaml`，按需替换环境变量：

```yaml
job:
  name: demo

flow:
  source: { concurrency: 8 }
  pipeline: { concurrency: 8 }
  sink:
    concurrency: 8
    batch_bytes: 64MB

source:
  connector: dameng
  method: dpi
  connection:
    host: "${DM_HOST}"
    port: 5238
    user: "${DM_USER}"
    password: "${DM_PASS}"
  schema_name: "${DM_SCHEMA}"
  table: demo_src
  query:
    columns: [id, created_at, amount]
  type_contract:
    - { column: id,         source_type: BIGINT,       sink_type: BIGINT }
    - { column: created_at, source_type: TIMESTAMP(6), sink_type: DATETIME(6) }
    - { column: amount,     source_type: DECIMAL(18,2), sink_type: DECIMAL(18,2) }
  partition:
    mode: hash
    column: id
    count: 8
  properties:
    dpi:
      library: "${DM_DPI_LIB}"    # libdmdpi.so 的绝对路径

sink:
  connector: doris
  connection:
    fe_hosts: ["${DORIS_FE}"]
    http_port: 8030
    be_hosts: ["${DORIS_BE}:8040"]
    username: "${DORIS_USER}"
    password: "${DORIS_PASS}"
  database: demo
  table: demo_dst

engine:
  memory_mb: 4096
```

```bash
# 只校验配置、类型契约与内存账，不连接数据库
./d2d --config job.yaml --dry-run

# 真正执行；-l 控制日志级别（trace/debug/info/warn/error）
./d2d --config job.yaml -l info
```

DPI 方式在运行机上还需要两样东西：

1. 已安装/授权的 **64 位达梦客户端**，提供 `libdmdpi.so`；
2. 系统有 `libodbc.so.2`（`yum install unixODBC`）。

`libdmdpi.so` 不在发布包里（达梦 SDK 许可原因）。见 [达梦 DPI](#达梦-dpi)。

## 配置参考

一份任务配置由六段组成，只有 `source` / `sink` 是必需的：

```yaml
job:        # 任务名
flow:       # 执行拓扑：并发、池、攒批
source:     # 读什么、怎么连
transform:  # 引擎侧转换算子链（可选）
sink:       # 写到哪里、怎么连
engine:     # 内存预算与核验开关（可选）
```

### flow：执行拓扑

```yaml
flow:
  source:
    concurrency: 8          # 同时几个取数单元；0 = 跟随 pipeline.concurrency
    pool:
      capacity: 512MB       # 池里最多积压多少字节
      on_overflow: spill    # backpressure（缺省）| spill | fail
      max_spill: 4GB        # spill 上限；超过后退化为背压
      spill_dir: /data/d2d_spill   # 缺省用系统临时目录
  pipeline:
    concurrency: 8          # 同时几个处理 task；0 = CPU 核数
    batch_bytes: 0          # 0 = 不累积（缺省）；非 0 = 用一次 concat 换更少算子调用
    batch_rows: 0           # 行数兜底；0 = 不限
  sink:
    concurrency: 4          # 同时几个写入；0 = 跟随 pipeline.concurrency
    batch_bytes: 100MB      # 内核一次递给连接器的字节上界（缺省 100MB）
    batch_rows: 200000      # 行数上界（缺省 200000）
    pool:
      capacity: 512MB
      on_overflow: backpressure
```

要点：

- 三个 `concurrency` 互相独立，**不会被互相推导**。内存不够时池溢出到磁盘或背压，
  不会偷偷减小并发。
- `pool.capacity` 单位是字节，支持 `512MB` / `2GB` 这种写法。
- `spill` 需要可写的 `spill_dir`；目录不可用在启动期就失败。
- `sink.batch_bytes` 决定一次写入多大。每次写入有与大小无关的固定开销，
  **没有特别理由不要往下调**；缺省 100MB 是实测甜点。
- 攒批住在内核：连接器收到一份就写一份，内部不再缓存。

### source：通用字段

```yaml
source:
  connector: dameng           # 必填
  method: dpi                 # 可选；缺省由连接器决定
  connection: { ... }         # 必填，连接器特定
  table: T_ORDER              # 必填
  schema_name: APP            # 可选；库/模式名（各连接器语义见下文）
  properties: { ... }         # 可选；连接器专属参数，引擎不解释

  query:                      # 可选：显式下推
    columns:                  # 缺省 = SELECT *
      - id
      - name
      - expr: "\"amount\" * 1.1"
        alias: scaled_amount
    where_clause: |
      "status" <> -1
    order_by: |
      "id" ASC
    limit: 1000000

  type_contract:              # 可选（dmfldr 源必填）
    - { column: id, source_type: BIGINT, sink_type: BIGINT }

  partition:                  # 可选：取数范围怎么切
    mode: range               # none（缺省）| list | range | hash
    column: id
    count: 8
    lower: 1                  # range 可显式给上下界；不给则连接器探测
    upper: 200000
```

**分区 ≠ 并发。** `partition` 只决定数据切成几段；`flow.source.concurrency`
决定同时跑几段。切 16 段、并发 4 路，就是 4 路从头跑到尾，谁腾出来谁接下一段。

| `partition.mode` | 写法 | 说明 |
|---|---|---|
| `none` | — | 不分片，单任务整表读 |
| `list` | `conditions: [ "col < 100", "col >= 100" ]` | 每条 = 一个分区；不探测 |
| `range` | `column` + `count`，可选 `lower`/`upper` | 给了上下界则零探测；不给则探测 MIN/MAX |
| `hash` | `column` + `count` | `MOD(HASH(col), n) = i`；不探测 |

`list` 的切法**正确性由你负责**：漏写一段就是漏迁一段数据。行数对账会在最后失败，
但最好一开始就切对。

**类型契约**（`type_contract`）是启动期的兼容性校验，也是 dmfldr 源唯一的 schema 来源。
每列声明源库类型和目标库类型；二进制列转字符串这类必然乱码的组合会被直接拒绝。

### transform：转换算子

只在 Arrow 逻辑面生效；算子链为空时自动走直通快路。

```yaml
transform:
  - op: project
    columns: [id, name, amount]
  - op: column_map
    mappings:
      - { from: create_time, to: dt }
  - op: cast
    mappings:
      - { column: amount, target_type: float64 }
  - op: const_column
    name: source_system
    value: dm
```

算子名或参数名拼错会在解析配置时直接报错，不会静默跳过。

### sink：通用字段

```yaml
sink:
  connector: doris
  method: stream_load        # 可选；缺省由连接器决定
  connection: { ... }
  properties: { ... }
  database: demo             # 目标库名（各连接器语义见下文）
  table: demo_dst
  write:                     # 可选：写入行为
    max_retries: 3
    retry_backoff_ms: 1000
```

### engine：内存预算与核验

```yaml
engine:
  memory_mb: 4096            # 全链路内存预算；省略 = 不做启动期校验
  verify_row_count: true     # 源端行数 vs 目标端确认行数，不一致即失败（缺省开）
  verify_forbidden_scan: true  # 直通载荷的受限字节扫描（缺省开）
  direct_preflight_scan: false # 开工前整片预扫（缺省关）
  worker_threads: 0          # tokio 线程数；0 = CPU 核数
```

`memory_mb` **不是并发的因**。启动期把六项常驻相加（源端取数单元、source_pool、
pipeline 工作集、转换中间态、sink_pool、sink 工作集）与它比对：装不下就拒绝启动，
并告诉你超在哪一项、有哪几条处置方式。它不会改变任何并发度。

`verify_row_count` 是目前唯一能抓住“静默丢数”的机制（例如 Doris UNIQUE 模型
对重复键静默 upsert）。除非有明确理由，否则不要关。

## 连接器详解

### 达梦

读、写两个方向都支持 `odbc`（缺省）/ `dmfldr` / `dpi`。

| 方法 | 速度 | 运行机依赖 | 适用 |
|---|---|---|---|
| `dpi` | 快 | `libdmdpi.so`（64 位）+ unixODBC 运行库 | 大批量读写；值不受文本限制 |
| `dmfldr` | 最快 | `dmfldr` 可执行 + unixODBC 运行库 | 全表导出/快载；值里不能有分隔符/换行 |
| `odbc` | 慢 | unixODBC + 达梦 ODBC 驱动 | 需要 WHERE / 计算列 / ORDER BY / LIMIT 的覆盖路径 |

#### 连接配置

```yaml
connection:
  host: "192.0.2.65"
  port: 5238
  user: "${DM_USER}"
  password: "${DM_PASS}"
  # svc: "DM8SVC"            # dm_svc.conf 服务名（优先于 host:port）
  # auth_as: "SYSDBA"        # SYSDBA / SYSSSO / SYSAUDITOR / USERS / AUTO
```

ODBC 方法还可以直接用 DSN：

```yaml
connection:
  dsn: "DM8SVC"              # /etc/odbc.ini 里的一条
  user: "${DM_USER}"
  password: "${DM_PASS}"
```

DSN 方式把驱动名和主机解析交给 `odbc.ini` / `dm_svc.conf` 管理，最省事。
如果驱动拒绝 DSN-less 连接串，就改用 DSN。

#### 达梦 DPI

```yaml
source:
  method: dpi
  properties:
    dpi:
      library: "/opt/dmdbms/drivers_odbc/libdmdpi.so"  # 不配则在 LD_LIBRARY_PATH 找
      fetch_rows: 4000          # 每次数组取数多少行
      column_bytes: 256         # 字符串列缓冲下限（实际按列宽 ×4 放大）
      character_code: "utf8"    # 会话字节声明，不是设置
      physical_block_mb: 64     # 稳态块；建议 ≥ sink.batch_bytes 对应大小
      first_block_mb: 33        # 首块
```

写方向：

```yaml
sink:
  method: dpi
  properties:
    dpi:
      library: "/opt/dmdbms/drivers_odbc/libdmdpi.so"
      param_rows: 20000         # 一次 dpi_exec 执行多少行
      max_buffer_mb: 64         # 单块参数缓冲上限
      character_code: "utf8"
```

注意事项：

- **只支持 64 位**。32 位构建会直接编译失败（DPI 的 `slength`/`ulength` 在 DM64
  下是 8 字节）。
- `libdmdpi.so` 由达梦客户端提供，不从本仓库分发。如果加载失败但文件存在，
  把达梦的 `bin` 和 `drivers_odbc` 目录都加进 `LD_LIBRARY_PATH`。
- 写方向只接受 `character_code: utf8`：DPI 按客户端会话字符集解释字节，
  非 UTF-8 会写乱码。
- NULL 由 DPI 指示器精确表达，和空串严格区分。

#### 达梦 dmfldr

```yaml
source:
  method: dmfldr
  properties:
    dmfldr:
      executable: "dmfldr"        # PATH 名或绝对路径
      work_dir: "/data/d2d_dmfldr" # 临时目录，需容纳导出文本
      character_code: "utf8"
      field_seperator: "|"
      null_mode: true
      null_str: "NULL"
      buffer_node_size: 64       # 每个子进程的读缓冲 MB
      parse_parallel: 1          # 一个产物内部并行解析几块
```

- `partition.mode: none` 时是 `TABLE=` 模式：整表导出一个文件，不支持
  WHERE / ORDER BY / LIMIT / 计算列；需要行级裁剪请用 `odbc`。
- 分片模式写 `SQLFILE=`，一片一个文件。
- `dmfldr` 源**必须**提供 `type_contract`：文本不带类型信息，列序 = 契约声明序。
- 值里出现列分隔符/换行会让这一份**失败**（dmfldr 无法转义）；`null_str` 撞车同理。
- 写方向的 `direct: true` 直接写 B 树、**不检查约束**，可能把表写成不可更新的状态。
  缺省 `false`，除非能保证数据不违反任何约束，否则不要开。

### Doris

Doris 有两个口，不要混：

| 端口 | 用途 |
|---|---|
| FE `http_port`（默认 8030） | Stream Load（写） |
| FE `query_port`（默认 9030） | MySQL 协议（读） |
| FE `flight_port`（默认 8070） | Arrow Flight SQL（读，`method: flight`） |
| BE `be_http_port`（默认 8040） | 直连 BE 的 Stream Load（推荐） |

```yaml
# 读：MySQL 协议（缺省）
source:
  connector: doris
  method: mysql
  connection:
    fe_hosts: ["192.0.2.47"]
    query_port: 19030
    username: "${DORIS_USER}"
    password: "${DORIS_PASS}"
  schema_name: "app_js_produce"   # Doris 里就是库名
  table: t_order

# 读：Arrow Flight SQL
source:
  connector: doris
  method: flight
  connection:
    fe_hosts: ["192.0.2.47"]
    flight_port: 8070
    flight_tls: false             # true = https
    username: "${DORIS_USER}"
    password: "${DORIS_PASS}"
  schema_name: "app_js_produce"
  table: t_order
```

```yaml
# 写：Stream Load
sink:
  connector: doris
  connection:
    fe_hosts: ["192.0.2.47"]
    http_port: 18030
    be_hosts: ["192.0.2.47:18040", "192.0.2.48:18040"]
    be_http_port: 18040
    username: "${DORIS_USER}"
    password: "${DORIS_PASS}"
  database: "app_js_produce"
  table: t_order
  properties:
    stream_load:
      format: "csv"               # csv | json（parquet 未实现）
      column_separator: "\t"      # 仅 csv
      line_delimiter: "\n"        # 仅 csv
      max_filter_ratio: 0.0
      strict_mode: true
      # max_request_bytes: 104857600  # json 缺省 100MB，csv 不限
```

- **强烈建议配置 `be_hosts` 直连 BE**：经 FE 转发 Stream Load 会丢 `Authorization` 头。
- 写 CSV 时，值里含分隔符/换行会被内核的直通路径拦下并失败；逻辑面会转义。
- `strict_mode: true` + `max_filter_ratio: 0` 是最安全的组合。
- Doris 源的分区列可以为 NULL：连接器渲染 `COALESCE`，不会丢行。
- 查询里的标识符用反引号；双引号在 Doris/MySQL 语义里是字符串字面量。

### MySQL

```yaml
source:
  connector: mysql
  connection: { host: "192.0.2.10", port: 3306, user: "${MYSQL_USER}", password: "${MYSQL_PASS}" }
  schema_name: "d2d_probe"    # MySQL 里就是库名
  table: src_big

sink:
  connector: mysql
  connection: { host: "192.0.2.10", port: 3306, user: "${MYSQL_USER}", password: "${MYSQL_PASS}" }
  database: "d2d_probe"
  table: dst_big
```

- 标识符用反引号（`SELECT "id"` 会返回常量）。
- 写入走多行绑定参数 INSERT，按占位符上限和 `max_allowed_packet` 分批。
- **不自动重试**：超时不等于没写进去，重试可能插两遍。

### PostgreSQL

```yaml
source:
  connector: postgres
  connection: { host: "192.0.2.13", port: 5432, user: "${PG_USER}", password: "${PG_PASS}" }
  schema_name: "postgres"     # ⚠️ 这里装的是**数据库名**
  table: src_big

sink:
  connector: postgres
  connection: { host: "192.0.2.13", port: 5432, user: "${PG_USER}", password: "${PG_PASS}" }
  database: "postgres"
  table: dst_big
```

- PostgreSQL 的 database 与 schema 是两层：`schema_name` 装数据库名，
  表所在 schema 固定用 `public`。
- 读取走简单查询协议（结果全是文本）；写入按目标表声明类型做 `::text::目标类型` 转换。
- 标识符用双引号（PostgreSQL 标准）。
- **不自动重试**，理由同 MySQL。

### QSDB

```yaml
sink:
  connector: qsdb
  method: http                      # 可省略；只给 url 时自动选 http
  connection:
    url: "http://192.0.2.20:9430"
    user: "${QSDB_USER}"            # Basic；或 token: "..." 二选一
    password: "${QSDB_PASS}"
    connect_timeout_ms: 10000
    query_timeout_ms: 0
  database: "default"               # server 模式只能是 default
  table: dst_big
  properties:
    create_table: true              # 表不存在时按首批 Arrow schema 自动建表
    # partition_by: ["dt"]
    compression: "zstd"             # zstd（缺省）| snappy
```

- 先启动服务：`qsdb server --path /var/lib/qsdb --addr 0.0.0.0:9430`。
- `qsdb-client` **不做 TLS**，只接受 `http://host:port`；需要加密请在前面挂 TLS 终止代理。
- 服务是单库的，`database` / `schema_name` 只能是 `default`。
- `connection.path`、`cache_mb` 等 embedded 配置已移除，配了会明确报错。

## 部署到 CentOS 7

发布产物在 CentOS 7 兼容环境（glibc 2.17）里构建，并做 GLIBC 符号版本断言。
在较新发行版上直接编译的二进制会引用更高的 glibc 符号版本，**不能**拿到 CentOS 7 上跑。

部署检查清单：

```bash
# 1. 系统有 unixODBC 运行库（二进制启动就需要）
yum install -y unixODBC
ldd ./d2d | grep 'not found'        # 必须为空

# 2. 用 dpi 时：达梦客户端库可见
export LD_LIBRARY_PATH=/opt/dmdbms/bin:/opt/dmdbms/drivers_odbc:$LD_LIBRARY_PATH
ls "$LD_LIBRARY_PATH"/libdmdpi.so   # 或在配置里写 properties.dpi.library

# 3. 用 dmfldr 时：可执行文件在 PATH 里
which dmfldr

# 4. 用 odbc 时：unixODBC + 达梦驱动 + odbc.ini
odbcinst -q -d
```

- `dpi` 只支持 64 位目标。
- `libdmdpi.so` / `dmfldr` 都是达梦客户端的一部分，本仓库不随包分发；
  分发前请自行确认达梦 SDK 的许可条款（见 `crates/connectors/d2d-dameng/NOTICE-dpi.md`）。
- QSDB 通过 HTTP 连接，不需要本地客户端库。

## 排错

| 现象 | 原因 / 处理 |
|---|---|
| `version 'GLIBC_2.xx' not found` | 用了在新发行版上编译的二进制。用 Release 里的 `glibc217` 包 |
| `error while loading shared libraries: libodbc.so.2` | 没装 unixODBC。`yum install unixODBC` |
| 加载 DPI 动态库失败，但文件存在 | `libdmdpi.so` 还依赖同目录的达梦库。把 `$DM_HOME/bin` 和 `drivers_odbc` 加进 `LD_LIBRARY_PATH` |
| `找不到 DPI 动态库` | 配 `properties.dpi.library` 绝对路径，或把目录加进 `LD_LIBRARY_PATH` |
| 配置报 `qsdb 现在只支持 server 模式` | 删掉 `connection.path` / `cache_mb`，改用 `connection.url` |
| Doris `no valid Basic authorization` | Stream Load 走了 FE 转发。显式配 `be_hosts` 直连 BE |
| `json body size ... exceed ... streaming_load_json_max_mb` | JSON 请求体超过 BE 上限。调 `stream_load.max_request_bytes` 或改用 csv |
| 任务失败：`对账失败` | 源端行数 ≠ 目标端确认行数。检查唯一键/去重、分区条件是否漏段 |
| 启动期拒绝：内存账装不下 | 调大 `engine.memory_mb`、调小池容量，或给池配 `on_overflow: spill` |
| `dmfldr` 失败：值里有分隔符/换行 | dmfldr 无法转义。换 `field_seperator`，或改用 `dpi` / `odbc` |
| 写达梦后是空串而不是 NULL | 检查方法：`dpi`/`odbc` 精确表达 NULL；`dmfldr` 的空值约定取决于 `null_mode` / `null_str` |

`--dry-run` 会走：配置解析 → 连接器注册 → 类型契约校验 → 内存账，不连接数据库，
是排查配置问题的第一步。

## 开发与测试

```bash
cargo test --workspace --lib --tests
```

需要真实数据库的集成测试默认 `#[ignore]`，本地 `cargo test` 不会连接外部服务。
CI 覆盖 linux/windows 的 x86_64 + aarch64，以及 CentOS 7（glibc 2.17）构建和 MSRV。

代码结构：

```text
crates/
├── kernel/                 与具体数据库无关的引擎
│   ├── d2d-core/           错误、批次、流等基础类型
│   ├── d2d-types/          逻辑类型与 Arrow 桥接
│   ├── d2d-memory/         内存账本
│   ├── d2d-transform/      转换算子
│   └── d2d-execution/      执行拓扑、攒批、链路
├── connectors/
│   ├── d2d-connector/      连接器契约（Source / Sink / TypeAdapter / Physical）
│   └── d2d-<db>/           各数据库实现；一个数据库一个 crate
└── d2d-cli/                入口：注册连接器、解析 YAML
```

## 许可证

MIT OR Apache-2.0，任选其一。见 [LICENSE](LICENSE)、[LICENSE-MIT](LICENSE-MIT)、
[LICENSE-APACHE](LICENSE-APACHE)。

达梦 DPI 的 FFI 声明来自达梦 DM8 SDK 头文件，版权归武汉达梦数据库股份有限公司所有；
本仓库不分发达梦头文件或动态库。见
[NOTICE-dpi.md](crates/connectors/d2d-dameng/NOTICE-dpi.md)。
