# D2D

**基于 Arrow 的有界内存数据迁移引擎。** 从源库分片并行读取，经过可选的转换流水线，
写入目标库；无转换的纯搬运场景可以让字节不经过 Arrow 解析。

> 0.1.x：配置与公开 API 仍可能调整。

## 特性

- **分片并行**：分片切法由内核统一决定（`partition::plan`），各连接器只负责
  渲染本数据库的 SQL / 请求，不各写一套切分逻辑。
- **两条路径**：无转换算子时，源端与目标端可以协商走**物理直通**（源端产出的
  字节直接交给目标端）；否则走 **Arrow 逻辑面**。协商失败自动回落，不会静默改值。
- **有界内存**：连接器内部不攒批；攒批由内核按 `flow.sink.batch_bytes` 完成，
  配合内存账本、背压与 spill。
- **失败要大声**：类型无法表达、列序漂移、目标端只确认了部分行，都会让任务失败
  并指出位置；不静默丢行或改值。
- **方法路由**：同一个数据库的不同访问方式（ODBC / 原生快载 / 原生 API）是
  `method` 参数，不是新连接器。

## 连接器

| connector | source | sink | methods |
|---|---|---|---|
| `dameng` | ✓ | ✓ | `odbc` / `dmfldr` / `dpi` |
| `doris` | ✓ | ✓ | 读：FE MySQL 协议口；写：Stream Load |
| `mysql` | ✓ | ✓ | MySQL 协议 |
| `postgres` | ✓ | ✓ | PostgreSQL 协议 |
| `qsdb` | ✓ | ✓ | HTTP（`qsdb server`） |

原生依赖只在选择对应 method 时需要：

- 达梦 `dpi`：运行期加载 `libdmdpi.so` / `dmdpi.dll`（64 位）；
- 达梦 `dmfldr`：目标机上有 `dmfldr` 可执行文件；
- 达梦 `odbc`：unixODBC 与达梦 ODBC 驱动。

其余连接器没有额外原生依赖。

QSDB 统一以 **server 模式**运行：先启动 `qsdb server`，再在 `connection.url` 里填
`http://host:port`。连接器通过官方 `qsdb-client` 读写（写走 Arrow IPC），
不支持进程内 embedded 模式。

## 构建

Rust 1.88+。

```bash
cargo build --release
```

## 快速开始

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
  type_contract:
    - { column: id,         source_type: BIGINT,       sink_type: BIGINT }
    - { column: created_at, source_type: TIMESTAMP(6), sink_type: DATETIME(6) }
    - { column: amount,     source_type: DECIMAL(18,2), sink_type: DECIMAL(18,2) }
  query:
    columns: [id, created_at, amount]
  partition:
    mode: hash
    column: id
    count: 8
  properties:
    dpi:
      library: "${DM_DPI_LIB}"

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
# 只校验配置与内存账，不连接数据库
cargo run --release -- --config job.yaml --dry-run

# 真正执行
cargo run --release -- --config job.yaml
```

## 测试

```bash
cargo test --workspace --lib --tests
```

需要真实数据库的集成测试默认 `#[ignore]`，本地 `cargo test` 不会连接外部服务。

## 代码结构

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
