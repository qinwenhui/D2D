//! qmdb 写入会话：**一次 `write_batch` = 一次完整的三段式装载**
//!
//! ```text
//! write_batch(batch):
//!   ① 首批先做列对齐校验（防服务端按列名对齐时"悄悄丢列"）
//!   ② begin_load → do_put(8192 行切片流) → commit_load
//!   ③ 任何失败 → abort_load，不留半截数据
//! commit():  no-op —— 每次写入都已经原子提交了
//! abort():   no-op —— write_batch 内部失败时已经自己 abort 了
//! ```

use crate::config::LoadOptions;
use crate::flight::QmdbFlight;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use d2d_connector::SinkSession;
use d2d_core::{D2DError, D2DResult};
use tracing::{debug, info, warn};

pub struct QmdbSinkSession {
    flight: QmdbFlight,
    table: String,
    /// 目标表 schema（init 时 LIMIT 0 探的；首批列对齐用）
    target_schema: SchemaRef,
    load: LoadOptions,
    /// 首批列对齐只做一次（每条会话的第一批）
    aligned: bool,
    /// 累计确认行数（收尾日志用）
    confirmed_rows: u64,
}

impl QmdbSinkSession {
    pub(crate) fn new(
        flight: QmdbFlight,
        table: String,
        target_schema: SchemaRef,
        load: LoadOptions,
    ) -> Self {
        Self {
            flight,
            table,
            target_schema,
            load,
            aligned: false,
            confirmed_rows: 0,
        }
    }
}

/// 首批列对齐校验：服务端按**列名**对齐（大小写无关），对不上的列
/// 静默忽略/兜底 —— 对迁移工具这是"悄悄丢数据"，必须在这里大声失败。
///
/// 纯函数：不触网，可以随便测。
fn check_alignment(target: &SchemaRef, table: &str, batch: &RecordBatch) -> D2DResult<()> {
    let target_names: Vec<&str> = target.fields().iter().map(|f| f.name().as_str()).collect();
    let has = |name: &str| target_names.iter().any(|t| t.eq_ignore_ascii_case(name));

    // 批里有、目标没有 → 服务端会**悄悄扔掉**这一列 → 报错
    let mut orphans = Vec::new();
    for f in batch.schema().fields() {
        if !has(f.name()) {
            orphans.push(f.name().clone());
        }
    }
    if !orphans.is_empty() {
        return Err(D2DError::schema(format!(
            "批次里的列 {} 在目标表 {table} 上不存在（qmdb 按**列名**对齐，大小写无关；\
             对不上的列会被服务端静默丢弃，所以在这里失败）。\
             请检查目标表结构，或用 transform 投影对齐列",
            orphans.join(", ")
        )));
    }

    // 目标有、批里没有 → 服务端按 DEFAULT/NULL 兜底；警告（可能是故意的部分写入）
    let mut missing = Vec::new();
    for t in &target_names {
        if !batch
            .schema()
            .fields()
            .iter()
            .any(|f| f.name().eq_ignore_ascii_case(t))
        {
            missing.push(*t);
        }
    }
    if !missing.is_empty() {
        warn!(
            table = %table,
            columns = %missing.join(", "),
            "目标表有、批次里没有的列：服务端将按 DEFAULT/NULL 兜底（若是部分写入可忽略本警告）"
        );
    }

    // 类型不同 → 服务端按 Arrow 合法 cast 自动转；非法 cast 在装载时报错（含列名）。
    // 这里列出来让人有个预期，不拦（cast 合法性矩阵是服务端的权威）。
    for bf in batch.schema().fields() {
        if let Some(tf) = target
            .fields()
            .iter()
            .find(|t| t.name().eq_ignore_ascii_case(bf.name()))
        {
            if bf.data_type() != tf.data_type() {
                warn!(
                    column = %bf.name(),
                    from = ?bf.data_type(),
                    to = ?tf.data_type(),
                    "列类型与目标表不同：服务端会按 Arrow 合法 cast 自动转换（非法 cast 装载时报错）"
                );
            }
        }
    }
    Ok(())
}

#[async_trait]
impl SinkSession for QmdbSinkSession {
    async fn write_batch(&mut self, batch: RecordBatch) -> D2DResult<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        if !self.aligned {
            check_alignment(&self.target_schema, &self.table, &batch)?;
            self.aligned = true;
        }
        let rows = batch.num_rows();
        let stats = self
            .flight
            .load_table(
                &self.table,
                vec![batch],
                &self.load.mode,
                self.load.watermark.as_ref(),
            )
            .await?;
        if stats.rows != rows as u64 {
            return Err(D2DError::external(
                "qmdb",
                format!(
                    "装载摘要行数 {} 与送入行数 {rows} 不一致（sn={}）—— 数据面异常，中止",
                    stats.rows, stats.sn
                ),
            ));
        }
        self.confirmed_rows += stats.rows;
        debug!(
            table = %self.table,
            rows,
            files = stats.files,
            sn = stats.sn,
            "qmdb 装载提交完成"
        );
        Ok(rows)
    }

    /// 每次 `write_batch` 都已原子提交（失败内部已 abort），会话级 commit 无事可做
    async fn commit(&mut self) -> D2DResult<()> {
        info!(
            table = %self.table,
            confirmed_rows = self.confirmed_rows,
            "qmdb 会话提交（全部装载已原子确认）"
        );
        Ok(())
    }

    /// 失败路径：`write_batch` 内部任何失败都已经 abort 过，无悬挂会话
    async fn abort(&mut self) -> D2DResult<()> {
        debug!(table = %self.table, "qmdb 会话中止（无悬挂装载：失败已在 write_batch 内 abort）");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    fn target_schema(extra: Option<Field>) -> SchemaRef {
        let mut fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ];
        if let Some(e) = extra {
            fields.push(e);
        }
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_alignment_orphan_column_is_error() {
        // 批里有一列目标表上没有（typo）→ 服务端会悄悄扔掉它 → 必须报错
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("typo_name", DataType::Utf8, true),
        ]));
        let bad = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .unwrap();
        let e = check_alignment(&target_schema(None), "db.t", &bad)
            .unwrap_err()
            .to_string();
        assert!(e.contains("typo_name"), "{}", e);
        assert!(e.contains("静默丢弃"), "{}", e);
    }

    /// **回归**：大小写不同也算对上了 —— qmdb 折叠小写，`PHID` == `phid`
    #[test]
    fn test_alignment_case_insensitive_match_passes() {
        let target = target_schema(None);
        assert!(check_alignment(&target, "db.t", &batch()).is_ok());
    }

    #[test]
    fn test_alignment_missing_target_column_passes() {
        // 目标多一列（批里没有）→ 合法的部分写入，不报错（警告在日志里）
        let target = target_schema(Some(Field::new("extra", DataType::Int64, true)));
        assert!(check_alignment(&target, "db.t", &batch()).is_ok());
    }
}
