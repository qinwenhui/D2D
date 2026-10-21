//! 文本行扫描 / 字段切分
//!
//! 语义来源（手册 3.3 / 2.3 / 3.9）：
//! - 行分隔符固定 LF；转义符规则：导出时相邻两个转义符表示一个字面转义符，
//!   本模块按对称规则反转义（escape + X → 字面 X）
//! - NULL_MODE=true：空值导出为 NULL 标记串；false：空串表示空值
//! - 数值/时间列的字段是 ASCII；字符串列按 character_code 解码为 UTF-8
//!
//! # 这里只留"dmfldr 怎么切行/切字段"
//!
//! 单元格**怎么按类型解析**（DECIMAL 校验、时间格式、编码解码、NULL 判定）
//! 已经上移到 [`d2d_types::text_batch`] —— 那条规则三条读取路径（dmfldr / ODBC /
//! Doris MySQL 协议口）共用一份。本模块只负责把字节流切成字段，
//! 切法才是 dmfldr 自己的事（转义符、多字节分隔符、CRLF）。
//! 移到那里的符号在下面原样转出，调用方不必改引用路径。

use std::borrow::Cow;
use std::io::{BufRead, BufReader, Read};

// 单元格解析：实现在 d2d_types::text_batch，这里转出给本模块的调用方
pub use d2d_types::text_batch::{
    decode_text, is_null_field, parse_bool, parse_decimal_i128, parse_lob_ref, read_lob_bytes,
    trimmed_str,
};

/// 读一个"逻辑行"（escape 感知的 LF 扫描）
///
/// 返回读入的字节数（含末尾 LF；EOF 无换行时也可能 > 0）。
/// escape 后的 LF 视为数据而非行终止符。
pub fn read_logical_line<R: BufRead>(
    r: &mut R,
    escape: Option<&[u8]>,
    out: &mut Vec<u8>,
) -> std::io::Result<usize> {
    // 空转义符等价于不转义（防御：空切片会让 starts_with 恒真）
    let Some(esc) = escape.filter(|e| !e.is_empty()) else {
        return r.read_until(b'\n', out);
    };

    // ⚠️ 必须用 fill_buf/consume，**不能用本地数组 read()**。
    // 历史 bug：曾用 `let mut scratch = [0u8; 16*1024]; r.read(&mut scratch)` 一次性读入
    // 本地缓冲，命中第一个 `\n` 就 return —— 缓冲区里 `\n` 之后的字节被**静默丢弃**，
    // 底层读取位置却已跳过整块。后果是下一"行"从**行中间**开始，字段数校验必然失败
    // （实测：8.9GB 文件，第 1 行正常、第 2 行起全是半截行，报 field count 18/25/32…）。
    // escape=None 走 read_until 不受影响，所以单元测试（默认不转义 + 小输入）全绿也漏掉了它。
    // fill_buf 只借出内部缓冲、consume 精确归还已消费部分，未消费字节留在 reader 里。
    let mut pending_literal = false;
    loop {
        let (used, done) = {
            let buf = match r.fill_buf() {
                Ok(b) => b,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            if buf.is_empty() {
                return Ok(out.len()); // EOF
            }
            let mut used = 0usize;
            let mut done = false;
            while used < buf.len() {
                let rest = &buf[used..];
                if pending_literal {
                    out.push(rest[0]);
                    pending_literal = false;
                    used += 1;
                    continue;
                }
                if rest.starts_with(esc) {
                    out.extend_from_slice(esc);
                    used += esc.len();
                    if rest.len() == esc.len() {
                        // 转义符正好压在缓冲区末尾，后随字节留到下一轮判定
                        pending_literal = true;
                        break;
                    }
                    out.push(rest[esc.len()]); // 转义符后随字节 = 字面量
                    used += 1;
                    continue;
                }
                if rest[0] == b'\n' {
                    out.push(b'\n');
                    used += 1;
                    done = true;
                    break;
                }
                out.push(rest[0]);
                used += 1;
            }
            (used, done)
        };
        r.consume(used);
        if done {
            return Ok(out.len());
        }
    }
}

/// 按分隔符切分一行字段（escape 感知，做反转义）
pub fn split_fields<'a>(line: &'a [u8], sep: &[u8], escape: Option<&[u8]>) -> Vec<Cow<'a, [u8]>> {
    let Some(esc) = escape else {
        return match sep.len() {
            1 => line.split(|&b| b == sep[0]).map(Cow::Borrowed).collect(),
            _ => split_multibyte_nocescape(line, sep),
        };
    };
    // escape 路径：逐字段构建 owned 缓冲
    let mut fields: Vec<Cow<[u8]>> = Vec::new();
    let mut cur: Vec<u8> = Vec::with_capacity(32);
    let mut i = 0;
    while i < line.len() {
        if line[i..].starts_with(esc) {
            i += esc.len();
            if i < line.len() {
                cur.push(line[i]);
                i += 1;
            }
            continue;
        }
        if line[i..].starts_with(sep) {
            fields.push(Cow::Owned(std::mem::take(&mut cur)));
            i += sep.len();
            continue;
        }
        cur.push(line[i]);
        i += 1;
    }
    fields.push(Cow::Owned(cur));
    fields
}

fn split_multibyte_nocescape<'a>(line: &'a [u8], sep: &[u8]) -> Vec<Cow<'a, [u8]>> {
    let mut fields = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i + sep.len() <= line.len() {
        if line[i..].starts_with(sep) {
            fields.push(Cow::Borrowed(&line[start..i]));
            i += sep.len();
            start = i;
        } else {
            i += 1;
        }
    }
    fields.push(Cow::Borrowed(&line[start..]));
    fields
}

/// 带字节上限的读取辅助（供 header 读取复用）
pub fn take_first_line<R: Read>(r: R) -> std::io::Result<Vec<u8>> {
    let mut br = BufReader::new(r);
    let mut buf = Vec::new();
    br.read_until(b'\n', &mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_logical_line_plain() {
        let data = b"a|b\nc|d\n".to_vec();
        let mut r = std::io::Cursor::new(data);
        let mut out = Vec::new();
        let n = read_logical_line(&mut r, None, &mut out).unwrap();
        assert_eq!(n, 4);
        assert_eq!(out, b"a|b\n");
    }

    #[test]
    fn test_read_logical_line_escaped_lf_is_data() {
        // escape='*'：导出串 "a|*\nb|c" 中的 LF 是转义后的数据，不终止行
        let data = b"a|*\nb|c\n".to_vec();
        let mut r = std::io::Cursor::new(data);
        let mut out = Vec::new();
        let _ = read_logical_line(&mut r, Some(b"*"), &mut out).unwrap();
        assert_eq!(
            out, b"a|*\nb|c\n",
            "escaped LF must not stop the logical line"
        );
    }

    /// 回归：开启转义时**不得丢弃已读入缓冲的后续行**。
    ///
    /// 旧实现用本地 16KB 数组 `read()` 一次读入，命中首个 `\n` 就 return，
    /// 同一批读到的后续字节被静默丢弃 —— 在大文件上表现为「第 1 行正常、
    /// 第 2 行起全是半截行」（实测 8.9GB 文件报 field count 18/25/32…）。
    /// 本用例用「一次能读完的小输入 + 开转义」精确复现该场景。
    #[test]
    fn test_escape_does_not_drop_buffered_lines() {
        let data = b"a|1\nbb|22\nccc|333\n".to_vec();
        let mut r = std::io::Cursor::new(data);
        let mut got: Vec<Vec<u8>> = Vec::new();
        for _ in 0..3 {
            let mut out = Vec::new();
            let n = read_logical_line(&mut r, Some(b"*"), &mut out).unwrap();
            assert!(n > 0, "行被丢弃了：第 {} 次调用返回 0", got.len() + 1);
            got.push(out);
        }
        assert_eq!(got[0], b"a|1\n");
        assert_eq!(got[1], b"bb|22\n");
        assert_eq!(got[2], b"ccc|333\n");
    }

    /// 小容量 BufReader 强制多次 fill_buf，覆盖「转义符压在缓冲边界」的情况
    #[test]
    fn test_escape_across_buffer_refills() {
        let data = b"aaaa|bbbb\ncccc|*dddd\neeee|ffff\n".to_vec();
        let mut r = std::io::BufReader::with_capacity(4, std::io::Cursor::new(data));
        let mut lines: Vec<Vec<u8>> = Vec::new();
        loop {
            let mut out = Vec::new();
            if read_logical_line(&mut r, Some(b"*"), &mut out).unwrap() == 0 {
                break;
            }
            lines.push(out);
        }
        assert_eq!(lines.len(), 3, "得到的行: {:?}", lines);
        assert_eq!(lines[0], b"aaaa|bbbb\n");
        assert_eq!(lines[1], b"cccc|*dddd\n");
        assert_eq!(lines[2], b"eeee|ffff\n");
    }

    #[test]
    fn test_split_fields_plain() {
        let f = split_fields(b"1|a|2.5", b"|", None);
        assert_eq!(f.len(), 3);
        assert_eq!(&f[0][..], b"1");
        assert_eq!(&f[2][..], b"2.5");
    }

    #[test]
    fn test_split_fields_escape_unescape() {
        // "t**1" → "t*1"；"a*|b" → "a|b"
        let f = split_fields(b"t**1|a*|b", b"|", Some(b"*"));
        assert_eq!(f.len(), 2);
        assert_eq!(&f[0][..], b"t*1");
        assert_eq!(&f[1][..], b"a|b");
    }

    #[test]
    fn test_split_fields_multibyte_sep() {
        let f = split_fields("a\u{1}b\u{1}c".as_bytes(), "\u{1}".as_bytes(), None);
        assert_eq!(f.len(), 3);
        assert_eq!(&f[1][..], "b".as_bytes());
    }
}
