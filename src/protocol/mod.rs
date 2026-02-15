use std::fmt;

use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;

// A minimal representation of RESP2 values.
//
// This enum is intentionally small to keep the teaching surface clear.
// It will grow over time (for example, to support arrays of values),
// but even this subset is enough to frame how parsing and encoding work.
#[derive(Debug, Clone)]
pub enum RespValue {
    // +OK\r\n
    SimpleString(String),
    // -ERR Some message\r\n
    Error(String),
    // :123\r\n
    Integer(i64),
    // $3\r\nfoo\r\n or $-1\r\n (nil bulk string)
    BulkString(Option<Vec<u8>>),
}

// Convenience constructor for a simple string response.
pub fn simple_string(s: impl Into<String>) -> RespValue {
    RespValue::SimpleString(s.into())
}

// Errors specific to RESP parsing and encoding.
//
// Keeping a dedicated error type here isolates protocol issues from
// transport-level or application-level errors, making debugging and
// testing easier.
#[derive(Error, Debug)]
pub enum RespError {
    #[error("incomplete frame")]
    Incomplete,

    #[error("malformed frame: {0}")]
    Malformed(&'static str),
}

// Attempt to parse a single RESP frame from the buffer.
//
// - On success with a complete frame, returns `Ok(Some(RespValue))`
//   and removes the bytes for that frame from `buf`.
// - If there are not enough bytes yet for a full frame, returns
//   `Ok(None)` and leaves `buf` unchanged.
// - On a malformed frame, returns `Err(RespError)`.
//
// Using `BytesMut` here is convenient because it supports efficient
// splitting of the front of the buffer (`split_to`), which matches
// how line- and length-delimited protocols typically operate.
pub fn try_parse(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // Look at the first byte to decide what kind of RESP value we have.
    let prefix = buf[0];
    match prefix {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        _ => Err(RespError::Malformed("unknown RESP prefix")),
    }
}

// Parse a simple string: +OK\r\n
fn parse_simple_string(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Find the position of the first CRLF after the '+'.
    if let Some((line, consumed)) = read_line(&buf[1..]) {
        // `split_to` consumes bytes from the front of the buffer.
        // We want to remove the '+' plus the line bytes and the CRLF.
        buf.split_to(1 + consumed);
        let s = String::from_utf8(line.to_vec()).map_err(|_| RespError::Malformed("invalid utf-8"))?;
        Ok(Some(RespValue::SimpleString(s)))
    } else {
        Ok(None)
    }
}

// Parse an error string: -ERR message\r\n
fn parse_error(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    if let Some((line, consumed)) = read_line(&buf[1..]) {
        buf.split_to(1 + consumed);
        let s = String::from_utf8(line.to_vec()).map_err(|_| RespError::Malformed("invalid utf-8"))?;
        Ok(Some(RespValue::Error(s)))
    } else {
        Ok(None)
    }
}

// Parse an integer: :123\r\n
fn parse_integer(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    if let Some((line, consumed)) = read_line(&buf[1..]) {
        buf.split_to(1 + consumed);
        let s = std::str::from_utf8(line).map_err(|_| RespError::Malformed("invalid integer utf-8"))?;
        let value = s.parse::<i64>().map_err(|_| RespError::Malformed("invalid integer"))?;
        Ok(Some(RespValue::Integer(value)))
    } else {
        Ok(None)
    }
}

// Parse a bulk string: $3\r\nfoo\r\n or $-1\r\n (nil)
fn parse_bulk_string(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // First read the length line after '$'.
    if let Some((line, consumed)) = read_line(&buf[1..]) {
        let len_str = std::str::from_utf8(line).map_err(|_| RespError::Malformed("invalid bulk length utf-8"))?;
        let len: i64 = len_str.parse().map_err(|_| RespError::Malformed("invalid bulk length"))?;

        // Remove the '$' plus the length line and its CRLF from the buffer.
        buf.split_to(1 + consumed);

        if len < 0 {
            // The special length -1 denotes a nil bulk string.
            return Ok(Some(RespValue::BulkString(None)));
        }

        let len = len as usize;

        // We now expect `len` bytes of data followed by CRLF.
        if buf.len() < len + 2 {
            // Not enough bytes yet; wait for more data.
            return Ok(None);
        }

        // Split out the data bytes.
        let data = buf.split_to(len);

        // Consume the trailing CRLF.
        if buf.len() < 2 {
            return Err(RespError::Incomplete);
        }
        let cr = buf.get_u8();
        let lf = buf.get_u8();
        if cr != b'\r' || lf != b'\n' {
            return Err(RespError::Malformed("bulk string missing CRLF"));
        }

        Ok(Some(RespValue::BulkString(Some(data.to_vec()))))
    } else {
        Ok(None)
    }
}

// Helper: read a single line terminated by CRLF from a byte slice.
//
// Returns:
// - `Some((line, consumed))` where `line` is the slice without CRLF,
//   and `consumed` is the number of bytes including the CRLF.
// - `None` if CRLF has not yet arrived (incomplete line).
fn read_line(buf: &[u8]) -> Option<(&[u8], usize)> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            // Line is everything before CRLF.
            let line = &buf[..i];
            let consumed = i + 2;
            return Some((line, consumed));
        }
    }
    None
}

// Encode a RESP value into a `Bytes` buffer ready to be written to a socket.
//
// For teaching purposes we focus on clarity over micro-optimizations.
pub fn encode(val: &RespValue) -> Bytes {
    let mut out = Vec::new();

    match val {
        RespValue::SimpleString(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(Some(data)) => {
            out.push(b'$');
            out.extend_from_slice(data.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(None) => {
            // Nil bulk string: $-1\r\n
            out.extend_from_slice(b"$-1\r\n");
        }
    }

    Bytes::from(out)
}

