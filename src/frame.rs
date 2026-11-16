//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use crate::tikv::errors::RTError;
use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// Inbound limits, applied per frame on both TCP and TLS before authentication.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BULK_BYTES: usize = 8 * 1024 * 1024;
/// Total declared array elements across the entire frame, including nested arrays.
pub const MAX_ARRAY_ELEMENTS: usize = 65_536;
pub const MAX_ARRAY_DEPTH: usize = 32;

/// A frame in the Redis protocol.
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    ErrorOwned(String),
    ErrorString(&'static str),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl Frame {
    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Push a "bulk" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Push an "integer" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_int(&mut self, value: i64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        Self::decode::<false>(src).map(|_| ())
    }

    /// Decode a frame using the same validation and budgets as `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        Self::decode::<true>(src)
    }

    fn decode<const BUILD: bool>(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        let start: usize = src.position().try_into()?;
        let data = src
            .get_ref()
            .get(start..)
            .ok_or("protocol error; invalid cursor position")?;
        // Limit only this frame; trailing pipelined frames have separate budgets.
        let mut bounded = Cursor::new(&data[..data.len().min(MAX_FRAME_BYTES)]);
        let mut elements_left = MAX_ARRAY_ELEMENTS;
        let result = Self::decode_inner::<BUILD>(&mut bounded, 0, &mut elements_left);
        if matches!(result, Err(Error::Incomplete)) && data.len() >= MAX_FRAME_BYTES {
            return Err("protocol error; frame byte limit exceeded".into());
        }
        src.set_position((start + bounded.position() as usize) as u64);
        result
    }

    // BUILD=false validates without allocating frame payloads or array storage.
    fn decode_inner<const BUILD: bool>(
        src: &mut Cursor<&[u8]>,
        depth: usize,
        elements_left: &mut usize,
    ) -> Result<Frame, Error> {
        match get_u8(src)? {
            kind @ (b'+' | b'-') => {
                let line = std::str::from_utf8(get_line(src)?)
                    .map_err(|_| "protocol error; invalid UTF-8 line")?;
                Ok(if !BUILD {
                    Frame::Null
                } else if kind == b'+' {
                    Frame::Simple(line.to_owned())
                } else {
                    Frame::ErrorOwned(line.to_owned())
                })
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                let len = match get_length(src, MAX_BULK_BYTES)? {
                    Some(len) => len,
                    None => return Ok(Frame::Null),
                };
                let start = src.position() as usize;
                // len is already bounded, so this addition cannot overflow.
                if start + len + 2 > MAX_FRAME_BYTES {
                    return Err("protocol error; frame byte limit exceeded".into());
                }
                skip(src, len)?;
                expect_byte(src, b'\r')?;
                expect_byte(src, b'\n')?;
                Ok(if BUILD {
                    Frame::Bulk(Bytes::copy_from_slice(&src.get_ref()[start..start + len]))
                } else {
                    Frame::Null
                })
            }
            b'*' => {
                if depth >= MAX_ARRAY_DEPTH {
                    return Err("protocol error; array nesting limit exceeded".into());
                }
                let len = match get_length(src, *elements_left)? {
                    Some(len) => len,
                    None => return Ok(Frame::Null),
                };
                *elements_left -= len;
                // Grow only as children arrive; never reserve an advertised length.
                let mut out = Vec::new();
                for _ in 0..len {
                    let child = Self::decode_inner::<BUILD>(src, depth + 1, elements_left)?;
                    if BUILD {
                        out.push(child);
                    }
                }
                Ok(Frame::Array(out))
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::ErrorOwned(msg) => write!(fmt, "error: {}", msg),
            Frame::ErrorString(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " ")?;
                        part.fmt(fmt)?;
                    }
                }

                Ok(())
            }
        }
    }
}

impl From<RTError> for Frame {
    fn from(e: RTError) -> Self {
        match e {
            RTError::Owned(s) => Frame::ErrorOwned(s),
            RTError::String(s) => Frame::ErrorString(s),
            RTError::TikvClient(_) => Frame::ErrorString("ERR tikv client error"),
        }
    }
}

fn expect_byte(src: &mut Cursor<&[u8]>, expected: u8) -> Result<(), Error> {
    if get_u8(src)? != expected {
        return Err("protocol error; expected CRLF".into());
    }
    Ok(())
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, Error> {
    let line = get_line(src)?;
    let digits = match line.first() {
        Some(b'+' | b'-') => &line[1..],
        _ => line,
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err("protocol error; invalid integer".into());
    }
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "protocol error; integer out of range".into())
}

fn get_length(src: &mut Cursor<&[u8]>, limit: usize) -> Result<Option<usize>, Error> {
    let line = get_line(src)?;
    if line == b"-1" {
        return Ok(None);
    }
    if line.is_empty() || !line.iter().all(u8::is_ascii_digit) {
        return Err("protocol error; invalid length".into());
    }
    let len = std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n <= limit)
        .ok_or("protocol error; length limit exceeded")?;
    Ok(Some(len))
}

/// Find a line
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Scan the bytes directly
    let start = src.position() as usize;
    let data = *src.get_ref();
    for i in start..data.len() {
        match data[i] {
            b'\r' => {
                match data.get(i + 1) {
                    Some(b'\n') => {}
                    Some(_) => return Err("protocol error; expected CRLF".into()),
                    None => return Err(Error::Incomplete),
                }
                src.set_position((i + 2) as u64);
                return Ok(&data[start..i]);
            }
            b'\n' => return Err("protocol error; expected CRLF".into()),
            _ => {}
        }
    }

    Err(Error::Incomplete)
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

#[cfg(test)]
#[path = "frame_tests.rs"]
mod tests;
