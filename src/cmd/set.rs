use crate::cmd::{Parse, ParseError};
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{timestamp_from_ttl, resp_invalid_arguments};

use bytes::Bytes;
use tikv_client::Transaction;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::config::LOGGER;
use slog::debug;

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
///
/// # Options
///
/// Currently, the following options are supported:
///
/// * EX `seconds` -- Set the specified expire time, in seconds.
/// * PX `milliseconds` -- Set the specified expire time, in milliseconds.
#[derive(Debug)]
pub struct Set {
    /// the lookup key
    key: String,

    /// the value to be stored
    value: Bytes,

    /// When to expire the key
    expire: Option<i64>,

    /// Set if key is not present
    nx: Option<bool>,

    valid: bool,
}

impl Set {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: impl ToString, value: Bytes, expire: Option<i64>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
            nx: None,
            valid: true,
        }
    }

    pub fn new_invalid() -> Set {
        Set {
            key: "".to_owned(),
            value: Bytes::new(),
            expire: None,
            nx: None,
            valid: false,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the value
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Get the expire
    pub fn expire(&self) -> Option<i64> {
        self.expire
    }

    /// Parse a `Set` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Set` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least 3 entries.
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        // The expiration is optional. If nothing else follows, then it is
        // `None`.
        let mut expire = None;

        let mut nx = None;

        // Attempt to parse another string.
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // An expiration is specified in seconds. The next value is an
                // integer.
                let secs = parse.next_int()?;
                expire = Some(secs * 1000);
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // An expiration is specified in milliseconds. The next value is
                // an integer.
                let ms = parse.next_int()?;
                expire = Some(ms);
            }
            Ok(s) if s.to_uppercase() == "NX" => {
                // Only set if key not present
                nx = Some(true);
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            // The `EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `SET` options.
            Err(EndOfStream) => {}
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire, nx, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Set> {
        if argv.len() < 2 {
            return Ok(Set::new_invalid());
        }
        let key = argv[0].clone();
        let value = Bytes::from(argv[1].clone());
        let mut expire = None;
        let mut nx = None;
        for mut idx in 2..argv.len() {
            if argv[idx].to_uppercase() == "EX" {
                idx += 1;
                let secs = argv[idx].parse::<i64>();
                if let Ok(v) = secs {
                    expire = Some(v * 1000);
                } else {
                    return Ok(Set::new_invalid());
                }
            } else if argv[idx].to_uppercase() == "PX" {
                idx += 1;
                let ms = argv[idx].parse::<i64>();
                if let Ok(v) = ms {
                    expire = Some(v);
                }
            } else if argv[idx].to_uppercase() == "NX" {
                nx = Some(true);
            } else {
                return Ok(Set::new_invalid());
            }
        }
        Ok(Set { key, value, expire, nx, valid: true })
    }

    /// Apply the `Set` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = match self.nx {
            Some(_) => {
                match self.put_not_exists(None).await {
                    Ok(val) => val,
                    Err(e) => Frame::Error(e.to_string()),
                }
            },
            None => {
                match self.put(None).await {
                    Ok(val) => val,
                    Err(e) => Frame::Error(e.to_string()),
                }
            },
        };
        debug!(LOGGER, "res, {} -> {}, {:?}", dst.local_addr(), dst.peer_addr(), response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) async fn set(self, txn: Option<Arc<Mutex<Transaction>>>) ->  AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let response = match self.nx {
            Some(_) => {
                match self.put_not_exists(txn).await {
                    Ok(val) => val,
                    Err(e) => Frame::Error(e.to_string()),
                }
            },
            None => {
                match self.put(txn).await {
                    Ok(val) => val,
                    Err(e) => Frame::Error(e.to_string()),
                }
            },
        };
        Ok(response)
    }

    async fn put_not_exists(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            StringCommandCtx::new(txn).do_async_txnkv_put_not_exists(&self.key, &self.value).await
        } else {
            StringCommandCtx::new(txn).do_async_rawkv_put_not_exists(&self.key, &self.value).await
        }
    }

    async fn put(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        let mut ts = 0;
        if is_use_txn_api() {
            if self.expire.is_some() {
                ts = timestamp_from_ttl(self.expire.unwrap() as u64);
            }
            StringCommandCtx::new(txn).do_async_txnkv_put(&self.key, &self.value, ts).await
        } else {
            StringCommandCtx::new(txn).do_async_rawkv_put(&self.key, &self.value).await
        }
    }
}
