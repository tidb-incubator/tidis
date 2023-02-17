use crate::cmd::{Parse, ParseError};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments, timestamp_from_ttl};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;
use std::sync::Arc;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use super::Invalid;

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
/// * NX -- Only set the key if it does not already exist.
/// * XX -- Only set the key if it already exist.
/// * GET -- Return the old string stored at key, or nil if key did not exist.
///          Errors if value stored at key is not a string.
/// * EX `seconds` -- Set the specified expire time, in seconds.
/// * PX `milliseconds` -- Set the specified expire time, in milliseconds.
#[derive(Debug, Clone)]
pub struct Set {
    /// Lookup key
    key: String,

    /// Value to be stored
    value: Bytes,

    /// Key expires after `expire` milliseconds
    expire: Option<i64>,

    /// NX = false, XX = true
    exists: Option<bool>,

    /// Return old string stored at key, or nil
    get: bool,

    valid: bool,
}

impl Set {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: impl ToString, value: Bytes) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire: None,
            exists: None,
            get: false,
            valid: true,
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

    pub fn set_expire(&mut self, expire: i64) -> crate::Result<()> {
        if self.expire.is_none() {
            Ok(self.expire = Some(expire))
        } else {
            Err("Multiple expiry options".into())
        }
    }

    pub fn set_exists(&mut self, exists: bool) -> crate::Result<()> {
        if self.exists.is_none() {
            Ok(self.exists = Some(exists))
        } else {
            Err("Multiple existence options".into())
        }
    }

    pub fn set_get(&mut self, get: bool) {
        self.get = get
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
    /// SET key value [NX | XX] [GET] [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        let mut set = Set::new(&key, value);

        // Attempt to parse option flags.
        loop {
            match parse.next_string() {
                Ok(s) => match s.to_uppercase().as_str() {
                    // [NX | XX]
                    "NX" => {
                        // Only set the key if it does not already exist.
                        set.set_exists(false)?
                    }
                    "XX" => {
                        // Only set the key if it already exists.
                        set.set_exists(true)?
                    }
                    // [GET]
                    "GET" => {
                        // Return overwritten value, or nil.
                        set.set_get(true)
                    }
                    // [EX s| PX ms]
                    "EX" => {
                        // TTL specified in seconds. The next value is an integer.
                        let secs = parse.next_int()?;
                        set.set_expire(secs * 1000)?
                    }
                    "PX" => {
                        // TTL specified in milliseconds. The next value is an integer.
                        let ms = parse.next_int()?;
                        set.set_expire(ms)?
                    }
                    _ => {
                        return Err(
                            "currently `SET` only supports [NX | XX] [GET] [EX s | PX ms]".into(),
                        )
                    }
                },
                // The `EndOfStream` error indicates there is no further data to
                // parse.
                Err(EndOfStream) => break,
                // All other errors are bubbled up, resulting in the connection
                // being terminated.
                Err(err) => return Err(err.into()),
            }
        }

        Ok(set)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Set> {
        if argv.len() < 2 {
            return Ok(Set::new_invalid());
        }
        let key = String::from_utf8_lossy(&argv[0]).to_string();
        let value = argv[1].clone();

        let mut set = Set::new(&key, value);

        let mut idx = 2;
        loop {
            if idx >= argv.len() {
                break;
            }
            match String::from_utf8_lossy(&argv[idx]).to_uppercase().as_str() {
                "NX" => {
                    if set.set_exists(false).is_err() {
                        return Ok(Set::new_invalid());
                    }
                }
                "XX" => {
                    if set.set_exists(true).is_err() {
                        return Ok(Set::new_invalid());
                    }
                }
                "GET" => set.set_get(true),
                "EX" => {
                    idx += 1;
                    match String::from_utf8_lossy(&argv[idx]).parse::<i64>() {
                        Ok(secs) if set.set_expire(secs * 1000).is_ok() => (),
                        _ => return Ok(Set::new_invalid()),
                    }
                }
                "PX" => {
                    idx += 1;
                    match String::from_utf8_lossy(&argv[idx]).parse::<i64>() {
                        Ok(ms) if set.set_expire(ms).is_ok() => (),
                        _ => return Ok(Set::new_invalid()),
                    }
                }
                _ => return Ok(Set::new_invalid()),
            }

            idx += 1;
        }

        Ok(set)
    }

    /// Apply the `Set` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.set(None).await?;

        debug!(
            LOGGER,
            "res, {} -> {}, {:?}",
            dst.local_addr(),
            dst.peer_addr(),
            response
        );
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) async fn set(self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        Ok(match self.exists {
            Some(true) => self.put_if_exists(txn).await,
            Some(false) => self.put_not_exists(txn).await,
            None => self.put(txn).await,
        }
        .unwrap_or_else(Into::into))
    }

    async fn put_not_exists(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        // TODO: TTL
        if self.expire.is_some() {
            return Ok(resp_err("EX/PX with NX/XX not implemented".into()));
        };

        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_put_not_exists(&self.key, &self.value, false, self.get)
                .await
        } else {
            StringCommandCtx::new(txn)
                .do_async_rawkv_put_not_exists(&self.key, &self.value)
                .await
        }
    }

    async fn put_if_exists(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        // TODO: TTL
        if self.expire.is_some() {
            return Ok(resp_err("EX/PX with NX/XX not implemented".into()));
        };

        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_put_if_exists(&self.key, &self.value, self.get)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }

    async fn put(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            let ts = self.expire.map_or(0, |v| timestamp_from_ttl(v as u64));
            StringCommandCtx::new(txn)
                .do_async_txnkv_put(&self.key, &self.value, ts, self.get)
                .await
        } else {
            StringCommandCtx::new(txn)
                .do_async_rawkv_put(&self.key, &self.value)
                .await
        }
    }
}

impl Invalid for Set {
    fn new_invalid() -> Set {
        Set {
            key: "".to_owned(),
            value: Bytes::new(),
            expire: None,
            exists: None,
            get: false,
            valid: false,
        }
    }
}
