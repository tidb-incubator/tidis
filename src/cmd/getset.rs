use std::sync::Arc;

use crate::config::is_use_txn_api;
use crate::config::LOGGER;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame, Parse};
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use super::Invalid;

/// Atomically sets key to value and returns the old value stored at key.
#[derive(Debug, Clone)]
pub struct Getset {
    /// Name of the key to get and set
    key: String,

    /// Value to be stored
    value: Bytes,

    valid: bool,
}

impl Getset {
    /// Create a new `Getset` command which fetches and sets `key`.
    pub fn new(key: impl ToString, value: Bytes) -> Getset {
        Getset {
            key: key.to_string(),
            value,
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Getset` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GETSET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Getset` value on success. If the frame is malformed, `Err`
    /// is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GETSET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Getset> {
        // The `GETSET` string has already been consumed. The next value is the
        // name of the key to get and set. If the next value is not a string
        // or the input is fully consumed, then an error is returned.
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        Ok(Getset::new(key, value))
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Getset> {
        if argv.len() != 2 {
            return Ok(Getset::new_invalid());
        }

        let key = &String::from_utf8_lossy(&argv[0]);

        let value = argv[1].clone();

        Ok(Getset::new(key, value))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.getset(None).await.unwrap_or_else(Into::into);

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

    pub async fn getset(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_put(&self.key, &self.value, 0, true) // 'get' set to true
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Getset {
    fn new_invalid() -> Getset {
        Getset {
            key: "".to_owned(),
            value: Bytes::new(),
            valid: false,
        }
    }
}
