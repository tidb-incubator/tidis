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

/// Get the value of key and delete the key.
///
/// This command is similar to GET, except for the fact that it also deletes the
/// key on success (if and only if the key's value type is a string).
#[derive(Debug, Clone)]
pub struct Getdel {
    /// Name of the key to get and delete
    key: String,

    valid: bool,
}

impl Getdel {
    /// Create a new `Getdel` command which fetches and deletes `key`.
    pub fn new(key: impl ToString) -> Getdel {
        Getdel {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Getdel` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GETDEL` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Getdel` value on success. If the frame is malformed, `Err`
    /// is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GETDEL key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Getdel> {
        // The `GETDEL` string has already been consumed. The next value is the
        // name of the key to get and delete. If the next value is not a string
        // or the input is fully consumed, then an error is returned.
        let key = parse.next_string()?;

        Ok(Getdel::new(key))
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Getdel> {
        if argv.len() != 1 {
            return Ok(Getdel::new_invalid());
        }

        let key = &String::from_utf8_lossy(&argv[0]);

        Ok(Getdel::new(key))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.getdel(None).await.unwrap_or_else(Into::into);

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

    pub async fn getdel(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_string_del(&self.key, true)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Getdel {
    fn new_invalid() -> Getdel {
        Getdel {
            key: "".to_owned(),
            valid: false,
        }
    }
}
