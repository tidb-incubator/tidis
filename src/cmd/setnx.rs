use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame};

use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

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
#[derive(Debug, Clone)]
pub struct SetNX {
    /// the lookup key
    key: String,

    /// the value to be stored
    value: Bytes,

    valid: bool,
}

impl SetNX {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: impl ToString, value: Bytes) -> SetNX {
        SetNX {
            key: key.to_string(),
            value,
            valid: true,
        }
    }

    pub fn new_invalid() -> SetNX {
        SetNX {
            key: "".to_owned(),
            value: Bytes::new(),
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SetNX> {
        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        Ok(SetNX {
            key,
            value,
            valid: true,
        })
    }

    pub fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<SetNX> {
        if argv.len() != 2 {
            return Ok(SetNX::new_invalid());
        }
        let key = String::from_utf8_lossy(&argv[0]).to_string();
        let value = argv[1].clone();

        Ok(SetNX {
            key,
            value,
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.put_not_exists(None).await.unwrap_or_else(Into::into);

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

    pub async fn put_not_exists(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_put_not_exists(&self.key, &self.value, true, false)
                .await
        } else {
            StringCommandCtx::new(txn)
                .do_async_rawkv_put_not_exists(&self.key, &self.value)
                .await
        }
    }
}

impl Invalid for SetNX {
    fn new_invalid() -> SetNX {
        SetNX {
            key: "".to_owned(),
            value: Bytes::new(),
            valid: false,
        }
    }
}
