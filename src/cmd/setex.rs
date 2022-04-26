use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::{timestamp_from_ttl, resp_err};
use crate::{Connection, Frame, is_use_txn_api};

use bytes::Bytes;
use tracing::{debug, instrument};

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
pub struct SetEX {
    /// the lookup key
    key: String,

    /// the value to be stored
    value: Bytes,

    /// When to expire the key
    expire: i64,
}

impl SetEX {
    /// Create a new `SetEX` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: impl ToString, value: Bytes, expire: i64) -> SetEX {
        SetEX {
            key: key.to_string(),
            value,
            expire,
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
    pub fn expire(&self) -> i64 {
        self.expire
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SetEX> {

        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the ttl to set.
        let uexpire = parse.next_int()?;

        let expire = uexpire as i64;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        Ok(SetEX { key, value, expire })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.setex().await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn setex(self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            let ts = timestamp_from_ttl(self.expire as u64);
            StringCommandCtx::new(None).do_async_txnkv_put(&self.key, &self.value, ts).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
