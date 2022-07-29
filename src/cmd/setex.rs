use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments, timestamp_from_ttl};
use crate::{is_use_txn_api, Connection, Frame};

use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct SetEX {
    /// the lookup key
    key: String,

    /// the value to be stored
    value: Bytes,

    /// When to expire the key
    expire: i64,

    valid: bool,
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
            valid: true,
        }
    }

    pub fn new_invalid() -> SetEX {
        SetEX {
            key: "".to_owned(),
            value: Bytes::new(),
            expire: 0,
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
    pub fn expire(&self) -> i64 {
        self.expire
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SetEX> {
        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the ttl to set.
        let uexpire = parse.next_int()?;

        let expire = uexpire as i64 * 1000;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        Ok(SetEX {
            key,
            value,
            expire,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<SetEX> {
        if argv.len() != 3 {
            return Ok(SetEX::new_invalid());
        }
        let key = argv[0].clone();
        let expire = argv[1].parse::<i64>();
        let value = Bytes::from(argv[2].clone());
        if let Ok(v) = expire {
            return Ok(SetEX::new(key, value, v * 1000));
        }
        Ok(SetEX::new_invalid())
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.setex(None).await.unwrap_or_else(Into::into);
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

    pub async fn setex(self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            let ts = timestamp_from_ttl(self.expire as u64);
            StringCommandCtx::new(txn)
                .do_async_txnkv_put(&self.key, &self.value, ts)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for SetEX {
    fn new_invalid() -> SetEX {
        SetEX {
            key: "".to_owned(),
            value: Bytes::new(),
            expire: 0,
            valid: false,
        }
    }
}
