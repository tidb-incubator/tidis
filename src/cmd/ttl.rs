use std::sync::Arc;

use crate::config::is_use_txn_api;
use crate::config::LOGGER;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame, Parse};
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct TTL {
    key: String,
    valid: bool,
}

impl TTL {
    pub fn new(key: impl ToString) -> TTL {
        TTL {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<TTL> {
        let key = parse.next_string()?;

        Ok(TTL { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<TTL> {
        if argv.len() != 1 {
            return Ok(TTL {
                key: "".to_owned(),
                valid: false,
            });
        }
        return Ok(TTL {
            key: argv[0].to_owned(),
            valid: true,
        });
    }

    pub(crate) async fn apply(self, dst: &mut Connection, is_millis: bool) -> crate::Result<()> {
        let response = match self.ttl(is_millis, None).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

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

    pub async fn ttl(
        self,
        is_millis: bool,
        txn: Option<Arc<Mutex<Transaction>>>,
    ) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_ttl(&self.key, is_millis)
                .await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
