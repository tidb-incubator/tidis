use std::sync::Arc;

use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct DecrBy {
    key: String,
    step: i64,
    valid: bool,
}

impl DecrBy {
    pub fn new(key: impl ToString, step: i64) -> DecrBy {
        DecrBy {
            key: key.to_string(),
            step,
            valid: true,
        }
    }

    pub fn new_invalid() -> DecrBy {
        DecrBy {
            key: "".to_owned(),
            step: 0,
            valid: false,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse, single_step: bool) -> crate::Result<DecrBy> {
        let key = parse.next_string()?;
        let step = if single_step { 1 } else { parse.next_int()? };
        Ok(DecrBy {
            key,
            step,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>, single_step: bool) -> crate::Result<DecrBy> {
        if (single_step && argv.len() != 1) || (!single_step && argv.len() != 2) {
            return Ok(DecrBy::new_invalid());
        }
        let key = &argv[0];
        let step = if single_step {
            Ok(1)
        } else {
            argv[1].parse::<i64>()
        };

        match step {
            Ok(step) => Ok(DecrBy::new(key, step)),
            Err(_) => Ok(DecrBy::new_invalid()),
        }
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.decr_by(None).await.unwrap_or_else(Into::into);

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

    pub async fn decr_by(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_incr(&self.key, false, self.step)
                .await
        } else {
            StringCommandCtx::new(None)
                .do_async_rawkv_incr(&self.key, false, self.step)
                .await
        }
    }
}
