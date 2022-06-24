use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Zscore {
    key: String,
    member: String,
    valid: bool,
}

impl Zscore {
    pub fn new(key: &str, member: &str) -> Zscore {
        Zscore {
            key: key.to_string(),
            member: member.to_string(),
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zscore> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;

        Ok(Zscore {
            key,
            member,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zscore> {
        if argv.len() != 2 {
            return Ok(Zscore::new_invalid());
        }
        Ok(Zscore::new(&argv[0], &argv[1]))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zscore(None).await?;
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

    pub async fn zscore(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zcore(&self.key, &self.member)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Zscore {
    fn new_invalid() -> Zscore {
        Zscore {
            key: "".to_string(),
            member: "".to_string(),
            valid: false,
        }
    }
}
