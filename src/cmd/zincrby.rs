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

#[derive(Debug, Clone)]
pub struct Zincrby {
    key: String,
    step: f64,
    member: String,
    valid: bool,
}

impl Zincrby {
    pub fn new(key: &str, step: f64, member: &str) -> Zincrby {
        Zincrby {
            key: key.to_string(),
            step,
            member: member.to_string(),
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zincrby> {
        let key = parse.next_string()?;
        let step_byte = parse.next_bytes()?;
        let member = parse.next_string()?;

        let step = String::from_utf8_lossy(&step_byte.to_vec()).parse::<f64>()?;

        Ok(Zincrby::new(&key, step, &member))
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zincrby> {
        if argv.len() != 3 {
            return Ok(Zincrby::new_invalid());
        }

        let key = &argv[0];
        let step = argv[1].parse::<f64>()?;
        let member = &argv[2];

        Ok(Zincrby::new(key, step, member))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zincrby(None).await?;

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

    pub async fn zincrby(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zincrby(&self.key, self.step, &self.member)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Zincrby {
    fn new_invalid() -> Zincrby {
        Zincrby {
            key: "".to_string(),
            member: "".to_string(),
            step: 0f64,
            valid: false,
        }
    }
}
