use std::sync::Arc;

use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err, resp_invalid_arguments};

use tikv_client::Transaction;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

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

    pub fn new_invalid() -> Zscore {
        Zscore {
            key: "".to_string(),
            member: "".to_string(),
            valid: false,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zscore> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;

        Ok(Zscore{key, member, valid: true})
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zscore> {
        if argv.len() != 2 {
            return Ok(Zscore::new_invalid());
        }
        Ok(Zscore::new(&argv[0], &argv[1]))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.zscore(None).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zscore(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments())
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn).do_async_txnkv_zcore(&self.key, &self.member).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
