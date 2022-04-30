use std::sync::Arc;

use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::HashCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tikv_client::Transaction;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hget {
    key: String,
    field: String,
    valid: bool,
}

impl Hget {
    pub fn new(key: &str, field: &str) -> Hget {
        Hget {
            field: field.to_owned(),
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Hget {
        Hget {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hget> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(Hget::new(&key, &field))
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Hget> {
        if argv.len() != 2 {
            return Ok(Hget::new_invalid());
        }
        Ok(Hget::new(&argv[0], &argv[1]))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hget(None).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hget(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            HashCommandCtx::new(txn).do_async_txnkv_hget(&self.key, &self.field).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
