use std::sync::Arc;

use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::{is_use_txn_api};
use crate::tikv::errors::AsyncResult;

use tikv_client::Transaction;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Incr {
    key: String,
    valid: bool,
}

impl Incr {
    pub fn new(key: impl ToString) -> Incr {
        Incr {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Incr {
        Incr {
            key: "".to_owned(),
            valid: false,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Incr> {
        let key = parse.next_string()?;

        Ok(Incr { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Incr> {
        if argv.len() != 1 {
            return Ok(Incr::new_invalid());
        }
        let key = &argv[0];
        Ok(Incr::new(key))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.incr(None).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn incr(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            StringCommandCtx::new(txn).do_async_txnkv_incr(&self.key, true, 1).await
        } else {
            StringCommandCtx::new(txn).do_async_rawkv_incr(&self.key, false, 1).await
        }
    }
}
