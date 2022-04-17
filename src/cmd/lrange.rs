use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::ListCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Lrange {
    key: String,
    left: i64,
    right: i64,
}

impl Lrange {
    pub fn new(key: &str, left: i64, right: i64) -> Lrange {
        Lrange {
            key: key.to_owned(),
            left: left,
            right: right,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lrange> {
        let key = parse.next_string()?;
        let left = parse.next_int()?;
        let right = parse.next_int()?;

        Ok(Lrange { key, left, right })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lrange().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn lrange(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ListCommandCtx::new(None).do_async_txnkv_lrange(&self.key, self.left, self.right).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
