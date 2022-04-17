use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::ListCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Lindex {
    key: String,
    idx: i64,
}

impl Lindex {
    pub fn new(key: &str, idx: i64) -> Lindex {
        Lindex {
            key: key.to_owned(),
            idx: idx,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lindex> {
        let key = parse.next_string()?;
        let idx = parse.next_int()?;

        Ok(Lindex { key, idx })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lindex().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn lindex(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ListCommandCtx::new(None).do_async_txnkv_lindex(&self.key, self.idx).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
