use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::ListCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Lset {
    key: String,
    idx: i64,
    element: Bytes,
}

impl Lset {
    pub fn new(key: &str, idx: i64, ele: Bytes) -> Lset {
        Lset {
            key: key.to_owned(),
            idx: idx,
            element: ele.to_owned(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lset> {
        let key = parse.next_string()?;
        let idx = parse.next_int()?;
        let element = parse.next_bytes()?;

        Ok(Lset { key, idx, element })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lset().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn lset(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ListCommandCtx::new(None).do_async_txnkv_lset(&self.key, self.idx, &self.element).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
