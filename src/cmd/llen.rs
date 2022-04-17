use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::ListCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Llen {
    key: String,
}

impl Llen {
    pub fn new(key: &str) -> Llen {
        Llen {
            key: key.to_owned(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Llen> {
        let key = parse.next_string()?;
        let mut count = 1;

        Ok(Llen{ key })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.llen().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn llen(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ListCommandCtx::new(None).do_async_txnkv_llen(&self.key).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
