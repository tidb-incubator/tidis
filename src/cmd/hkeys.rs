use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::HashCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hkeys {
    key: String,
}

impl Hkeys {
    pub fn new(key: &str) -> Hkeys {
        Hkeys {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hkeys> {
        let key = parse.next_string()?;
        Ok(Hkeys{key:key})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hkeys().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn hkeys(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            HashCommandCtx::new(None).do_async_txnkv_hgetall(&self.key, true, false).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
