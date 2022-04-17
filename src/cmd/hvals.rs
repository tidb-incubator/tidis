use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::HashCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hvals {
    key: String,
}

impl Hvals {
    pub fn new(key: &str) -> Hvals {
        Hvals {
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


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hvals> {
        let key = parse.next_string()?;
        Ok(Hvals{key:key})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hvals().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn hvals(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            HashCommandCtx::new(None).do_async_txnkv_hgetall(&self.key, false, true).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
