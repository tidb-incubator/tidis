use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Smembers {
    key: String,
}

impl Smembers {
    pub fn new(key: &str) -> Smembers {
        Smembers {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Smembers> {
        let key = parse.next_string()?;
        Ok(Smembers::new(&key))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.smembers().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn smembers(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            SetCommandCtx::new(None).do_async_txnkv_smembers(&self.key).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
