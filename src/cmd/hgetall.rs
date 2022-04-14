use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::{do_async_txnkv_hgetall};
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hgetall {
    key: String,
}

impl Hgetall {
    pub fn new(key: &str) -> Hgetall {
        Hgetall {
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


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hgetall> {
        let key = parse.next_string()?;
        Ok(Hgetall{key:key})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hgetall().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn hgetall(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_hgetall(&self.key, true, true).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
