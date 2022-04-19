use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Spop {
    key: String,
    count: i64,
}

impl Spop {
    pub fn new(key: &str, count: i64) -> Spop {
        Spop {
            key: key.to_string(),
            count: count,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Spop> {
        let key = parse.next_string()?;
        
        let mut count = 1;
        if let Ok(v) = parse.next_int() {
            count = v;
        }
        Ok(Spop{ key, count })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.spop().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn spop(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            SetCommandCtx::new(None).do_async_txnkv_spop(&self.key, self.count as u64).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
