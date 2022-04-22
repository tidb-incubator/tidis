use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zpop {
    key: String,
    count: i64,
}

impl Zpop {
    pub fn new(key: &str, count: i64) -> Zpop {
        Zpop {
            key: key.to_string(),
            count: count,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zpop> {
        let key = parse.next_string()?;
        // default count is 1
        let mut count = 1;
        if let Ok(c) = parse.next_int() {
            count = c;
        }
        Ok(Zpop{key, count})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection, from_min: bool) -> crate::Result<()> {
        
        let response = self.pop(from_min).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn pop(&self, from_min: bool) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ZsetCommandCtx::new(None).do_async_txnkv_zpop(&self.key, from_min, self.count as u64).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
