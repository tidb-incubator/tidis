use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zremrangebyscore {
    key: String,
    min: i64,
    max: i64,
}

impl Zremrangebyscore {
    pub fn new(key: &str, min: i64, max: i64) -> Zremrangebyscore {
        Zremrangebyscore {
            key: key.to_string(),
            min: min,
            max: max,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zremrangebyscore> {
        let key = parse.next_string()?;

        // TODO support (/-inf/+inf
        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let z = Zremrangebyscore::new(&key, min, max);

        Ok(z)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zremrangebyscore().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn zremrangebyscore(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ZsetCommandCtx::new(None).do_async_txnkv_zremrange_by_score(&self.key, self.min, self.max).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
