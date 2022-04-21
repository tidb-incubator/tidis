use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zrevrange {
    key: String,
    min: i64,
    max: i64,
    withscores: bool,
}

impl Zrevrange {
    pub fn new(key: &str, min: i64, max: i64, withscores: bool) -> Zrevrange {
        Zrevrange {
            key: key.to_string(),
            min: min,
            max: max,
            withscores: withscores,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrevrange> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let mut withscores = false;

        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            match v.to_uppercase().as_str() {
                "WITHSCORES" => {
                    withscores = true;
                },
                _ => {}
            }
        }

        let z = Zrevrange::new(&key, min, max, withscores);

        Ok(z)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrevrange().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn zrevrange(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ZsetCommandCtx::new(None).do_async_txnkv_zrange(&self.key, self.min, self.max, self.withscores, true).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
