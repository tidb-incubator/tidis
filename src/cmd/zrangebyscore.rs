use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use bytes::Buf;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zrangebyscore {
    key: String,
    min: i64,
    min_inclusive: bool,
    max: i64,
    max_inclusive: bool,
    withscores: bool,
}

impl Zrangebyscore {
    pub fn new(key: &str, min: i64, min_inclusive: bool, max: i64, max_inclusive: bool, withscores: bool) -> Zrangebyscore {
        Zrangebyscore {
            key: key.to_string(),
            min: min,
            min_inclusive: min_inclusive,
            max: max,
            max_inclusive: max_inclusive,
            withscores: withscores,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrangebyscore> {
        let key = parse.next_string()?;
        let mut min_inclusive = true;
        let mut max_inclusive = true;

        // parse score range as bytes, to handle exclusive bounder
        let mut bmin = parse.next_bytes()?;
        // check first byte
        if bmin[0] == b'(' {
            // drain the first byte
            bmin.advance(1);
            min_inclusive = false;
        }
        let min = String::from_utf8_lossy(&bmin.to_vec()).parse::<i64>().unwrap();
        
        let mut bmax = parse.next_bytes()?;
        if bmax[0] == b'(' {
            bmax.advance(1);
            max_inclusive = false;
        }
        let max = String::from_utf8_lossy(&bmax.to_vec()).parse::<i64>().unwrap();

        let mut withscores = false;

        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            match v.to_uppercase().as_str() {
                // flags implement in signle command, such as ZRANGEBYSCORE
                "LIMIT" => {},
                "WITHSCORES" => {
                    withscores = true;
                },
                _ => {}
            }
        }

        let z = Zrangebyscore::new(&key, min, min_inclusive, max, max_inclusive, withscores);

        Ok(z)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection, reverse: bool) -> crate::Result<()> {
        let response = self.zrangebyscore(reverse).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn zrangebyscore(&self, reverse: bool) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ZsetCommandCtx::new(None).do_async_txnkv_zrange_by_score(&self.key, self.min, self.min_inclusive, self.max, self.max_inclusive, self.withscores, reverse).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
