use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use bytes::Buf;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zcount {
    key: String,
    min: i64,
    min_inclusive: bool,
    max: i64,
    max_inclusive: bool,
}

impl Zcount {
    pub fn new(key: &str, min: i64, min_inclusive: bool, max: i64, max_inclusive: bool) -> Zcount {
        Zcount {
            key: key.to_string(),
            min: min,
            min_inclusive: min_inclusive,
            max: max,
            max_inclusive: max_inclusive,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zcount> {
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

        let z = Zcount::new(&key, min, min_inclusive, max, max_inclusive);

        Ok(z)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zcount().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn zcount(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ZsetCommandCtx::new(None).do_async_txnkv_zcount(&self.key, self.min, self.min_inclusive, self.max, self.max_inclusive).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
