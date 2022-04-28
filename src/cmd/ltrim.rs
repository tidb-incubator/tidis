use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::ListCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Ltrim {
    key: String,
    start: i64,
    end: i64,
}

impl Ltrim {
    pub fn new(key: &str, start: i64, end: i64) -> Ltrim {
        Ltrim {
            key: key.to_owned(),
            start: start,
            end: end,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ltrim> {
        let key = parse.next_string()?;
        let start = parse.next_int()?;
        let end = parse.next_int()?;

        Ok(Ltrim { key, start, end })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.ltrim().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn ltrim(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ListCommandCtx::new(None).do_async_txnkv_ltrim(&self.key, self.start, self.end).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
