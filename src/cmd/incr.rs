use crate::{Connection, Frame, Parse};
use crate::tikv::string::{do_async_rawkv_incr, do_async_txnkv_incr};
use crate::config::{is_use_txn_api};
use crate::tikv::errors::AsyncResult;

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Incr {
    key: String,
}

impl Incr {
    pub fn new(key: impl ToString) -> Incr {
        Incr {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Incr> {
        let key = parse.next_string()?;

        Ok(Incr { key })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.incr(&self.key).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn incr(&self, key: &String) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_incr(key, true, 1).await
        } else {
            do_async_rawkv_incr(key, false, 1).await
        }
    }
}
