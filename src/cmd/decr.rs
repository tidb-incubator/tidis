use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::{is_use_txn_api};
use crate::tikv::errors::AsyncResult;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Decr {
    key: String,
}

impl Decr {
    pub fn new(key: impl ToString) -> Decr {
        Decr {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Decr> {
        let key = parse.next_string()?;

        Ok(Decr { key })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.decr().await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn decr(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            StringCommandCtx::new(None).do_async_txnkv_incr(&self.key, true, -1).await
        } else {
            StringCommandCtx::new(None).do_async_rawkv_incr(&self.key, false, -1).await
        }
    }
}
