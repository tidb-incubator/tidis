use crate::tikv::errors::AsyncResult;
use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::{is_use_txn_api};
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct TTL {
    key: String,
}

impl TTL {
    pub fn new(key: impl ToString) -> TTL {
        TTL {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<TTL> {
        let key = parse.next_string()?;

        Ok(TTL { key })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection, is_millis: bool) -> crate::Result<()> {
        let response = match self.ttl(is_millis).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn ttl(self, is_millis: bool) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            StringCommandCtx::new(None).do_async_txnkv_ttl(&self.key, is_millis).await
        } else {
            StringCommandCtx::new(None).do_async_rawkv_get_ttl(&self.key).await
        }
    }
}
