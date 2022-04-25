use crate::tikv::errors::AsyncResult;
use crate::utils::{resp_err, timestamp_from_ttl};
use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::{is_use_txn_api};
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Expire {
    key: String,
    seconds: i64,
}

impl Expire {
    pub fn new(key: impl ToString, seconds: i64) -> Expire {
        Expire {
            key: key.to_string(),
            seconds: seconds,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Expire> {
        let key = parse.next_string()?;
        let seconds = parse.next_int()?;

        Ok(Expire { key, seconds})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection, is_millis: bool, expire_at: bool) -> crate::Result<()> {
        let response = match self.expire(is_millis, expire_at).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };
        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn expire(self, is_millis: bool, expire_at: bool) -> AsyncResult<Frame> {
        let mut ttl = self.seconds as u64;
        if is_use_txn_api() {
            if !is_millis {
                ttl = ttl * 1000;
            }
            if !expire_at {
                ttl = timestamp_from_ttl(ttl);
            }
            StringCommandCtx::new(None).do_async_txnkv_expire(&self.key, ttl).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
