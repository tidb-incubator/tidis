use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::HashCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hincrby {
    key: String,
    field: String,
    step: i64,
}

impl Hincrby {
    pub fn new(key: &str, field: &str, step: i64) -> Hincrby {
        Hincrby {
            key: key.to_string(),
            field: field.to_string(),
            step: step,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    pub fn set_field(&mut self, field: &str) {
        self.field = field.to_owned();
    }


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hincrby> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        let step = parse.next_int()?;
        Ok(Hincrby{key, field, step})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hincrby().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn hincrby(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            HashCommandCtx::new(None).do_async_txnkv_hincrby(&self.key, &self.field, self.step).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
