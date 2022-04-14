use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::{do_async_txnkv_hdel};
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hdel {
    key: String,
    field: String,
}

impl Hdel {
    pub fn new(key: &str, field: &str) -> Hdel {
        Hdel {
            field: field.to_owned(),
            key: key.to_owned(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hdel> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(Hdel::new(&key, &field))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hdel().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn hdel(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_hdel(&self.key, &self.field).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
