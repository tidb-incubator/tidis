use crate::tikv::errors::AsyncResult;
use crate::utils::resp_err;
use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::is_use_txn_api;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub fn new() -> Del {
        Del {
            keys: vec![],
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let mut del = Del::new();
        while let Ok(key) = parse.next_string() {
            del.add_key(key);
        }

        Ok(del)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.del(&self.keys).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn del(&self, keys: &Vec<String>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            StringCommandCtx::new(None).do_async_txnkv_del(keys).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}