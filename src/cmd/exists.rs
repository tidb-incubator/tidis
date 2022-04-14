use crate::{Connection, Frame, Parse};
use crate::tikv::string::{do_async_rawkv_exists, do_async_txnkv_exists};
use crate::config::{is_use_txn_api};
use crate::tikv::errors::AsyncResult;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Exists {
    keys: Vec<String>,
}

impl Exists {
    pub fn new() -> Exists {
        Exists {
            keys: vec![],
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key)
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Exists> {
        let mut exists = Exists::new();

        while let Ok(key) = parse.next_string() {
            exists.add_key(key);
        }

        Ok(exists)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.exists(self.keys()).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn exists(&self, keys: &Vec<String>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_exists(keys).await
        } else {
            do_async_rawkv_exists(keys).await
        }
    }
}
