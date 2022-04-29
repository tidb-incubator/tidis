use std::sync::Arc;

use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::{is_use_txn_api};
use crate::tikv::errors::AsyncResult;
use tikv_client::Transaction;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Exists {
    keys: Vec<String>,
    valid: bool,
}

impl Exists {
    pub fn new() -> Exists {
        Exists {
            keys: vec![],
            valid: false,
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

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Exists> {
        if argv.len() == 0 {
            return Ok(Exists{keys: vec![], valid: false})
        }
        Ok(Exists{keys: argv.to_owned(), valid: true})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.exists(None).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn exists(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            StringCommandCtx::new(txn).do_async_txnkv_exists(&self.keys).await
        } else {
            StringCommandCtx::new(txn).do_async_rawkv_exists(&self.keys).await
        }
    }
}
