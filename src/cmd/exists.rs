use std::sync::Arc;

use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use tikv_client::Transaction;
use tokio::sync::Mutex;

use crate::cmd::Invalid;
use crate::config::LOGGER;
use slog::debug;

#[derive(Debug)]
pub struct Exists {
    keys: Vec<String>,
    valid: bool,
}

impl Exists {
    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key)
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Exists> {
        let mut exists = Exists::default();

        while let Ok(key) = parse.next_string() {
            exists.add_key(key);
        }

        Ok(exists)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Exists> {
        if argv.is_empty() {
            return Ok(Exists {
                keys: vec![],
                valid: false,
            });
        }
        Ok(Exists {
            keys: argv.to_owned(),
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.exists(None).await.unwrap_or_else(Into::into);

        debug!(
            LOGGER,
            "res, {} -> {}, {:?}",
            dst.local_addr(),
            dst.peer_addr(),
            response
        );

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn exists(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_exists(&self.keys)
                .await
        } else {
            StringCommandCtx::new(txn)
                .do_async_rawkv_exists(&self.keys)
                .await
        }
    }
}

impl Default for Exists {
    fn default() -> Self {
        Exists {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Exists {
    fn new_invalid() -> Exists {
        Exists {
            keys: vec![],
            valid: false,
        }
    }
}
