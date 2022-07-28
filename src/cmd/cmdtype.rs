use std::sync::Arc;

use crate::config::LOGGER;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use crate::config::is_use_txn_api;

use super::Invalid;

#[derive(Debug, Clone)]
pub struct Type {
    key: String,
    valid: bool,
}

impl Type {
    pub fn new(key: impl ToString) -> Type {
        Type {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Type> {
        let key = parse.next_string()?;

        Ok(Type::new(key))
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Type> {
        if argv.len() != 1 {
            return Ok(Type::new_invalid());
        }
        let key = &argv[0];
        Ok(Type::new(key))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.cmd_type(None).await.unwrap_or_else(Into::into);

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

    pub async fn cmd_type(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_type(&self.key)
                .await
        } else {
            StringCommandCtx::new(txn)
                .do_async_rawkv_type(&self.key)
                .await
        }
    }
}

impl Invalid for Type {
    fn new_invalid() -> Type {
        Type {
            key: "".to_owned(),
            valid: false,
        }
    }
}
