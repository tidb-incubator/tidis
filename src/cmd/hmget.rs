use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::hash::HashCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use crate::tikv::client::Transaction;
use slog::debug;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Hmget {
    key: String,
    fields: Vec<String>,
    valid: bool,
}

impl Hmget {
    pub fn new(key: &str) -> Hmget {
        Hmget {
            key: key.to_owned(),
            fields: vec![],
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub fn add_field(&mut self, field: &str) {
        self.fields.push(field.to_string());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hmget> {
        let key = parse.next_string()?;
        let mut hmget = Hmget::new(&key);
        while let Ok(field) = parse.next_string() {
            hmget.add_field(&field);
        }
        Ok(hmget)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Hmget> {
        if argv.len() < 2 {
            return Ok(Hmget::new_invalid());
        }
        let key = &argv[0];
        let mut hmget = Hmget::new(key);
        for arg in &argv[1..argv.len()] {
            hmget.add_field(arg);
        }
        Ok(hmget)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hmget(None).await?;
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

    pub async fn hmget(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            HashCommandCtx::new(txn)
                .do_async_txnkv_hmget(&self.key, &self.fields)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Hmget {
    fn new_invalid() -> Hmget {
        Hmget {
            key: "".to_owned(),
            fields: vec![],
            valid: false,
        }
    }
}
