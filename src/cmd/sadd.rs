use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::set::SetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Sadd {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Sadd {
    pub fn new(key: &str) -> Sadd {
        Sadd {
            key: key.to_string(),
            members: vec![],
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    pub fn add_member(&mut self, member: &str) {
        self.members.push(member.to_string());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sadd> {
        let key = parse.next_string()?;
        let mut sadd = Sadd::new(&key);
        while let Ok(member) = parse.next_string() {
            sadd.add_member(&member);
        }
        Ok(sadd)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Sadd> {
        if argv.len() < 2 {
            return Ok(Sadd::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let mut sadd = Sadd::new(key);
        for arg in &argv[1..] {
            sadd.add_member(&String::from_utf8_lossy(arg));
        }
        Ok(sadd)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.sadd(None).await?;
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

    pub async fn sadd(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            SetCommandCtx::new(txn)
                .do_async_txnkv_sadd(&self.key, &self.members)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Sadd {
    fn new_invalid() -> Sadd {
        Sadd {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
