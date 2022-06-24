use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Zrem {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Zrem {
    pub fn new(key: &str) -> Zrem {
        Zrem {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrem> {
        let key = parse.next_string()?;
        let mut zrem = Zrem::new(&key);

        // parse member
        while let Ok(member) = parse.next_string() {
            zrem.add_member(&member);
        }

        Ok(zrem)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zrem> {
        if argv.len() < 2 {
            return Ok(Zrem::new_invalid());
        }
        let mut zrem = Zrem::new(&argv[0]);
        for arg in &argv[1..] {
            zrem.add_member(arg);
        }
        Ok(zrem)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrem(None).await?;
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

    pub async fn zrem(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zrem(&self.key, &self.members)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Zrem {
    fn new_invalid() -> Zrem {
        Zrem {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
