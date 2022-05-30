use std::sync::Arc;

use crate::cmd::Parse;
use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Srem {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Srem {
    pub fn new(key: &str) -> Srem {
        Srem {
            key: key.to_string(),
            members: vec![],
            valid: true,
        }
    }

    pub fn new_invalid() -> Srem {
        Srem {
            key: "".to_string(),
            members: vec![],
            valid: false,
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Srem> {
        let key = parse.next_string()?;
        let mut srem = Srem::new(&key);
        while let Ok(member) = parse.next_string() {
            srem.add_member(&member);
        }
        Ok(srem)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Srem> {
        if argv.len() < 2 {
            return Ok(Srem::new_invalid());
        }
        let key = &argv[0];
        let mut srem = Srem::new(key);
        for arg in &argv[1..] {
            srem.add_member(arg);
        }
        Ok(srem)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.srem(None).await?;
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

    pub async fn srem(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            SetCommandCtx::new(txn)
                .do_async_txnkv_srem(&self.key, &self.members)
                .await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
