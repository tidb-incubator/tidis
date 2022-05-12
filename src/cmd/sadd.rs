use std::sync::Arc;

use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err, resp_invalid_arguments};

use tikv_client::Transaction;
use tokio::sync::Mutex;
use crate::config::LOGGER;
use slog::debug;

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

    pub fn new_invalid() -> Sadd {
        Sadd {
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


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sadd> {
        let key = parse.next_string()?;
        let mut sadd = Sadd::new(&key);
        loop {
            if let Ok(member) = parse.next_string() {
                sadd.add_member(&member);
            } else {
                break;
            }
        }
        Ok(sadd)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Sadd> {
        if argv.len() < 2 {
            return Ok(Sadd::new_invalid());
        }
        let key = &argv[0];
        let mut sadd = Sadd::new(key);
        for arg in &argv[1..] {
            sadd.add_member(arg);
        }
        Ok(sadd)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.sadd(None).await?;
        debug!(LOGGER, "res, {} -> {}, {:?}", dst.local_addr(), dst.peer_addr(), response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn sadd(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            SetCommandCtx::new(txn).do_async_txnkv_sadd(&self.key, &self.members).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
