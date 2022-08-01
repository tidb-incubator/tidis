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
pub struct Smismember {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Smismember {
    pub fn new(key: &str) -> Smismember {
        Smismember {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Smismember> {
        let key = parse.next_string()?;
        let mut smismember = Smismember::new(&key);
        while let Ok(member) = parse.next_string() {
            smismember.add_member(&member);
        }
        Ok(smismember)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Smismember> {
        if argv.len() < 2 {
            return Ok(Smismember::new_invalid());
        }
        let mut s = Smismember::new(&String::from_utf8_lossy(&argv[0]));
        for arg in &argv[1..] {
            s.add_member(&String::from_utf8_lossy(arg));
        }
        Ok(s)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.smismember(None).await?;
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

    pub async fn smismember(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            SetCommandCtx::new(txn)
                .do_async_txnkv_sismember(&self.key, &self.members, true)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Smismember {
    fn new_invalid() -> Smismember {
        Smismember {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
