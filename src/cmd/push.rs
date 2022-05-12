use std::sync::Arc;

use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::ListCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err, resp_invalid_arguments};

use bytes::Bytes;
use tikv_client::Transaction;
use tokio::sync::Mutex;
use crate::config::LOGGER;
use slog::debug;

#[derive(Debug)]
pub struct Push {
    key: String,
    items: Vec<Bytes>,
    valid: bool,
}

impl Push {
    pub fn new(key: &str) -> Push {
        Push {
            items: vec![],
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Push {
        Push {
            items: vec![],
            key: "".to_owned(),
            valid: false,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn items(&self) -> &Vec<Bytes> {
        &self.items
    }

    pub fn add_item(&mut self, item: Bytes) {
        self.items.push(item);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Push> {
        let key = parse.next_string()?;
        let mut push = Push::new(&key);

        while let Ok(item) = parse.next_bytes() {
            push.add_item(item);
        }

        Ok(push)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Push> {
        if argv.len() < 2 {
            return Ok(Push::new_invalid());
        }
        let mut push = Push::new(&argv[0]);

        for arg in &argv[1..] {
            push.add_item(Bytes::from(arg.to_owned()));
        }

        Ok(push)
    }

    pub(crate) async fn apply(self, dst: &mut Connection, op_left: bool) -> crate::Result<()> {
        let response = self.push(None, op_left).await?;
        debug!(LOGGER, "res, {} -> {}, {:?}", dst.local_addr(), dst.peer_addr(), response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn push(&self, txn: Option<Arc<Mutex<Transaction>>>, op_left: bool) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ListCommandCtx::new(txn).do_async_txnkv_push(&self.key, &self.items, op_left).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
