use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::{do_async_txnkv_push};
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Push {
    key: String,
    items: Vec<Bytes>,
}

impl Push {
    pub fn new(key: &str) -> Push {
        Push {
            items: vec![],
            key: key.to_owned(),
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

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection, op_left: bool) -> crate::Result<()> {
        let response = self.push(op_left).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn push(&self, op_left: bool) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_push(&self.key, &self.items, op_left).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
