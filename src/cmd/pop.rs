use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::{do_async_txnkv_pop};
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Pop {
    key: String,
    count: i64,
}

impl Pop {
    pub fn new(key: &str, count: i64) -> Pop {
        Pop {
            key: key.to_owned(),
            count: count,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Pop> {
        let key = parse.next_string()?;
        let mut count = 1;
        
        if let Ok(n) = parse.next_int() {
            count = n;
        }

        let pop = Pop::new(&key, count);

        Ok(pop)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection, op_left: bool) -> crate::Result<()> {
        let response = self.pop(op_left).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn pop(&self, op_left: bool) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_pop(&self.key, op_left, self.count).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
