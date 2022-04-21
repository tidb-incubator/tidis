use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zrem {
    key: String,
    members: Vec<String>,
}

impl Zrem {
    pub fn new(key: &str) -> Zrem {
        Zrem {
            key: key.to_string(),
            members: vec![],
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

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.zrem().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn zrem(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            ZsetCommandCtx::new(None).do_async_txnkv_zrem(&self.key, &self.members).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
