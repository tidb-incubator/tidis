use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Srem {
    key: String,
    members: Vec<String>,
}

impl Srem {
    pub fn new(key: &str) -> Srem {
        Srem {
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


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Srem> {
        let key = parse.next_string()?;
        let mut srem = Srem::new(&key);
        loop {
            if let Ok(member) = parse.next_string() {
                srem.add_member(&member);
            } else {
                break;
            }
        }
        Ok(srem)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.srem().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn srem(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            SetCommandCtx::new(None).do_async_txnkv_srem(&self.key, &self.members).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
