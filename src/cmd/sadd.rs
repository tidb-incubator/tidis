use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Sadd {
    key: String,
    members: Vec<String>,
}

impl Sadd {
    pub fn new(key: &str) -> Sadd {
        Sadd {
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

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.sadd().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn sadd(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            SetCommandCtx::new(None).do_async_txnkv_sadd(&self.key, &self.members).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
