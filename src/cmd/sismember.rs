use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Sismember {
    key: String,
    member: String,
}

impl Sismember {
    pub fn new(key: &str, member: &str) -> Sismember {
        Sismember {
            key: key.to_string(),
            member: member.to_string(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sismember> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;
        Ok(Sismember{key, member})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.sismember().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn sismember(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            let mut members = vec![];
            members.push(self.member.clone());
            SetCommandCtx::new(None).do_async_txnkv_sismember(&self.key, &members).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
