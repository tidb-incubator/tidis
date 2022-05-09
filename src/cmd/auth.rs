use std::sync::Arc;

use crate::tikv::errors::AsyncResult;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::{is_use_txn_api};
use tikv_client::Transaction;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Auth {
    passwd: String,
}

impl Auth {
    pub fn new(passwd: String) -> Auth {
        Auth {
            passwd: passwd.to_string(),
        }
    }

    pub fn passwd(&self) -> &str {
        &self.passwd
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Auth> {
        let passwd = parse.next_string()?;

        Ok(Auth { passwd })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        Ok(())
    }
}
