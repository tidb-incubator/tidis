use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::{do_async_txnkv_hmget};
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hmget {
    key: String,
    fields: Vec<String>,
}

impl Hmget {
    pub fn new(key: &str) -> Hmget {
        Hmget {
            key: key.to_owned(),
            fields: vec![],
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub fn add_field(&mut self, field: &str) {
        self.fields.push(field.to_string());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hmget> {
        let key = parse.next_string()?;
        let mut hmget = Hmget::new(&key);
        loop {
            if let Ok(field) = parse.next_string() {
                hmget.add_field(&field);
            } else {
                break;
            }
        }
        Ok(hmget)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hmget().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn hmget(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_hmget(&self.key, &self.fields).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
