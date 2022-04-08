use crate::{Connection, Db, Frame, Parse, tikv::string::do_async_rawkv_incr};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Decr {
    key: String,
}

impl Decr {
    pub fn new(key: impl ToString) -> Decr {
        Decr {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Decr> {
        let key = parse.next_string()?;

        Ok(Decr { key })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match do_async_rawkv_incr(self.key, false, 1).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame
    }
}
