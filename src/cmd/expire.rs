use crate::{Connection, Frame, Parse, tikv::string::do_async_rawkv_expire};

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Expire {
    key: String,
    seconds: u64,
}

impl Expire {
    pub fn new(key: impl ToString, seconds: u64) -> Expire {
        Expire {
            key: key.to_string(),
            seconds: seconds,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn seconds(&self) -> u64 {
        self.seconds
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Expire> {
        let key = parse.next_string()?;
        let seconds = parse.next_int()?;

        Ok(Expire { key, seconds })
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match do_async_rawkv_expire(&self.key, None, self.seconds as i64).await {
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
