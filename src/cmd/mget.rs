use crate::{Connection, Db, Frame, Parse};
use crate::tikv::string::{do_async_rawkv_batch_get};
use tracing::{debug, instrument};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Mget {
    /// Name of the keys to get
    keys: Vec<String>,
}

impl Mget {
    /// Create a new `Mget` command which fetches `key` vector.
    pub fn new() -> Mget {
        Mget {
            keys: vec![],
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Mget> {
        // The `MGET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let mut mget = Mget::new();

        while let Ok(key) = parse.next_string() {
            mget.add_key(key);
        }

        Ok(mget)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match do_async_rawkv_batch_get(self.keys).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Get` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        // TODO
        frame
    }
}