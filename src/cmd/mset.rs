use crate::{Connection, Db, Frame, Parse};
use crate::tikv::string::{do_async_rawkv_batch_put};
use tikv_client::{Key, Value, KvPair};
use crate::tikv::{
    encoding::{KeyEncoder}
};
use bytes::Bytes;
use tracing::{debug, instrument};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Mset {
    /// Name of the keys to get
    keys: Vec<String>,
    vals: Vec<Bytes>,
}

impl Mset {
    /// Create a new `Mset` command which fetches `key` vector.
    pub fn new() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn vals(&self) -> &Vec<Bytes> {
        &self.vals
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub fn add_val(&mut self, val: Bytes) {
        self.vals.push(val);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Mset> {
        let mut mset = Mset::new();

        loop{
            if let Ok(key) = parse.next_string() {
                mset.add_key(key);
                if let Ok(val) = parse.next_bytes() {
                    mset.add_val(val);
                } else {
                    return Err("protocol error".into());
                }
            } else {
                break;
            }
        }

        Ok(mset)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let mut kvs = Vec::new();
        for (idx, key) in self.keys.iter().enumerate() {
            let val = &self.vals[idx];
            let ekey = KeyEncoder::new().encode_string(&key);
            let kvpair = KvPair::from((ekey, String::from_utf8(val.to_vec()).unwrap()));
            kvs.push(kvpair);
        }
        let response = match do_async_rawkv_batch_put(kvs).await {
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