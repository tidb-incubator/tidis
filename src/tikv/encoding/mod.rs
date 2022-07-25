pub mod decode;
pub mod encode;

#[derive(Debug, Clone)]
pub enum DataType {
    String,
    Hash,
    List,
    Set,
    Zset,
    Null,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Hash => write!(f, "hash"),
            DataType::List => write!(f, "list"),
            DataType::Set => write!(f, "set"),
            DataType::Zset => write!(f, "zset"),
            DataType::Null => write!(f, "none"),
        }
    }
}

use std::fmt;
pub use {decode::KeyDecoder, encode::KeyEncoder};

const SIGN_MASK: u64 = 0x8000000000000000;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_ASC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];
