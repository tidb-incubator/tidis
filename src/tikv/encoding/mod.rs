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
