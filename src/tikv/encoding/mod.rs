pub mod decode;
pub mod encode;

pub enum DataType {
    String,
    Hash,
    List,
    Set,
    Zset,
    Null,
}

pub use {decode::KeyDecoder, encode::KeyEncoder};
