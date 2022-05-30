use thiserror::Error;
use tikv_client::Error as TiKVError;

#[derive(Error, Debug)]
pub enum RTError {
    #[error("{0}")]
    TikvClientError(Box<TiKVError>),
    #[error("{0}")]
    StringError(String),
}

impl From<TiKVError> for RTError {
    fn from(e: TiKVError) -> Self {
        RTError::TikvClientError(Box::new(e))
    }
}

impl From<String> for RTError {
    fn from(e: String) -> Self {
        RTError::StringError(e)
    }
}

impl From<&str> for RTError {
    fn from(e: &str) -> Self {
        RTError::StringError(e.to_string())
    }
}

pub type AsyncResult<T> = std::result::Result<T, RTError>;

pub const REDIS_WRONG_TYPE_ERR: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";
pub const REDIS_NO_SUCH_KEY_ERR: &str = "ERR no such key";
pub const REDIS_INDEX_OUT_OF_RANGE: &str = "ERR index out of range";
pub const REDIS_VALUE_IS_NOT_INTEGER: &str = "ERR value is not an integer or out of range";
