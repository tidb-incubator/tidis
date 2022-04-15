use tikv_client::Error as TiKVError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RTError {
    #[error("{0}")]
    TikvClientError(TiKVError),
    #[error("{0}")]
    StringError(String),
}

impl From<TiKVError> for RTError {
    fn from(e: TiKVError) -> Self {
        RTError::TikvClientError(e)
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


pub const REDIS_WRONG_TYPE_ERR: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";