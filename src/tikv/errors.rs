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

pub type AsyncResult<T> = std::result::Result<T, RTError>;