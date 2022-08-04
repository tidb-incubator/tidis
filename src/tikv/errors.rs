use mlua::prelude::LuaError;
use std::num::{ParseFloatError, ParseIntError};
use thiserror::Error;
use tikv_client::Error as TiKVError;

#[derive(Error, Debug)]
pub enum RTError {
    #[error("{0}")]
    TikvClient(Box<TiKVError>),
    #[error("{0}")]
    String(&'static str),
    #[error("{0}")]
    Owned(String),
}

impl RTError {
    pub fn to_is_not_integer_error<E>(_: E) -> RTError {
        REDIS_VALUE_IS_NOT_INTEGER_ERR
    }

    pub fn to_owned_error<T>(s: T) -> RTError
    where
        T: Into<String>,
    {
        RTError::Owned(s.into())
    }
}

impl From<TiKVError> for RTError {
    fn from(e: TiKVError) -> Self {
        RTError::TikvClient(Box::new(e))
    }
}

impl From<LuaError> for RTError {
    fn from(e: LuaError) -> Self {
        RTError::Owned(e.to_string())
    }
}

impl From<&'static str> for RTError {
    fn from(e: &'static str) -> Self {
        RTError::String(e)
    }
}

impl From<ParseIntError> for RTError {
    fn from(_: ParseIntError) -> Self {
        REDIS_VALUE_IS_NOT_INTEGER_ERR
    }
}

impl From<ParseFloatError> for RTError {
    fn from(_: ParseFloatError) -> Self {
        REDIS_VALUE_IS_NOT_VALID_FLOAT_ERR
    }
}

pub type AsyncResult<T> = std::result::Result<T, RTError>;

pub const REDIS_WRONG_TYPE_ERR: RTError =
    RTError::String("WRONGTYPE Operation against a key holding the wrong kind of value");
pub const REDIS_NO_SUCH_KEY_ERR: RTError = RTError::String("ERR no such key");
pub const REDIS_INDEX_OUT_OF_RANGE_ERR: RTError = RTError::String("ERR index out of range");
pub const REDIS_VALUE_IS_NOT_INTEGER_ERR: RTError =
    RTError::String("ERR value is not an integer or out of range");
pub const REDIS_VALUE_IS_NOT_VALID_FLOAT_ERR: RTError =
    RTError::String("ERR value is not a valid float");
pub const REDIS_BACKEND_NOT_CONNECTED_ERR: RTError = RTError::String("ERR backend not connected");
pub const REDIS_COMPARE_AND_SWAP_EXHAUSTED_ERR: RTError =
    RTError::String("ERR compare-and-swap exhausted");
pub const REDIS_NOT_SUPPORTED_ERR: RTError = RTError::String("ERR not supported");
pub const REDIS_NOT_SUPPORTED_DEBUG_SUB_COMMAND_ERR: RTError =
    RTError::String("ERR not supported debug sub command");
pub const REDIS_AUTH_WHEN_DISABLED_ERR: RTError =
    RTError::String("ERR Client sent AUTH, but no password is set");
pub const REDIS_AUTH_INVALID_PASSWORD_ERR: RTError = RTError::String("ERR invalid password");
pub const REDIS_AUTH_REQUIRED_ERR: RTError = RTError::String("NOAUTH Authentication required.");
pub const REDIS_NO_MATCHING_SCRIPT_ERR: RTError =
    RTError::String("NOSCRIPT No matching script. Please use EVAL.");
pub const REDIS_LUA_CONTEXT_IS_NOT_INITIALIZED_ERR: RTError =
    RTError::String("ERR lua context is not initialized");
pub const REDIS_LUA_PANIC: RTError = RTError::String("ERR lua panic");
pub const REDIS_UNKNOWN_SUBCOMMAND: RTError =
    RTError::String("Unknown subcommand or wrong number of arguments");
pub const DECREMENT_OVERFLOW: RTError = RTError::String("Decrement would overflow");
pub const REDIS_LIST_TOO_LARGE_ERR: RTError = RTError::String("ERR list is too large to execute");
pub const KEY_VERSION_EXHUSTED_ERR: RTError = RTError::String("ERR key version exhausted");
pub const REDIS_MULTI_NESTED_ERR: RTError = RTError::String("ERR MULTI calls can not be nested");
pub const REDIS_DISCARD_WITHOUT_MULTI_ERR: RTError = RTError::String("ERR DISCARD without MULTI");
pub const REDIS_EXEC_WITHOUT_MULTI_ERR: RTError = RTError::String("ERR EXEC without MULTI");
pub const REDIS_EXEC_ERR: RTError =
    RTError::String("EXECABORT Transaction discarded because of previous errors.");

pub const REDIS_INVALID_CLIENT_ID_ERR: RTError = RTError::String("ERR Invalid client ID");
pub const REDIS_NO_SUCH_CLIENT_ERR: RTError = RTError::String("ERR No such client");
