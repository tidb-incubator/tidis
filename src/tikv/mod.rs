use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use std::time::Duration;

use tikv_client::{RawClient, Transaction, TransactionClient};

use crate::config::LOGGER;

use self::client::RawClientWrapper;
use self::client::TxnClientWrapper;

use self::errors::{RTError, AsyncResult};

pub mod string;
pub mod hash;
pub mod list;
pub mod set;
pub mod zset;
pub mod lua;
pub mod errors;
pub mod client;
pub mod encoding;

lazy_static! {
    pub static ref PD_ADDRS: Arc<RwLock<Option<Vec<String>>>> = Arc::new(RwLock::new(None));
    pub static ref TIKV_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction>>> =
        Arc::new(RwLock::new(HashMap::new()));
    pub static ref TIKV_TNX_CONN_POOL: Arc<Mutex<LinkedList<TransactionClient>>> =
        Arc::new(Mutex::new(LinkedList::new()));
}

pub static mut TIKV_RAW_CLIENT: Option<RawClient> = None;

pub static mut TIKV_TXN_CLIENT: Option<TransactionClient> = None;

pub static mut INSTANCE_ID: u64 = 0;

pub fn set_instance_id(id: u64) {
    unsafe {
        INSTANCE_ID = id;
    }
}

pub fn get_instance_id() -> u64 {
    unsafe { INSTANCE_ID }
}


pub fn get_client() -> Result<RawClientWrapper, RTError> {
    if unsafe {TIKV_RAW_CLIENT.is_none() } {
        return Err(RTError::StringError(String::from("Not Connected")))
    }
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap() };
    let ret = RawClientWrapper::new(client);
    Ok(ret)
}

pub fn get_txn_client() -> Result<TxnClientWrapper<'static>, RTError> {
    if unsafe {TIKV_RAW_CLIENT.is_none() } {
        return Err(RTError::StringError(String::from("Not Connected")))
    }
    let client = unsafe {TIKV_TXN_CLIENT.as_ref().unwrap() };
    let ret = TxnClientWrapper::new(client);
    Ok(ret)
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}


pub async fn do_async_txn_connect(addrs: Vec<String>) -> AsyncResult<()> {
    PD_ADDRS.write().unwrap().replace(addrs.clone());

    let client = TransactionClient::new(addrs.clone(), Some(LOGGER.clone())).await?;
    unsafe {
        TIKV_TXN_CLIENT.replace(client);
    }
    Ok(())
}

pub async fn do_async_raw_connect(addrs: Vec<String>) -> AsyncResult<()> {
    let client = RawClient::new(addrs.clone(), Some(LOGGER.clone())).await?;
    unsafe {
        TIKV_RAW_CLIENT.replace(client);
    }
    Ok(())
}

pub async fn do_async_connect(addrs: Vec<String>) -> AsyncResult<()> {
    do_async_txn_connect(addrs.clone()).await?;
    do_async_raw_connect(addrs).await?;
    Ok(())
}