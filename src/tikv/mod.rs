use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tikv_client::{RawClient, Transaction, TransactionClient};

use self::client::RawClientWrapper;
use self::errors::{RTError, AsyncResult};

pub mod string;
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
pub static mut TIKV_RAW_CLIENT_2: Option<RawClient> = None;
pub static mut CLIENT_COUNTER: u64 = 0;

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
    let idx: u64;
    let ret: RawClientWrapper;
    unsafe {
        CLIENT_COUNTER += 1;
        idx = CLIENT_COUNTER;
    }
    if idx % 2 == 0 {
        let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap() };
        ret = RawClientWrapper::new(client);
    } else {
        let client = unsafe {TIKV_RAW_CLIENT_2.as_ref().unwrap() };
        ret = RawClientWrapper::new(client);
    }
    Ok(ret)
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}


pub async fn do_async_txn_connect(addrs: Vec<String>) -> AsyncResult<()> {
    PD_ADDRS.write().unwrap().replace(addrs.clone());
    Ok(())
}

pub async fn do_async_raw_connect(addrs: Vec<String>) -> AsyncResult<()> {
    let client = RawClient::new(addrs.clone(), None).await?;
    unsafe {
        TIKV_RAW_CLIENT.replace(client);
    };
    let client_2 = RawClient::new(addrs, None).await?;
    unsafe {
        TIKV_RAW_CLIENT_2.replace(client_2);
    }
    Ok(())
}

pub async fn do_async_close() -> AsyncResult<()> {
    *PD_ADDRS.write().unwrap() = None;
    let mut pool = TIKV_TNX_CONN_POOL.lock().unwrap();
    for _i in 0..pool.len() {
        let client = pool.pop_front();
        drop(client);
    }
    Ok(())
}

pub async fn do_async_connect(addrs: Vec<String>) -> AsyncResult<()> {
    do_async_txn_connect(addrs.clone()).await?;
    do_async_raw_connect(addrs).await?;
    Ok(())
}