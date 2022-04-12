use tikv_client::{RawClient, Value, Key, Error, BoundRange, KvPair, ColumnFamily};

use tikv_client::{
    TransactionClient,
    Timestamp,
    Result as TiKVResult,
    Snapshot,
    TransactionOptions,
    Transaction
};

use std::future::Future;
use super::{errors::AsyncResult};
use futures::future::{BoxFuture, FutureExt};

use crate::metrics::TIKV_CLIENT_RETRIES;

use super::sleep;

pub struct TxnClientWrapper<'a> {
    client: &'a TransactionClient,
    retries: u32,
}

impl TxnClientWrapper<'static> {
    pub fn new(c: &'static TransactionClient) -> Self {
        TxnClientWrapper {
            client: c,
            retries: 2000,
        }
    }

    pub async fn current_timestamp(&self) -> TiKVResult<Timestamp> {
        self.client.current_timestamp().await
    }

    pub async fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> Snapshot {
        self.client.snapshot(timestamp, options)
    }

    pub async fn newest_snapshot(&self) -> Snapshot {
        let options = TransactionOptions::new_pessimistic();
        let current_timestamp = self.current_timestamp().await.unwrap();
        self.snapshot(current_timestamp, options).await
    }

    pub async fn begin(&self) -> TiKVResult<Transaction> {
        // use pessimistic transaction for now
        self.client.begin_pessimistic().await
        //self.client.begin_optimistic().await
    }

    
    /// Auto begin new txn, call f with the txn, commit or callback due to the result
    pub async fn exec_in_txn<T, F>(&self, f: F) -> AsyncResult<T> where 
    F: FnOnce(&mut Transaction) -> BoxFuture<'_, AsyncResult<T>>,
    {
        // TODO: add retry policy

        // begin new transaction
        let mut txn = self.begin().await?;

        // call f
        let result = f(&mut txn).await;
        match result {
            Ok(res) => { 
                txn.commit().await?;
                Ok(res)
            },
            Err(err) => {
                // log and rollback
                txn.rollback().await?;
                Err(err)
            }
        }

    }



}

pub struct RawClientWrapper {
    client: Box<RawClient>,
    retries: u32,
}

impl RawClientWrapper {
    pub fn new(c: &RawClient) -> Self {
        RawClientWrapper { 
            client: Box::new(c.with_cf(ColumnFamily::Default)),
            retries: 2000,
        }
    }

    pub fn with_cf(&self, cf: ColumnFamily) -> RawClient {
        self.client.with_cf(cf)
    }

    fn error_retryable(&self, err: &Error) -> bool {
        let ret = match err {
            Error::RegionError(_) => true,
            Error::EntryNotFoundInRegionCache => true,
            Error::KvError { message: _ } => true,
            Error::MultipleKeyErrors(_) => true,
            _ => false,
        };
        if ret {
            TIKV_CLIENT_RETRIES.inc();
        }
        ret
    }

    pub async fn get(&self, key: Key) -> Result<Option<Value>, Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client.get(key.clone()).await {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(None),
        }
    }

    pub async fn put(&self, key: Key, val: Value) -> Result<(), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client.put(key.clone(), val.to_owned()).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn compare_and_swap(
        &self, 
        key: Key, 
        prev_val: Option<Value>, 
        val: Value,
    ) -> Result<(Option<Value>, bool), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .with_atomic_for_cas()
                .compare_and_swap(key.clone(), prev_val.clone(), val.to_owned())
                .await
            {
                Ok((val, swapped)) => {
                    return Ok((val, swapped));
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok((None, false)),
        }
    }

    pub async fn compare_and_swap_with_ttl(
        &self, 
        key: Key, 
        prev_val: Option<Value>, 
        val: Value,
        ttl: i64,
    ) -> Result<(Option<Value>, bool), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .with_atomic_for_cas()
                .compare_and_swap_with_ttl(key.clone(), prev_val.clone(), val.to_owned(), ttl as u64)
                .await
            {
                Ok((val, swapped)) => {
                    return Ok((val, swapped));
                }
                Err(err) => {
                    println!("Err: {:?}", &err);
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok((None, false)),
        }
    }

    pub async fn batch_delete(&self, keys: Vec<Key>) -> Result<(), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .batch_delete(keys.clone())
                .await
            {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn scan(&self, range: BoundRange, limit: u32) -> Result<Vec<KvPair>, Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .scan(range.clone(), limit)
                .await
            {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(Vec::new()),
        }
    }

    pub async fn batch_get(&self, keys: Vec<Key>) -> Result<Vec<KvPair>, Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .batch_get(keys.clone())
                .await
            {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(Vec::new()),
        }
    }

    pub async fn batch_put(&self, kvs: Vec<KvPair>) -> Result<(), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .batch_put(kvs.clone())
                .await
            {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn delete_range(&self, range: BoundRange) -> Result<(), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .delete_range(range.clone())
                .await
            {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn get_ttl(&self, key: Key) -> Result<i64, Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .get_ttl(key.clone())
                .await
            {
                Ok(val) => {
                    match val {
                        Some(ttl) => {
                            if ttl == 0 {
                                return Ok(-1);
                            }
                            return Ok(ttl as i64);
                        },
                        None => {
                            return Ok(-2);
                        }
                    }
                }
                Err(err) => {
                    if self.error_retryable(&err) {
                        last_err.replace(err);
                        sleep(std::cmp::min(2 + i, 200)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(-2),
        }
    }
}