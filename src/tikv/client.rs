use std::sync::Arc;
use tokio::sync::Mutex;

use tikv_client::Error::StringError;
use tikv_client::{
    Backoff, BoundRange, ColumnFamily, Error, Key, KvPair, RawClient, Result as TiKVResult,
    RetryOptions, Snapshot, Timestamp, TimestampExt, Transaction, TransactionClient,
    TransactionOptions, Value,
};

use crate::config::LOGGER;
use crate::{
    is_try_one_pc_commit, is_use_async_commit, is_use_pessimistic_txn,
    txn_lock_backoff_delay_attemps, txn_lock_backoff_delay_ms, txn_region_backoff_delay_attemps,
    txn_region_backoff_delay_ms, txn_retry_count,
};

use super::errors::{AsyncResult, RTError};

use futures::future::BoxFuture;

use slog::{debug, error};

use crate::metrics::TIKV_CLIENT_RETRIES;

use super::sleep;

const MAX_DELAY_MS: u64 = 500;

pub struct TxnClientWrapper<'a> {
    client: &'a TransactionClient,
    retries: u32,
}

impl TxnClientWrapper<'static> {
    pub fn new(c: &'static TransactionClient) -> Self {
        TxnClientWrapper {
            client: c,
            retries: txn_retry_count(),
        }
    }

    pub async fn current_timestamp(&self) -> TiKVResult<Timestamp> {
        self.client.current_timestamp().await
    }

    pub fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> Snapshot {
        self.client.snapshot(timestamp, options)
    }

    pub async fn snapshot_from_txn(&self, txn: Arc<Mutex<Transaction>>) -> Snapshot {
        // add retry options
        let region_backoff = Backoff::no_jitter_backoff(
            txn_region_backoff_delay_ms(),
            MAX_DELAY_MS,
            txn_region_backoff_delay_attemps(),
        );
        let lock_backoff = Backoff::no_jitter_backoff(
            txn_lock_backoff_delay_ms(),
            MAX_DELAY_MS,
            txn_lock_backoff_delay_attemps(),
        );
        let retry_options = RetryOptions::new(region_backoff, lock_backoff);

        let options = if is_use_pessimistic_txn() {
            TransactionOptions::new_pessimistic().retry_options(retry_options)
        } else {
            TransactionOptions::new_optimistic().retry_options(retry_options)
        };
        let txn = txn.lock().await;
        let ts = txn.start_timestamp();
        self.snapshot(ts, options)
    }

    pub async fn newest_snapshot(&self) -> Snapshot {
        // add retry options
        let region_backoff = Backoff::no_jitter_backoff(
            txn_region_backoff_delay_ms(),
            MAX_DELAY_MS,
            txn_region_backoff_delay_attemps(),
        );
        let lock_backoff = Backoff::no_jitter_backoff(
            txn_lock_backoff_delay_ms(),
            MAX_DELAY_MS,
            txn_lock_backoff_delay_attemps(),
        );
        let retry_options = RetryOptions::new(region_backoff, lock_backoff);

        let options = if is_use_pessimistic_txn() {
            TransactionOptions::new_pessimistic().retry_options(retry_options)
        } else {
            TransactionOptions::new_optimistic().retry_options(retry_options)
        };

        let current_timestamp = match self.current_timestamp().await {
            Ok(ts) => ts,
            Err(e) => {
                error!(
                    LOGGER,
                    "get current timestamp failed: {:?}, use max for this query", e
                );
                Timestamp::from_version(i64::MAX as u64)
            }
        };
        self.snapshot(current_timestamp, options)
    }

    pub async fn begin(&self) -> TiKVResult<Transaction> {
        // add retry options
        let region_backoff = Backoff::no_jitter_backoff(
            txn_region_backoff_delay_ms(),
            MAX_DELAY_MS,
            txn_region_backoff_delay_attemps(),
        );
        let lock_backoff = Backoff::no_jitter_backoff(
            txn_lock_backoff_delay_ms(),
            MAX_DELAY_MS,
            txn_lock_backoff_delay_attemps(),
        );
        let retry_options = RetryOptions::new(region_backoff, lock_backoff);

        let mut txn_options = if is_use_pessimistic_txn() {
            TransactionOptions::new_pessimistic().retry_options(retry_options)
        } else {
            TransactionOptions::new_optimistic().retry_options(retry_options)
        };
        txn_options = if is_use_async_commit() {
            txn_options.use_async_commit()
        } else {
            txn_options
        };
        txn_options = if is_try_one_pc_commit() {
            txn_options.try_one_pc()
        } else {
            txn_options
        };

        self.client.begin_with_options(txn_options).await
    }

    fn error_retryable(&self, err: &Error) -> bool {
        let ret = matches!(
            err,
            Error::RegionError(_)
                | Error::EntryNotFoundInRegionCache
                | Error::KvError { message: _ }
                | Error::MultipleKeyErrors(_)
                | Error::PessimisticLockError {
                    inner: _,
                    success_keys: _,
                }
        );

        if ret {
            TIKV_CLIENT_RETRIES.inc();
        }
        ret
    }

    /// Auto begin new txn, call f with the txn, commit or callback due to the result
    pub async fn exec_in_txn<T, F>(
        &mut self,
        txn: Option<Arc<Mutex<Transaction>>>,
        f: F,
    ) -> AsyncResult<T>
    where
        F: FnOnce(Arc<Mutex<Transaction>>) -> BoxFuture<'static, AsyncResult<T>> + Clone,
    {
        match txn {
            Some(txn) => {
                // call f
                let result = f(txn).await;
                match result {
                    Ok(res) => Ok(res),
                    Err(err) => Err(err),
                }
            }
            None => {
                let mut retry_count = 0;
                while self.retries > 0 {
                    self.retries -= 1;
                    retry_count += 1;

                    let f = f.clone();

                    // begin new transaction
                    let txn = match self.begin().await {
                        Ok(t) => t,
                        Err(e) => {
                            error!(LOGGER, "error to begin new transaction: {}", e);
                            if self.retries == 0 {
                                return Err(RTError::TikvClient(Box::new(e)));
                            }
                            continue;
                        }
                    };

                    let txn_arc = Arc::new(Mutex::new(txn));

                    // call f
                    let result = f(txn_arc.clone()).await;
                    let mut txn = txn_arc.lock().await;
                    match result {
                        Ok(res) => match txn.commit().await {
                            Ok(_) => {
                                return Ok(res);
                            }
                            Err(e) => {
                                error!(LOGGER, "error to commit transaction: {}", e);
                                if self.error_retryable(&e) {
                                    if self.retries == 0 {
                                        return Err(RTError::TikvClient(Box::new(e)));
                                    }
                                    debug!(
                                        LOGGER,
                                        "retry transaction in the caller caused by error {}", e
                                    );
                                    continue;
                                }
                            }
                        },
                        Err(e) => {
                            txn.rollback().await?;
                            error!(LOGGER, "error occured so rollback transaction: {}", e);
                            if let RTError::TikvClient(client_err) = e {
                                if self.error_retryable(&client_err) {
                                    if self.retries == 0 {
                                        return Err(RTError::TikvClient(client_err));
                                    }
                                    debug!(
                                        LOGGER,
                                        "retry transaction in the caller caused by error {}",
                                        client_err
                                    );
                                    continue;
                                } else {
                                    return Err(RTError::TikvClient(client_err));
                                }
                            } else {
                                return Err(e);
                            }
                        }
                    }

                    // backoff retry
                    sleep(std::cmp::min(2 + retry_count * 10, 200)).await;
                }
                error!(LOGGER, "transaction retry count reached limit");
                Err(RTError::TikvClient(Box::new(StringError(
                    "retry count exceeded".to_string(),
                ))))
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

    #[allow(dead_code)]
    pub fn with_cf(&self, cf: ColumnFamily) -> RawClient {
        self.client.with_cf(cf)
    }

    fn error_retryable(&self, err: &Error) -> bool {
        let ret = matches!(
            err,
            Error::RegionError(_)
                | Error::EntryNotFoundInRegionCache
                | Error::KvError { message: _ }
                | Error::MultipleKeyErrors(_)
        );

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
            match self
                .client
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

    #[allow(dead_code)]
    pub async fn batch_delete(&self, keys: Vec<Key>) -> Result<(), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client.batch_delete(keys.clone()).await {
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

    #[allow(dead_code)]
    pub async fn scan(&self, range: BoundRange, limit: u32) -> Result<Vec<KvPair>, Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client.scan(range.clone(), limit).await {
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
            match self.client.batch_get(keys.clone()).await {
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
            match self.client.batch_put(kvs.clone()).await {
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

    #[allow(dead_code)]
    pub async fn delete_range(&self, range: BoundRange) -> Result<(), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client.delete_range(range.clone()).await {
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
}
