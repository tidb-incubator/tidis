use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::Mutex;

use tikv_client::Error::StringError;
use tikv_client::{
    Backoff, BoundRange, ColumnFamily, Error, Key, KvPair, RawClient, Result as TiKVResult,
    RetryOptions, Transaction, TransactionClient, TransactionOptions, Value,
};

use crate::config::LOGGER;
use crate::{
    async_deletion_enabled_or_default, is_try_one_pc_commit, is_use_async_commit,
    is_use_pessimistic_txn, txn_lock_backoff_delay_attemps, txn_lock_backoff_delay_ms,
    txn_region_backoff_delay_attemps, txn_region_backoff_delay_ms, txn_retry_count,
};

use super::errors::{AsyncResult, RTError, KEY_VERSION_EXHUSTED_ERR};

use futures::future::BoxFuture;

use slog::{debug, error};

use crate::metrics::{
    ACQUIRE_LOCK_DURATION, TIKV_CLIENT_RETRIES, TIKV_ERR_COUNTER, TXN_COUNTER, TXN_DURATION,
    TXN_MECHANISM_COUNTER, TXN_RETRY_COUNTER, TXN_RETRY_ERR, TXN_RETRY_KIND_COUNTER,
};

use super::{sleep, KEY_ENCODER};
use crate::server::duration_to_sec;
use tokio::time::Instant;

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

        let mut mechanism: (&str, &str) = ("two_pc", "sync");
        txn_options = if is_try_one_pc_commit() {
            mechanism.0 = "one_pc";
            txn_options.try_one_pc()
        } else {
            txn_options
        };
        txn_options = if is_use_async_commit() {
            mechanism.1 = "async";
            txn_options.use_async_commit()
        } else {
            txn_options
        };

        TXN_COUNTER.inc();
        TXN_MECHANISM_COUNTER
            .with_label_values(&[mechanism.0, mechanism.1])
            .inc();

        self.client
            .begin_with_options(txn_options)
            .await
            .map_err(|err| {
                TIKV_ERR_COUNTER
                    .with_label_values(&["start_txn_error"])
                    .inc();
                err
            })
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
                let start_at = Instant::now();
                let result = f(txn).await;
                let duration = Instant::now() - start_at;
                TXN_DURATION.observe(duration_to_sec(duration));
                match result {
                    Ok(res) => Ok(res),
                    Err(err) => Err(err),
                }
            }
            None => {
                let mut retry_count = 0;
                while self.retries > 0 {
                    self.retries -= 1;

                    if retry_count > 0 {
                        TXN_RETRY_COUNTER.inc();
                        let kind = if is_use_pessimistic_txn() {
                            "pessimistic"
                        } else {
                            "optimistic"
                        };
                        TXN_RETRY_KIND_COUNTER.with_label_values(&[kind]).inc();
                    }
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
                    let start_at = Instant::now();
                    let result = f(txn_arc.clone()).await;
                    let duration = Instant::now() - start_at;
                    TXN_DURATION.observe(duration_to_sec(duration));

                    let start_at = Instant::now();
                    let mut txn = txn_arc.lock().await;
                    let duration = Instant::now() - start_at;
                    ACQUIRE_LOCK_DURATION.observe(duration_to_sec(duration));
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
                                    TXN_RETRY_ERR.with_label_values(&["retry_error"]).inc();
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
                                    TXN_RETRY_ERR.with_label_values(&["retry_error"]).inc();
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
                TXN_RETRY_ERR
                    .with_label_values(&["retry_count_exceeded"])
                    .inc();
                Err(RTError::TikvClient(Box::new(StringError(
                    "retry count exceeded".to_string(),
                ))))
            }
        }
    }
}

// get_version_for_new must be called outside of a MutexGuard, otherwise it will deadlock.
pub async fn get_version_for_new(key: &str, txn_rc: Arc<Mutex<Transaction>>) -> AsyncResult<u16> {
    // check if async deletion is enabled, return ASAP if not
    if !async_deletion_enabled_or_default() {
        return Ok(0);
    }

    let mut txn = txn_rc.lock().await;
    let gc_key = KEY_ENCODER.encode_txnkv_gc_key(key);
    let next_version = txn.get(gc_key).await?.map_or_else(
        || 0,
        |v| {
            let version = u16::from_be_bytes(v[..].try_into().unwrap());
            if version == u16::MAX {
                0
            } else {
                version + 1
            }
        },
    );
    // check next version available
    let gc_version_key = KEY_ENCODER.encode_txnkv_gc_version_key(key, next_version);
    txn.get(gc_version_key)
        .await?
        .map_or_else(|| Ok(next_version), |_| Err(KEY_VERSION_EXHUSTED_ERR))
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
                    TIKV_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
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
