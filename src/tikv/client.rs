use tikv_client::{RawClient, Value, Key, Error, BoundRange, KvPair, ColumnFamily};

use super::sleep;


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
        match err {
            Error::RegionError(_) => true,
            Error::EntryNotFoundInRegionCache => true,
            Error::KvError { message: _ } => true,
            Error::MultipleKeyErrors(_) => true,
            _ => false,
        }
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

    pub async fn put(&self, key: Key, val: &str) -> Result<(), Error> {
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
        ttl: u64,
    ) -> Result<(Option<Value>, bool), Error> {
        let mut last_err: Option<Error> = None;
        for i in 0..self.retries {
            match self.client
                .with_atomic_for_cas()
                .compare_and_swap_with_ttl(key.clone(), prev_val.clone(), val.to_owned(), ttl)
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