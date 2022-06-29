use futures::FutureExt;
use slog::{debug, error, info};
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{self, Duration, MissedTickBehavior};

use crc::{Algorithm, Crc, CRC_16_IBM_SDLC, CRC_32_ISCSI};

use crate::config::LOGGER;
use crate::tikv::encoding::{DataType, KeyDecoder};
use crate::tikv::errors::{AsyncResult, RTError};
use crate::tikv::{get_txn_client, KEY_ENCODER};

const X25: Crc<u16> = Crc::<u16>::new(&CRC_16_IBM_SDLC);

#[derive(Debug, Clone)]
pub struct GcTask {
    key_type: DataType,
    user_key: Vec<u8>,
    version: u16,
}

impl GcTask {
    fn new(key_type: DataType, user_key: Vec<u8>, version: u16) -> GcTask {
        GcTask {
            key_type,
            user_key,
            version,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(3 + self.user_key.len());
        bytes.push(self.key_type.clone() as u8);
        bytes.extend_from_slice(&self.user_key);
        bytes.extend_from_slice(&self.version.to_be_bytes());
        bytes
    }
}

#[derive(Debug)]
pub struct GcMaster {
    workers: Vec<GcWorker>,
}

impl GcMaster {
    pub fn new(worker_num: usize) -> Self {
        let mut workers = Vec::with_capacity(worker_num);

        // create workers pool
        for id in 0..worker_num {
            let (tx, rx) = mpsc::channel::<GcTask>(1000);
            let worker = GcWorker::new(id, rx, tx);
            workers.push(worker);
        }

        GcMaster { workers }
    }

    pub async fn start_workers(&self) {
        // run all workers
        // worker will wait for task from channel
        for worker in &self.workers {
            worker.clone().run().await;
        }
    }

    // dispatch task to a worker
    pub async fn dispatch_task(&mut self, task: GcTask) -> AsyncResult<()> {
        // calculate task hash and dispatch to worker
        let idx = X25.checksum(&task.to_bytes()) as usize % self.workers.len();
        debug!(LOGGER, "dispatch task to worker: {}", idx);
        self.workers[idx].add_task(task).await
    }

    // scan gc version keys
    // create gc task for each version key
    // dispatch gc task to workers
    pub async fn run(&mut self) -> AsyncResult<()> {
        let mut interval = time::interval(Duration::from_millis(10000));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let txn_client = get_txn_client()?;
        loop {
            interval.tick().await;

            let mut ss = txn_client.newest_snapshot().await;
            let bound_range = KEY_ENCODER.encode_txnkv_gc_version_key_range();

            // TODO scan speed throttling
            let iter = ss.scan(bound_range, u32::MAX).await?;

            for kv in iter {
                let (user_key, version) = KeyDecoder::decode_key_gc_userkey_version(kv.0);
                let key_type = match kv.1[0] {
                    0 => DataType::String,
                    1 => DataType::Hash,
                    2 => DataType::List,
                    3 => DataType::Set,
                    4 => DataType::Zset,
                    _ => DataType::Null,
                };
                let task = GcTask::new(key_type, user_key, version);
                self.dispatch_task(task).await?;
            }
        }
    }

    pub fn shutdown(&self) {}
}

#[derive(Debug, Clone)]
struct GcWorker {
    id: usize,

    rx: Arc<Mutex<Receiver<GcTask>>>,
    tx: Sender<GcTask>,

    // check task already in queue, avoid duplicate task
    task_sets: HashSet<Vec<u8>>,
}

impl GcWorker {
    pub fn new(id: usize, rx: Receiver<GcTask>, tx: Sender<GcTask>) -> Self {
        GcWorker {
            id,
            rx: Arc::new(Mutex::new(rx)),
            tx,
            task_sets: HashSet::new(),
        }
    }

    // queue task to channel
    pub async fn add_task(&mut self, task: GcTask) -> AsyncResult<()> {
        let bytes = task.to_bytes();
        if !self.task_sets.contains(&bytes) {
            self.task_sets.insert(bytes);
            if let Err(e) = self.tx.send(task).await {
                error!(LOGGER, "send task to channel failed"; "error" => ?e);
                return Err(RTError::Owned(e.to_string()));
            } else {
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn queued_task(&self) -> usize {
        self.task_sets.len()
    }

    pub async fn handle_task(&self, task: GcTask) -> AsyncResult<()> {
        let mut txn_client = get_txn_client()?;
        txn_client
            .exec_in_txn(None, |txn_rc| {
                async move {
                    let mut txn = txn_rc.lock().await;
                    let user_key = String::from_utf8_lossy(&task.user_key);
                    let version = task.version;
                    match task.key_type {
                        DataType::String => {
                            panic!("string not support async deletion");
                        }
                        DataType::Hash => {
                            debug!(
                                LOGGER,
                                "async delete hash key {} with version {}", user_key, version
                            );
                            // delete all sub meta key of this key and version
                            let bound_range = KEY_ENCODER
                                .encode_txnkv_sub_meta_key_range(&user_key, task.version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }

                            // delete all data key of this key and version
                            let bound_range = KEY_ENCODER
                                .encode_txnkv_hash_data_key_range(&user_key, task.version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                        }
                        DataType::List => {
                            debug!(
                                LOGGER,
                                "async delete list key {} with version {}", user_key, version
                            );
                            // delete all data key of this key and version
                            let bound_range = KEY_ENCODER
                                .encode_txnkv_list_data_key_range(&user_key, task.version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                        }
                        DataType::Set => {
                            debug!(
                                LOGGER,
                                "async delete set key {} with version {}", user_key, version
                            );
                            // delete all sub meta key of this key and version
                            let bound_range = KEY_ENCODER
                                .encode_txnkv_sub_meta_key_range(&user_key, task.version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                            // delete all data key of this key and version
                            let bound_range = KEY_ENCODER
                                .encode_txnkv_set_data_key_range(&user_key, task.version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                        }
                        DataType::Zset => {
                            debug!(
                                LOGGER,
                                "async delete zset key {} with version {}", user_key, version
                            );
                            // delete all sub meta key of this key and version
                            let bound_range = KEY_ENCODER
                                .encode_txnkv_sub_meta_key_range(&user_key, task.version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }

                            // delete all score key of this key and version
                            let bound_range = KEY_ENCODER.encode_txnkv_zset_score_key_range(
                                &String::from_utf8_lossy(&task.user_key),
                                task.version,
                            );
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }

                            // delete all data key of this key and version
                            let bound_range = KEY_ENCODER
                                .encode_txnkv_zset_data_key_range(&user_key, task.version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                        }
                        DataType::Null => {
                            panic!("unknown data type to do async deletion");
                        }
                    }

                    // delete gc version key
                    let gc_version_key =
                        KEY_ENCODER.encode_txnkv_gc_version_key(&user_key, task.version);
                    txn.delete(gc_version_key).await?;

                    // also delete gc key if version in gc key is same as task.version
                    let gc_key = KEY_ENCODER.encode_txnkv_gc_key(&user_key);
                    match txn.get(gc_key.clone()).await? {
                        Some(v) => {
                            let version = u16::from_be_bytes(v[..2].try_into().unwrap());
                            if version == task.version {
                                debug!(LOGGER, "clean gc key for user key {}", user_key);
                                txn.delete(gc_key).await?;
                            }
                        }
                        None => {}
                    }
                    Ok(())
                }
                .boxed()
            })
            .await
    }

    pub async fn run(mut self) {
        tokio::spawn(async move {
            info!(LOGGER, "start gc worker thread: {}", self.id);
            while let Some(task) = self.rx.lock().await.recv().await {
                match self.handle_task(task.clone()).await {
                    Ok(_) => {
                        self.task_sets.remove(&task.to_bytes());
                    }
                    Err(e) => {
                        error!(LOGGER, "handle task error: {:?}", e);
                    }
                }
            }
        });
    }
}
