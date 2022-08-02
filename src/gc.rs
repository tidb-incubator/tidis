use futures::FutureExt;
use slog::{debug, error, info};
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{self, Duration, MissedTickBehavior};

use crc::{Crc, CRC_16_XMODEM};

use crate::cluster::Cluster;
use crate::config::LOGGER;
use crate::metrics::GC_TASK_QUEUE_COUNTER;
use crate::tikv::encoding::{DataType, KeyDecoder};
use crate::tikv::errors::{AsyncResult, RTError};
use crate::tikv::{get_txn_client, KEY_ENCODER};
use crate::{
    async_deletion_enabled_or_default, async_gc_interval_or_default,
    async_gc_worker_queue_size_or_default,
};

const CRC16: Crc<u16> = Crc::<u16>::new(&CRC_16_XMODEM);

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
    topo: Cluster,
}

impl GcMaster {
    pub fn new(worker_num: usize, topo: Cluster) -> Self {
        let mut workers = Vec::with_capacity(worker_num);

        // create workers pool
        for id in 0..worker_num {
            let (tx, rx) = mpsc::channel::<GcTask>(async_gc_worker_queue_size_or_default());
            let worker = GcWorker::new(id, rx, tx);
            workers.push(worker);
        }

        GcMaster { workers, topo }
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
        let idx = CRC16.checksum(&task.to_bytes()) as usize % self.workers.len();
        debug!(LOGGER, "[GC] dispatch task {:?} to worker: {}", task, idx);
        self.workers[idx].add_task(task).await
    }

    // scan gc version keys
    // create gc task for each version key
    // dispatch gc task to workers
    pub async fn run(&mut self) -> AsyncResult<()> {
        let mut interval = time::interval(Duration::from_millis(async_gc_interval_or_default()));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let txn_client = get_txn_client()?;
        loop {
            interval.tick().await;

            if !async_deletion_enabled_or_default() {
                continue;
            }

            let mut ss = txn_client.newest_snapshot().await;
            let bound_range = KEY_ENCODER.encode_txnkv_gc_version_key_range();

            // TODO scan speed throttling
            let iter = ss.scan(bound_range, u32::MAX).await?;

            for kv in iter {
                let (user_key, version) = KeyDecoder::decode_key_gc_userkey_version(kv.0);

                let (slot_range_left, slot_range_right) = self.topo.myself_owned_slots();
                // crc16 to user key with hashtag `{}` support
                // check if user key contains valid hashtag
                let mut left_tag_idx = usize::MAX;
                let mut right_tag_idx = usize::MAX;
                for (idx, byte) in user_key.iter().enumerate() {
                    if byte == &b'{' {
                        left_tag_idx = idx;
                    }
                    if left_tag_idx != usize::MAX && byte == &b'}' {
                        right_tag_idx = idx;
                        break;
                    }
                }
                let user_key_hash: usize =
                    if right_tag_idx != usize::MAX && right_tag_idx - left_tag_idx > 1 {
                        // we have a valid hashtag, do crc16 to string to the content in hashtag
                        (CRC16.checksum(&user_key[left_tag_idx + 1..right_tag_idx]) & 0x3FFF).into()
                    } else {
                        (CRC16.checksum(&user_key) & 0x3FFF).into()
                    };

                // skip if user key is not owned by myself
                if user_key_hash < slot_range_left || user_key_hash > slot_range_right {
                    continue;
                }
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
    task_sets: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl GcWorker {
    pub fn new(id: usize, rx: Receiver<GcTask>, tx: Sender<GcTask>) -> Self {
        GcWorker {
            id,
            rx: Arc::new(Mutex::new(rx)),
            tx,
            task_sets: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    // queue task to channel
    pub async fn add_task(&mut self, task: GcTask) -> AsyncResult<()> {
        let bytes = task.to_bytes();
        let mut task_sets = self.task_sets.lock().await;
        if !task_sets.contains(&bytes) {
            debug!(LOGGER, "[GC] add task: {:?}", task);
            task_sets.insert(bytes);
            if let Err(e) = self.tx.send(task).await {
                error!(LOGGER, "[GC] send task to channel failed"; "error" => ?e);
                return Err(RTError::Owned(e.to_string()));
            } else {
                GC_TASK_QUEUE_COUNTER
                    .with_label_values(&[&self.id.to_string()])
                    .inc();
                return Ok(());
            }
        }
        Ok(())
    }

    pub async fn handle_task(&self, task: GcTask) -> AsyncResult<()> {
        let mut txn_client = get_txn_client()?;

        txn_client
            .exec_in_txn(None, |txn_rc| {
                let task = task.clone();
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
                                "[GC] async delete hash key {} with version {}", user_key, version
                            );
                            // delete all sub meta key of this key and version
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_sub_meta_key_range(&user_key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }

                            // delete all data key of this key and version
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_hash_data_key_range(&user_key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                        }
                        DataType::List => {
                            debug!(
                                LOGGER,
                                "[GC] async delete list key {} with version {}", user_key, version
                            );
                            // delete all data key of this key and version
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_list_data_key_range(&user_key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                        }
                        DataType::Set => {
                            debug!(
                                LOGGER,
                                "[GC] async delete set key {} with version {}", user_key, version
                            );
                            // delete all sub meta key of this key and version
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_sub_meta_key_range(&user_key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                            // delete all data key of this key and version
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_set_data_key_range(&user_key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                        }
                        DataType::Zset => {
                            debug!(
                                LOGGER,
                                "[GC] async delete zset key {} with version {}", user_key, version
                            );
                            // delete all sub meta key of this key and version
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_sub_meta_key_range(&user_key, version);
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
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_zset_data_key_range(&user_key, version);
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
                        KEY_ENCODER.encode_txnkv_gc_version_key(&user_key, version);
                    txn.delete(gc_version_key).await?;

                    Ok(())
                }
                .boxed()
            })
            .await?;

        // check the gc key in a small txn, avoid transaction confliction
        txn_client
            .exec_in_txn(None, |txn_rc| {
                let task = task.clone();
                async move {
                    let mut txn = txn_rc.lock().await;
                    let user_key = String::from_utf8_lossy(&task.user_key);
                    // also delete gc key if version in gc key is same as task.version
                    let gc_key = KEY_ENCODER.encode_txnkv_gc_key(&user_key);
                    let version = task.version;
                    match txn.get(gc_key.clone()).await? {
                        Some(v) => {
                            let ver = u16::from_be_bytes(v[..2].try_into().unwrap());
                            if ver == version {
                                debug!(
                                    LOGGER,
                                    "[GC] clean gc key for user key {} with version {}",
                                    user_key,
                                    version
                                );
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

    pub async fn run(self) {
        tokio::spawn(async move {
            info!(LOGGER, "[GC] start gc worker thread: {}", self.id);
            while let Some(task) = self.rx.lock().await.recv().await {
                match self.handle_task(task.clone()).await {
                    Ok(_) => {
                        debug!(LOGGER, "[GC] gc task done: {:?}", task);
                        self.task_sets.lock().await.remove(&task.to_bytes());
                        GC_TASK_QUEUE_COUNTER
                            .with_label_values(&[&self.id.to_string()])
                            .dec();
                    }
                    Err(e) => {
                        error!(LOGGER, "[GC] handle task error: {:?}", e);
                    }
                }
            }
            info!(LOGGER, "[GC] gc worker thread exit: {}", self.id);
        });
    }
}
