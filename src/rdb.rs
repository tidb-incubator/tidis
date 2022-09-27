use crate::config::LOGGER;
use crate::tikv::encoding::{DataType, KeyDecoder};
use crate::tikv::errors::{
    AsyncResult, REDIS_DUMPING_ERR, REDIS_LIST_TOO_LARGE_ERR, REDIS_VALUE_IS_NOT_INTEGER_ERR,
};
use crate::tikv::{get_txn_client, KEY_ENCODER};
use crc::{Crc, Digest, CRC_64_GO_ISO};
use futures::FutureExt;
use slog::debug;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::RangeFrom;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tikv_client::{BoundRange, Key, Transaction, Value};
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt};

const RDB_VERSION: u32 = 9;
const REDIS_VERSION: &str = "6.0.16";

const RDB_6BITLEN: u8 = 0;
const RDB_14BITLEN: u8 = 1;
const RDB_32BITLEN: u8 = 0x80;
const RDB_64BITLEN: u8 = 0x81;

const ZIP_LIST_HEADER_SIZE: usize = 10;
const ZIP_BIG_PREV_LEN: usize = 254;
const ZIP_END: u8 = 255;
const ZIP_INT_16B: u8 = 0xc0;
const ZIP_INT_32B: u8 = 0xc0 | 1 << 4;
const ZIP_INT_64B: u8 = 0xc0 | 2 << 4;
const ZIP_INT_24B: u8 = 0xc0 | 3 << 4;
const ZIP_INT_8B: u8 = 0xfe;

const LEN_SPECIAL: u8 = 3;
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const ENCODE_INT8_PREFIX: u8 = (LEN_SPECIAL << 6) | RDB_ENC_INT8;
const ENCODE_INT16_PREFIX: u8 = (LEN_SPECIAL << 6) | RDB_ENC_INT16;
const ENCODE_INT32_PREFIX: u8 = (LEN_SPECIAL << 6) | RDB_ENC_INT32;

const RDB_OPCODE_AUX: u8 = 250;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
const RDB_OPCODE_SELECTDB: u8 = 254;
const RDB_OPCODE_EOF: u8 = 255;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;

static DUMPING: AtomicBool = AtomicBool::new(false);

pub struct RDB;

struct RDBEncoder {
    writer: Pin<Box<dyn AsyncWrite + Send>>,
    buf: [u8; 16],
}

impl RDBEncoder {
    pub fn new(dump_file: File) -> Self {
        RDBEncoder {
            writer: Box::pin(dump_file),
            buf: [0; 16],
        }
    }

    pub async fn save_header<'a>(&mut self, digest: &mut Digest<'a, u64>) -> AsyncResult<()> {
        self.write(format!("REDIS{:04}", RDB_VERSION).as_bytes(), digest)
            .await?;
        self.save_aux_field("redis-ver", REDIS_VERSION, digest)
            .await?;
        self.save_aux_field("redis-bits", &(usize::BITS * 8).to_string(), digest)
            .await?;
        self.save_aux_field(
            "ctime",
            &SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string(),
            digest,
        )
        .await?;

        self.write(&RDB_OPCODE_SELECTDB.to_ne_bytes(), digest)
            .await?;
        // all data saved in db 0 by default
        self.save_length(0, digest).await?;

        Ok(())
    }

    pub async fn save_kv_pair<'a>(
        &mut self,
        user_key: &str,
        val: Value,
        readonly_txn: &mut Transaction,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        let ttl = KeyDecoder::decode_key_ttl(&val);
        if ttl > 0 {
            self.write(&RDB_OPCODE_EXPIRETIME_MS.to_le_bytes(), digest)
                .await?;
            self.write(&ttl.to_le_bytes(), digest).await?;
        }

        match KeyDecoder::decode_key_type(&val) {
            DataType::String => self.save_string_obj(user_key, val, digest).await,
            DataType::Hash => {
                self.save_hash_obj(user_key, val, readonly_txn, digest)
                    .await
            }
            DataType::List => {
                self.save_list_obj(user_key, val, readonly_txn, digest)
                    .await
            }
            DataType::Set => self.save_set_obj(user_key, val, readonly_txn, digest).await,
            DataType::Zset => {
                self.save_zset_obj(user_key, val, readonly_txn, digest)
                    .await
            }
            _ => panic!("RDB decode unknown key type"),
        }
    }

    pub async fn save_footer<'a>(&mut self, mut digest: Digest<'a, u64>) -> AsyncResult<()> {
        self.write(&RDB_OPCODE_EOF.to_ne_bytes(), &mut digest)
            .await?;
        let checksum = digest.finalize();
        self.writer.write_all(&checksum.to_le_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn save_string_obj<'a>(
        &mut self,
        user_key: &str,
        val: Value,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        let data_val = KeyDecoder::decode_key_string_value(&val);
        let data_val_str = String::from_utf8_lossy(data_val.as_slice());
        debug!(
            LOGGER,
            "[saving string obj] key: {}, value: {}", user_key, data_val_str
        );
        self.write(&RDB_TYPE_STRING.to_ne_bytes(), digest).await?;
        self.save_raw_string(user_key, digest).await?;
        self.save_raw_string(&data_val_str, digest).await?;
        Ok(())
    }

    async fn save_hash_obj<'a>(
        &mut self,
        user_key: &str,
        val: Value,
        readonly_txn: &mut Transaction,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        self.write(&RDB_TYPE_HASH.to_ne_bytes(), digest).await?;
        self.save_raw_string(user_key, digest).await?;

        let key = user_key.to_owned();
        let (_, version, _) = KeyDecoder::decode_key_meta(&val);
        let bound_range: BoundRange = (KEY_ENCODER.encode_txnkv_hash_data_key_start(&key, version)
            ..KEY_ENCODER.encode_txnkv_hash_data_key_end(&key, version))
            .into();
        let iter = readonly_txn.scan(bound_range, u32::MAX).await?;
        let hash_map: HashMap<String, String> = iter
            .map(|kv| {
                let field =
                    String::from_utf8(KeyDecoder::decode_key_hash_userkey_from_datakey(&key, kv.0))
                        .unwrap();
                let field_val = String::from_utf8_lossy(kv.1.as_slice()).to_string();
                (field, field_val)
            })
            .collect();
        let hash_map_vec: Vec<String> = hash_map
            .iter()
            .map(|(field, value)| field.to_string() + ":" + value)
            .collect();
        debug!(
            LOGGER,
            "[saving hash obj] key: {}, fields: [{}]",
            user_key,
            hash_map_vec.join(", ")
        );

        self.save_length(hash_map.len(), digest).await?;
        for kv in hash_map {
            self.save_raw_string(&kv.0, digest).await?;
            self.save_raw_string(&kv.1, digest).await?;
        }

        Ok(())
    }

    async fn save_list_obj<'a>(
        &mut self,
        user_key: &str,
        val: Value,
        readonly_txn: &mut Transaction,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        self.write(&RDB_TYPE_LIST_ZIPLIST.to_ne_bytes(), digest)
            .await?;
        self.save_raw_string(user_key, digest).await?;

        let key = user_key.to_owned();
        let (_, version, left, right) = KeyDecoder::decode_key_list_meta(&val);
        let data_key_start = KEY_ENCODER.encode_txnkv_list_data_key(&key, left, version);
        let range: RangeFrom<Key> = data_key_start..;
        let from_range: BoundRange = range.into();
        let iter = readonly_txn
            .scan(from_range, (right - left).try_into().unwrap())
            .await?;
        let data_vals: Vec<String> = iter
            .map(|kv| String::from_utf8_lossy(kv.1.as_slice()).to_string())
            .collect();
        debug!(
            LOGGER,
            "[saving list obj] key: {}, members: [{}]",
            user_key,
            data_vals.join(", ")
        );

        self.save_ziplist(data_vals, digest).await?;

        Ok(())
    }

    async fn save_set_obj<'a>(
        &mut self,
        user_key: &str,
        val: Value,
        readonly_txn: &mut Transaction,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        self.write(&RDB_TYPE_SET.to_ne_bytes(), digest).await?;
        self.save_raw_string(user_key, digest).await?;

        let key = user_key.to_owned();
        let (_, version, _) = KeyDecoder::decode_key_meta(&val);
        let bound_range = KEY_ENCODER.encode_txnkv_set_data_key_range(&key, version);
        let iter = readonly_txn.scan_keys(bound_range, u32::MAX).await?;
        let members: Vec<String> = iter
            .map(|k| {
                String::from_utf8(KeyDecoder::decode_key_set_member_from_datakey(&key, k)).unwrap()
            })
            .collect();
        debug!(
            LOGGER,
            "[saving set obj] key: {}, members: [{}]",
            user_key,
            members.join(", ")
        );

        self.save_length(members.len(), digest).await?;
        for member in members {
            self.save_raw_string(&member, digest).await?;
        }

        Ok(())
    }

    async fn save_zset_obj<'a>(
        &mut self,
        user_key: &str,
        val: Value,
        readonly_txn: &mut Transaction,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        self.write(&RDB_TYPE_ZSET_ZIPLIST.to_ne_bytes(), digest)
            .await?;
        self.save_raw_string(user_key, digest).await?;

        let key = user_key.to_owned();
        let (_, version, _) = KeyDecoder::decode_key_meta(&val);
        let sub_meta_key_range = KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
        let size: i64 = readonly_txn
            .scan(sub_meta_key_range, u32::MAX)
            .await?
            .map(|kv| i64::from_be_bytes(kv.1.try_into().unwrap()))
            .sum();
        let bound_range = KEY_ENCODER.encode_txnkv_zset_score_key_range(&key, version);
        let iter = readonly_txn
            .scan(bound_range, size.try_into().unwrap())
            .await?;
        let member_score_map: HashMap<String, f64> = iter
            .map(|kv| {
                let member = String::from_utf8_lossy(kv.1.as_slice()).to_string();
                let score = KeyDecoder::decode_key_zset_score_from_scorekey(&key, kv.0);
                (member, score)
            })
            .collect();
        let member_score_vec: Vec<String> = member_score_map
            .iter()
            .map(|(member, score)| member.to_string() + ":" + score.to_string().as_str())
            .collect();
        debug!(
            LOGGER,
            "[save zset obj] key: {}, members: [{}]",
            user_key,
            member_score_vec.join(", ")
        );

        let mut zl_elements: Vec<String> = Vec::with_capacity(member_score_map.len() * 2);
        for (member, score) in member_score_map {
            zl_elements.push(member);
            zl_elements.push(score.to_string());
        }
        self.save_ziplist(zl_elements, digest).await?;

        Ok(())
    }

    async fn save_aux_field<'a>(
        &mut self,
        key: &str,
        val: &str,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        self.write(&RDB_OPCODE_AUX.to_ne_bytes(), digest).await?;
        self.save_raw_string(key, digest).await?;
        self.save_raw_string(val, digest).await?;
        Ok(())
    }

    async fn save_ziplist<'a>(
        &mut self,
        values: Vec<String>,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        if values.len() > u16::MAX as usize {
            return Err(REDIS_LIST_TOO_LARGE_ERR);
        }

        // zip list header + EOF
        let mut zl_bytes = ZIP_LIST_HEADER_SIZE + 1;
        let mut zl_tail_offset = ZIP_LIST_HEADER_SIZE;
        let mut zl_buf = vec![0; ZIP_LIST_HEADER_SIZE];

        let mut prev_len = 0;
        for i in 0..values.len() {
            let mut entry = self.encode_ziplist_entry(prev_len, values.get(i).unwrap())?;
            prev_len = entry.len();
            zl_buf.append(&mut entry);
            zl_bytes += prev_len;
            if i < values.len() - 1 {
                zl_tail_offset += prev_len;
            }
        }
        zl_buf.push(ZIP_END);

        zl_buf[0..4].copy_from_slice(&(zl_bytes as u32).to_le_bytes());
        zl_buf[4..8].copy_from_slice(&(zl_tail_offset as u32).to_le_bytes());
        zl_buf[8..10].copy_from_slice(&(values.len() as u16).to_le_bytes());
        unsafe {
            self.save_raw_string(&String::from_utf8_unchecked(zl_buf), digest)
                .await?;
        }
        Ok(())
    }

    fn encode_ziplist_entry(&mut self, prev_len: usize, val: &str) -> AsyncResult<Vec<u8>> {
        let mut entry_buf = vec![];

        if prev_len < ZIP_BIG_PREV_LEN {
            entry_buf.push(prev_len as u8);
        } else {
            entry_buf.push(ZIP_BIG_PREV_LEN as u8);
            entry_buf.append(&mut (prev_len as u32).to_le_bytes().to_vec());
        }

        match val.parse::<i64>() {
            Ok(int_val) => {
                if (0..13).contains(&int_val) {
                    entry_buf.push(0xF0 | ((int_val + 1) as u8));
                } else if ((-1 << 7)..(1 << 7)).contains(&int_val) {
                    entry_buf.push(ZIP_INT_8B);
                    entry_buf.append(&mut (int_val as i8).to_le_bytes().to_vec());
                } else if ((-1 << 15)..(1 << 15)).contains(&int_val) {
                    entry_buf.push(ZIP_INT_16B);
                    entry_buf.append(&mut (int_val as i16).to_le_bytes().to_vec());
                } else if ((-1 << 23)..(1 << 23)).contains(&int_val) {
                    entry_buf.push(ZIP_INT_24B);
                    entry_buf.append(&mut (int_val as i32).to_le_bytes()[0..3].to_vec());
                } else if ((-1 << 31)..(1 << 31)).contains(&int_val) {
                    entry_buf.push(ZIP_INT_32B);
                    entry_buf.append(&mut (int_val as i32).to_le_bytes().to_vec());
                } else {
                    entry_buf.push(ZIP_INT_64B);
                    entry_buf.append(&mut int_val.to_le_bytes().to_vec());
                }
            }
            Err(_) => {
                let len = val.len();
                if len < (1 << 6) {
                    entry_buf.push(len as u8);
                } else if len < (1 << 14) {
                    entry_buf.push(((len >> 8) as u8) | (RDB_14BITLEN << 6));
                    entry_buf.push(len as u8);
                } else if len <= u32::MAX as usize {
                    entry_buf.push(RDB_32BITLEN);
                    entry_buf.append(&mut (len as u32).to_ne_bytes().to_vec());
                } else {
                    entry_buf.push(RDB_64BITLEN);
                    entry_buf.append(&mut (len as u64).to_ne_bytes().to_vec());
                }
                entry_buf.append(&mut val.as_bytes().to_vec());
            }
        }

        Ok(entry_buf)
    }

    async fn save_raw_string<'a>(
        &mut self,
        s: &str,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        match self.try_integer_encoding(s, digest).await {
            Ok(_) => Ok(()),
            Err(_) => {
                self.save_length(s.len(), digest).await?;
                self.write(s.as_bytes(), digest).await?;
                Ok(())
            }
        }
    }

    async fn try_integer_encoding<'a>(
        &mut self,
        s: &str,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        let int_val = s.parse::<i64>()?;

        if int_val >= i8::MIN as i64 && int_val <= i8::MAX as i64 {
            self.buf[0] = ENCODE_INT8_PREFIX;
            self.buf[1..2].copy_from_slice(&(int_val as i8).to_le_bytes());
            self.write_buf(2, digest).await?;
        } else if int_val >= i16::MIN as i64 && int_val <= i16::MAX as i64 {
            self.buf[0] = ENCODE_INT16_PREFIX;
            self.buf[1..3].copy_from_slice(&(int_val as i16).to_le_bytes());
            self.write_buf(3, digest).await?;
        } else if int_val >= i32::MIN as i64 && int_val <= i32::MAX as i64 {
            self.buf[0] = ENCODE_INT32_PREFIX;
            self.buf[1..5].copy_from_slice(&(int_val as i32).to_le_bytes());
            self.write_buf(5, digest).await?;
        } else {
            // out of i32 range, waive integer encoding
            return Err(REDIS_VALUE_IS_NOT_INTEGER_ERR);
        }

        Ok(())
    }

    async fn save_length<'a>(
        &mut self,
        len: usize,
        digest: &mut Digest<'a, u64>,
    ) -> AsyncResult<()> {
        if len < (1 << 6) {
            self.buf[0] = (len as u8) | (RDB_6BITLEN << 6);
            self.write_buf(1, digest).await?;
        } else if len < (1 << 14) {
            self.buf[0] = ((len >> 8) as u8) | (RDB_14BITLEN << 6);
            self.buf[1] = len as u8;
            self.write_buf(2, digest).await?;
        } else if len <= u32::MAX as usize {
            self.buf[0] = RDB_32BITLEN;
            self.buf[1..5].copy_from_slice(&(len as u32).to_ne_bytes());
            self.write_buf(5, digest).await?;
        } else {
            self.buf[0] = RDB_64BITLEN;
            self.buf[1..9].copy_from_slice(&(len as u64).to_ne_bytes());
            self.write_buf(9, digest).await?;
        }

        Ok(())
    }

    async fn write<'a>(&mut self, bytes: &[u8], digest: &mut Digest<'a, u64>) -> AsyncResult<()> {
        self.writer.write_all(bytes).await?;
        digest.update(bytes);
        Ok(())
    }

    async fn write_buf<'a>(&mut self, end: usize, digest: &mut Digest<'a, u64>) -> AsyncResult<()> {
        self.writer.write_all(&self.buf[..end]).await?;
        self.buf.fill(0);
        digest.update(&self.buf[..end]);
        Ok(())
    }
}

impl RDB {
    pub async fn dump() -> AsyncResult<()> {
        if DUMPING.load(Ordering::Relaxed) {
            return Err(REDIS_DUMPING_ERR);
        }

        DUMPING.store(true, Ordering::Relaxed);
        get_txn_client()?
            .exec_in_txn(None, |txn_rc| {
                async move {
                    let rdb_file = RDB::create_rdb();
                    let crc64 = Crc::<u64>::new(&CRC_64_GO_ISO);
                    let mut digest = crc64.digest();
                    let mut encoder = RDBEncoder::new(rdb_file);
                    encoder.save_header(&mut digest).await?;

                    //TODO call update_service_gc_safepoint with current timestamp (need client-rust support)
                    let mut readonly_txn = txn_rc.lock().await;
                    let mut left_bound = KEY_ENCODER.encode_txnkv_string("");
                    let mut last_round_iter_count = 1;
                    while last_round_iter_count > 0 {
                        let range = left_bound.clone()..KEY_ENCODER.encode_txnkv_keyspace_end();
                        let bound_range: BoundRange = range.into();
                        //TODO configurable scan limit or stream scan
                        let iter = readonly_txn.scan(bound_range, 100).await?;
                        last_round_iter_count = 0;
                        for kv in iter {
                            if kv.0 == left_bound {
                                continue;
                            }
                            last_round_iter_count += 1;
                            left_bound = kv.0.clone();

                            let (user_key, is_meta_key) =
                                KeyDecoder::decode_key_userkey_from_metakey(&kv.0);
                            if is_meta_key {
                                let user_key_str = String::from_utf8(user_key).unwrap();
                                encoder
                                    .save_kv_pair(
                                        &user_key_str,
                                        kv.1,
                                        &mut readonly_txn,
                                        &mut digest,
                                    )
                                    .await?;
                            }
                        }
                    }
                    //TODO call update_service_gc_safepoint with nonpositive ttl to remove the safepoint

                    encoder.save_footer(digest).await?;
                    DUMPING.store(false, Ordering::Relaxed);
                    Ok(())
                }
                .boxed()
            })
            .await
    }

    pub fn reset_dumping() {
        DUMPING.store(false, Ordering::Relaxed);
    }

    fn create_rdb() -> File {
        File::from_std(
            std::fs::File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open("dump.rdb")
                .unwrap(),
        )
    }
}
