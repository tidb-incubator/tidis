use super::DataType;
use crate::tikv::get_instance_id;
use std::convert::TryFrom;
use tikv_client::Key;
use tikv_client::Value;

pub struct KeyEncoder {
    // instance_id will be encoded to 2 bytes vec
    pub instance_id: [u8; 2],
}
pub const RAW_KEY_PREFIX: u8 = b'r';
pub const TXN_KEY_PREFIX: u8 = b'x';

pub const DATA_TYPE_META: u8 = b'm';
pub const DATA_TYPE_DATA: u8 = b'd';
pub const DATA_TYPE_SCORE: u8 = b's';

pub const DATA_TYPE_HASH: u8 = b'h';
pub const DATA_TYPE_LIST: u8 = b'l';
pub const DATA_TYPE_SET: u8 = b's';
pub const DATA_TYPE_ZSET: u8 = b'z';
pub const DATA_TYPE_TOPO: u8 = b't';

pub const PLACE_HOLDER: u8 = b'`';

impl KeyEncoder {
    pub fn new() -> Self {
        KeyEncoder {
            instance_id: u16::try_from(get_instance_id()).unwrap().to_be_bytes(),
        }
    }

    fn get_type_bytes(&self, dt: DataType) -> u8 {
        match dt {
            DataType::String => 0,
            DataType::Hash => 1,
            DataType::List => 2,
            DataType::Set => 3,
            DataType::Zset => 4,
            DataType::Null => 5,
        }
    }

    /// encode key for register myself to tikv, for topology usage
    pub fn encode_txnkv_cluster_topo(&self, addr: &str) -> Key {
        let mut key = Vec::with_capacity(4 + addr.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_TOPO);
        key.extend_from_slice(addr.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_cluster_topo_value(&self, ttl: u64) -> Value {
        ttl.to_be_bytes().to_vec()
    }

    pub fn encode_txnkv_cluster_topo_start(&self) -> Key {
        let mut key = Vec::with_capacity(4);
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_TOPO);
        key.into()
    }

    pub fn encode_txnkv_cluster_topo_end(&self) -> Key {
        let mut key = Vec::with_capacity(5);
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_TOPO);
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_rawkv_string(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(4 + ukey.len());
        key.push(RAW_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(ukey.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_string(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(4 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(ukey.as_bytes());
        key.into()
    }

    fn encode_txnkv_string_internal(&self, vsize: usize, ttl: u64) -> Value {
        let dt = self.get_type_bytes(DataType::String);
        let mut val = Vec::with_capacity(5 + vsize);
        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_string_slice(&self, value: &[u8], ttl: u64) -> Value {
        let mut val = self.encode_txnkv_string_internal(value.len(), ttl);
        val.extend_from_slice(value);
        val
    }

    pub fn encode_txnkv_string_value(&self, value: &mut Value, ttl: u64) -> Value {
        let mut val = self.encode_txnkv_string_internal(value.len(), ttl);
        val.append(value);
        val
    }

    pub fn encode_rawkv_strings(&self, keys: &[String]) -> Vec<Key> {
        keys.iter()
            .map(|ukey| {
                let mut key = Vec::with_capacity(4 + ukey.len());
                key.push(RAW_KEY_PREFIX);
                key.extend_from_slice(self.instance_id.as_slice());
                key.push(DATA_TYPE_META);
                key.extend_from_slice(ukey.as_bytes());
                key.into()
            })
            .collect()
    }

    pub fn encode_txnkv_strings(&self, keys: &[String]) -> Vec<Key> {
        keys.iter()
            .map(|ukey| {
                let mut key = Vec::with_capacity(4 + ukey.len());
                key.push(TXN_KEY_PREFIX);
                key.extend_from_slice(self.instance_id.as_slice());
                key.push(DATA_TYPE_META);
                key.extend_from_slice(ukey.as_bytes());
                key.into()
            })
            .collect()
    }

    pub fn encode_txnkv_hash_meta_key(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(4 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(ukey.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_hash_data_key(&self, ukey: &str, field: &str) -> Key {
        let mut key = Vec::with_capacity(6 + ukey.len() + field.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_DATA);
        key.push(DATA_TYPE_HASH);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(field.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_hash_data_key_start(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(6 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_DATA);
        key.push(DATA_TYPE_HASH);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_hash_meta_value(&self, ttl: u64, size: u64) -> Value {
        let dt = self.get_type_bytes(DataType::Hash);
        let mut val = Vec::new();

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut size.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_list_meta_key(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(4 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(ukey.as_bytes());
        key.into()
    }

    /// idx range [0, 1<<64]
    /// left initial value  1<<32, left is point to the left element
    /// right initial value 1<<32, right is point to the next right position of right element
    /// list is indicated as null if left index equal to right
    pub fn encode_txnkv_list_data_key(&self, ukey: &str, idx: u64) -> Key {
        let mut key = Vec::with_capacity(13 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_DATA);
        key.push(DATA_TYPE_LIST);
        key.extend_from_slice(ukey.as_bytes());
        key.extend_from_slice(&idx.to_be_bytes());
        key.into()
    }

    pub fn encode_txnkv_list_meta_value(&self, ttl: u64, left: u64, right: u64) -> Value {
        let dt = self.get_type_bytes(DataType::List);
        let mut val = Vec::with_capacity(25);

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut left.to_be_bytes().to_vec());
        val.append(&mut right.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_set_meta_key(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(4 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(ukey.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_set_meta_value(&self, ttl: u64, size: u64) -> Value {
        let dt = self.get_type_bytes(DataType::Set);
        let mut val = Vec::with_capacity(17);

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut size.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_set_data_key(&self, ukey: &str, member: &str) -> Key {
        let mut key = Vec::with_capacity(5 + ukey.len() + member.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_DATA);
        key.push(DATA_TYPE_SET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_set_data_key_start(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(5 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_DATA);
        key.push(DATA_TYPE_SET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_zset_meta_key(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(4 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(ukey.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_zset_meta_value(&self, ttl: u64, size: u64) -> Value {
        let dt = self.get_type_bytes(DataType::Zset);
        let mut val = Vec::with_capacity(17);

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut size.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_zset_data_key(&self, ukey: &str, member: &str) -> Key {
        let mut key = Vec::with_capacity(6 + ukey.len() + member.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_DATA);
        key.push(DATA_TYPE_ZSET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_zset_data_key_start(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(6 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_DATA);
        key.push(DATA_TYPE_ZSET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_zset_data_value(&self, score: i64) -> Value {
        score.to_be_bytes().to_vec()
    }

    // encode the member to score key
    pub fn encode_txnkv_zset_score_key(&self, ukey: &str, score: i64, member: &str) -> Key {
        let mut key = Vec::with_capacity(15 + ukey.len() + member.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_SCORE);
        key.push(DATA_TYPE_ZSET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_zset_score_key_start(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(5 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_SCORE);
        key.push(DATA_TYPE_ZSET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_zset_score_key_range_start(&self, ukey: &str, score: i64) -> Key {
        let mut key = Vec::with_capacity(15 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_SCORE);
        key.push(DATA_TYPE_ZSET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_zset_score_key_range_end(&self, ukey: &str, score: i64) -> Key {
        let mut key = Vec::with_capacity(15 + ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_SCORE);
        key.push(DATA_TYPE_ZSET);
        key.extend_from_slice(ukey.as_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER + 1);
        key.into()
    }
}
