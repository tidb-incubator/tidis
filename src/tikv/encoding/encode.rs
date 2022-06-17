use super::DataType;
use super::SIGN_MASK;
use crate::config_meta_key_number_or_default;
use crate::tikv::get_instance_id;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::ops::Range;
use tikv_client::BoundRange;
use tikv_client::Key;
use tikv_client::Value;

pub struct KeyEncoder {
    // instance_id will be encoded to 2 bytes vec
    instance_id: [u8; 2],
    // meta_key_number is the number of sub meta key of a new key
    meta_key_number: u16,
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
            meta_key_number: config_meta_key_number_or_default(),
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
        let mut key = Vec::with_capacity(6 + ukey.len());
        let key_len: u16 = ukey.len().try_into().unwrap();

        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(&key_len.to_be_bytes());
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
            .map(|ukey| self.encode_rawkv_string(ukey))
            .collect()
    }

    pub fn encode_txnkv_strings(&self, keys: &[String]) -> Vec<Key> {
        keys.iter()
            .map(|ukey| self.encode_txnkv_string(ukey))
            .collect()
    }

    fn encode_txnkv_meta_common_prefix(&self, ukey: &str, key: &mut Vec<u8>) {
        let key_len: u16 = ukey.len().try_into().unwrap();

        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(&key_len.to_be_bytes());
        key.extend_from_slice(ukey.as_bytes());
    }

    pub fn encode_txnkv_meta_key(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(6 + ukey.len());

        self.encode_txnkv_meta_common_prefix(ukey, &mut key);
        key.into()
    }

    pub fn encode_txnkv_sub_meta_key(&self, ukey: &str, version: u16, idx: u16) -> Key {
        let mut key = Vec::with_capacity(11 + ukey.len());

        self.encode_txnkv_meta_common_prefix(ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&idx.to_be_bytes());
        key.into()
    }

    pub fn encode_txnkv_sub_meta_key_start(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(9 + ukey.len());

        self.encode_txnkv_meta_common_prefix(ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_sub_meta_key_end(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(9 + ukey.len());

        self.encode_txnkv_meta_common_prefix(ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_txnkv_sub_meta_key_range(&self, key: &str, version: u16) -> BoundRange {
        let sub_meta_key_start = self.encode_txnkv_sub_meta_key_start(key, version);
        let sub_meta_key_end = self.encode_txnkv_sub_meta_key_end(key, version);
        let range: Range<Key> = sub_meta_key_start..sub_meta_key_end;
        range.into()
    }

    fn encode_txnkv_type_data_key_prefix(
        &self,
        key_type: u8,
        data_type: u8,
        ukey: &str,
        key: &mut Vec<u8>,
        version: u16,
    ) {
        let key_len: u16 = ukey.len().try_into().unwrap();
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(key_type);
        key.push(data_type);
        key.extend_from_slice(&key_len.to_be_bytes());
        key.extend_from_slice(ukey.as_bytes());
        key.extend_from_slice(&version.to_be_bytes());
    }

    pub fn encode_txnkv_hash_data_key(&self, ukey: &str, field: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len() + field.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_HASH,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.extend_from_slice(field.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_hash_data_key_start(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_HASH,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_hash_data_key_end(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len());
        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_HASH,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_txnkv_hash_data_key_range(&self, key: &str, version: u16) -> BoundRange {
        let data_key_start = self.encode_txnkv_hash_data_key_start(key, version);
        let data_key_end = self.encode_txnkv_hash_data_key_end(key, version);
        let range: Range<Key> = data_key_start..data_key_end;
        range.into()
    }

    pub fn encode_txnkv_hash_meta_value(&self, ttl: u64, version: u16, index_size: u16) -> Value {
        let dt = self.get_type_bytes(DataType::Hash);
        let mut val = Vec::with_capacity(13);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());

        // if index_size is 0 means this is a new created key, use the default config number
        if index_size == 0 {
            val.extend_from_slice(&self.meta_key_number.to_be_bytes());
        } else {
            val.extend_from_slice(&index_size.to_be_bytes());
        }

        val
    }

    /// idx range [0, 1<<64]
    /// left initial value  1<<32, left is point to the left element
    /// right initial value 1<<32, right is point to the next right position of right element
    /// list is indicated as null if left index equal to right
    pub fn encode_txnkv_list_data_key(&self, ukey: &str, idx: u64, version: u16) -> Key {
        let mut key = Vec::with_capacity(13 + ukey.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_LIST,
            ukey,
            &mut key,
            version,
        );
        key.extend_from_slice(&idx.to_be_bytes());
        key.into()
    }

    pub fn encode_txnkv_list_meta_value(
        &self,
        ttl: u64,
        version: u16,
        left: u64,
        right: u64,
    ) -> Value {
        let dt = self.get_type_bytes(DataType::List);
        let mut val = Vec::with_capacity(27);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        val.extend_from_slice(&left.to_be_bytes());
        val.extend_from_slice(&right.to_be_bytes());
        val
    }

    pub fn encode_txnkv_set_meta_value(&self, ttl: u64, version: u16, index_size: u16) -> Value {
        let dt = self.get_type_bytes(DataType::Set);
        let mut val = Vec::with_capacity(13);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        // if index_size is 0 means this is a new created key, use the default config number
        if index_size == 0 {
            val.extend_from_slice(&self.meta_key_number.to_be_bytes());
        } else {
            val.extend_from_slice(&index_size.to_be_bytes());
        }
        val
    }

    pub fn encode_txnkv_set_data_key(&self, ukey: &str, member: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len() + member.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_SET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_set_data_key_start(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_SET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_set_data_key_end(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_SET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_txnkv_set_data_key_range(&self, key: &str, version: u16) -> BoundRange {
        let data_key_start = self.encode_txnkv_set_data_key_start(key, version);
        let data_key_end = self.encode_txnkv_set_data_key_end(key, version);
        let range: Range<Key> = data_key_start..data_key_end;
        range.into()
    }

    pub fn encode_txnkv_zset_meta_value(&self, ttl: u64, version: u16, index_size: u16) -> Value {
        let dt = self.get_type_bytes(DataType::Zset);
        let mut val = Vec::with_capacity(13);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        // if index_size is 0 means this is a new created key, use the default config number
        if index_size == 0 {
            val.extend_from_slice(&self.meta_key_number.to_be_bytes());
        } else {
            val.extend_from_slice(&index_size.to_be_bytes());
        }
        val
    }

    pub fn encode_txnkv_zset_data_key(&self, ukey: &str, member: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len() + member.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_zset_data_key_start(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_zset_data_key_end(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(10 + ukey.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_DATA,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_txnkv_zset_data_key_range(&self, ukey: &str, version: u16) -> BoundRange {
        let data_key_start = self.encode_txnkv_zset_data_key_start(ukey, version);
        let data_key_end = self.encode_txnkv_zset_data_key_end(ukey, version);
        let range: Range<Key> = data_key_start..data_key_end;
        range.into()
    }

    pub fn encode_txnkv_zset_data_value(&self, score: f64) -> Value {
        self.encode_f64_to_cmp_uint64(score).to_be_bytes().to_vec()
    }

    fn encode_f64_to_cmp_uint64(&self, score: f64) -> u64 {
        let mut b = score.to_bits();
        if score >= 0f64 {
            b |= SIGN_MASK;
        } else {
            b = !b;
        }

        b
    }

    // encode the member to score key
    pub fn encode_txnkv_zset_score_key(
        &self,
        ukey: &str,
        score: f64,
        member: &str,
        version: u16,
    ) -> Key {
        let mut key = Vec::with_capacity(19 + ukey.len() + member.len());
        let score = self.encode_f64_to_cmp_uint64(score);

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_SCORE,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_txnkv_zset_score_key_start(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(5 + ukey.len());

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_SCORE,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_zset_score_key_end(&self, ukey: &str, version: u16) -> Key {
        let mut key = Vec::with_capacity(5 + ukey.len());
        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_SCORE,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_txnkv_zset_score_key_range(&self, ukey: &str, version: u16) -> BoundRange {
        let range_start = self.encode_txnkv_zset_score_key_start(ukey, version);
        let range_end = self.encode_txnkv_zset_score_key_end(ukey, version);
        let range: Range<Key> = range_start..range_end;
        range.into()
    }

    pub fn encode_txnkv_zset_score_key_score_start(
        &self,
        ukey: &str,
        score: f64,
        with_frontier: bool,
        version: u16,
    ) -> Key {
        let mut key = Vec::with_capacity(15 + ukey.len());
        let mut score = self.encode_f64_to_cmp_uint64(score);
        if !with_frontier {
            score += 1;
        }

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_SCORE,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );

        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txnkv_zset_score_key_score_end(
        &self,
        ukey: &str,
        score: f64,
        with_frontier: bool,
        version: u16,
    ) -> Key {
        let mut key = Vec::with_capacity(15 + ukey.len());
        let mut score = self.encode_f64_to_cmp_uint64(score);
        if !with_frontier {
            score -= 1;
        }

        self.encode_txnkv_type_data_key_prefix(
            DATA_TYPE_SCORE,
            DATA_TYPE_ZSET,
            ukey,
            &mut key,
            version,
        );
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER + 1);
        key.into()
    }
}
