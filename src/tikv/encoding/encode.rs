use crate::tikv::get_instance_id;
use tikv_client::Key;
use tikv_client::Value;

use super::DataType;

pub struct KeyEncoder {
    pub instance_id: String,
}

impl KeyEncoder {
    pub fn new() -> Self {
        KeyEncoder {
            instance_id: get_instance_id().to_string(),
        }
    }

    fn get_prefix(&self, tp: DataType) -> String {
        let dt_prefix = match tp {
            DataType::String => "R",
            DataType::Hash => "H",
            DataType::List => "L",
            DataType::Set => "S",
            DataType::Zset => "Z",
            DataType::Null => "N",
        };
        format!("x$R_{}_{}", self.instance_id, dt_prefix)
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
        let ret = format!("x_{}_T_{}", self.instance_id, addr);
        ret.into()
    }

    pub fn encode_txnkv_cluster_topo_value(&self, ttl: u64) -> Value {
        ttl.to_be_bytes().to_vec()
    }

    pub fn encode_txnkv_cluster_topo_start(&self) -> Key {
        let ret = format!("x_{}_T_", self.instance_id);
        ret.into()
    }

    pub fn encode_txnkv_cluster_topo_end(&self) -> Key {
        let ret = format!("x_{}_T`", self.instance_id);
        ret.into()
    }

    pub fn encode_rawkv_string(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::String);
        let ret = format!("{}_M_{}", prefix, key);
        ret.into()
    }

    pub fn encode_txnkv_string(&self, key: &str) -> Key {
        let ret = format!("x_{}_M_{}", self.instance_id, key);
        ret.into()
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
        let prefix = self.get_prefix(DataType::String);
        keys.iter()
            .map(|val| format!("{}_{}", prefix, val).into())
            .collect()
    }

    pub fn encode_txnkv_strings(&self, keys: &[String]) -> Vec<Key> {
        keys.iter()
            .map(|val| format!("x_{}_M_{}", self.instance_id, val).into())
            .collect()
    }

    pub fn encode_txnkv_hash_meta_key(&self, key: &str) -> Key {
        let ret = format!("x_{}_M_{}", self.instance_id, key);
        ret.into()
    }

    pub fn encode_txnkv_hash_data_key(&self, key: &str, field: &str) -> Key {
        // TODO: maybe conflict
        let ret = format!("x_{}_D_H_{}_{}", self.instance_id, key, field);
        ret.into()
    }

    pub fn encode_txnkv_hash_data_key_start(&self, key: &str) -> Key {
        let ret = format!("x_{}_D_H_{}_", self.instance_id, key);
        ret.into()
    }

    pub fn encode_txnkv_hash_meta_value(&self, ttl: u64, size: u64) -> Value {
        let dt = self.get_type_bytes(DataType::Hash);
        let mut val = Vec::new();

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut size.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_list_meta_key(&self, key: &str) -> Key {
        let ret = format!("x_{}_M_{}", self.instance_id, key);
        ret.into()
    }

    /// idx range [0, 1<<64]
    /// left initial value  1<<32, left is point to the left element
    /// right initial value 1<<32, right is point to the next right position of right element
    /// list is indicated as null if left index equal to right
    pub fn encode_txnkv_list_data_key(&self, key: &str, idx: u64) -> Key {
        let prefix = format!("x_{}_D_L_{}", self.instance_id, key);
        let mut key = prefix.as_bytes().to_vec();
        key.append(&mut idx.to_be_bytes().to_vec());
        key.into()
    }

    pub fn encode_txnkv_list_meta_value(&self, ttl: u64, left: u64, right: u64) -> Value {
        let dt = self.get_type_bytes(DataType::List);
        let mut val = Vec::new();

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut left.to_be_bytes().to_vec());
        val.append(&mut right.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_set_meta_key(&self, key: &str) -> Key {
        let ret = format!("x_{}_M_{}", self.instance_id, key);
        ret.into()
    }

    pub fn encode_txnkv_set_meta_value(&self, ttl: u64, size: u64) -> Value {
        let dt = self.get_type_bytes(DataType::Set);
        let mut val = Vec::new();

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut size.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_set_data_key(&self, key: &str, members: &str) -> Key {
        let ret = format!("x_{}_D_S_{}_{}", self.instance_id, key, members);
        ret.into()
    }

    pub fn encode_txnkv_set_data_key_start(&self, key: &str) -> Key {
        let ret = format!("x_{}_D_S_{}_", self.instance_id, key);
        ret.into()
    }

    pub fn encode_txnkv_zset_meta_key(&self, key: &str) -> Key {
        let ret = format!("x_{}_M_{}", self.instance_id, key);
        ret.into()
    }

    pub fn encode_txnkv_zset_meta_value(&self, ttl: u64, size: u64) -> Value {
        let dt = self.get_type_bytes(DataType::Zset);
        let mut val = Vec::new();

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut size.to_be_bytes().to_vec());
        val
    }

    pub fn encode_txnkv_zset_data_key(&self, key: &str, member: &str) -> Key {
        let ret = format!("x_{}_D_Z_{}_{}", self.instance_id, key, member);
        ret.into()
    }

    pub fn encode_txnkv_zset_data_key_start(&self, key: &str) -> Key {
        let ret = format!("x_{}_D_Z_{}_", self.instance_id, key);
        ret.into()
    }

    pub fn encode_txnkv_zset_data_value(&self, score: i64) -> Value {
        score.to_be_bytes().to_vec()
    }

    pub fn encode_txnkv_zset_score_key(&self, key: &str, score: i64) -> Key {
        let prefix = format!("x_{}_S_Z_{}_", self.instance_id, key);
        let mut ret = prefix.as_bytes().to_vec();
        ret.append(&mut score.to_be_bytes().to_vec());
        ret.into()
    }

    pub fn encode_txnkv_zset_score_key_start(&self, key: &str) -> Key {
        let ret = format!("x_{}_S_Z_{}_", self.instance_id, key);
        ret.into()
    }
}
