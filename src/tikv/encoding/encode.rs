use tikv_client::Key;
use tikv_client::Value;
use crate::tikv::get_instance_id;

use super::DataType;

pub struct KeyEncoder {
    pub instance_id: String,
}

impl KeyEncoder {
    pub fn new() -> Self {
        let inst_id_bytes = get_instance_id().to_be_bytes().to_vec();
        let inst_id = String::from_utf8(inst_id_bytes).unwrap();
        KeyEncoder { instance_id: inst_id}
    }

    fn get_prefix(&self, tp: DataType) -> String {
        let dt_prefix = match tp {
            DataType::String => "R",
            DataType::Hash => "H",
            DataType::List => "L",
            DataType::Set => "S",
            DataType::Zset => "Z",
        };
        format!(
            "x$R_{}_{}",
            self.instance_id,
            dt_prefix
        )
    }

    fn get_type_bytes(&self, dt: DataType) -> u8 {
        match dt {
            DataType::String => 0,
            DataType::Hash => 1,
            DataType::List => 2,
            DataType::Set => 3,
            DataType::Zset => 4,
        }
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

    pub fn encode_txnkv_string_value(&self, value: &mut Value, ttl: u64) -> Value {
        let dt = self.get_type_bytes(DataType::String);
        let mut val = Vec::new();
        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(value);
        val.into()
    }

    pub fn encode_rawkv_strings(&self, keys: &Vec<String>) -> Vec<Key> {
        let prefix = self.get_prefix(DataType::String);
        keys.into_iter()
            .map(|val| format!("{}_{}", prefix, val).into())
            .collect()
    }

    pub fn encode_txnkv_strings(&self, keys: &Vec<String>) -> Vec<Key> {
        keys.into_iter()
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
        let ret = format!("x_{}_D_{}_", self.instance_id, key);
        ret.into()
    }

    pub fn encode_txnkv_hash_meta_value(&self, ttl: u64, size: u64) -> Value {
        let dt = self.get_type_bytes(DataType::Hash);
        let mut val = Vec::new();

        val.append(&mut dt.to_be_bytes().to_vec());
        val.append(&mut ttl.to_be_bytes().to_vec());
        val.append(&mut size.to_be_bytes().to_vec());
        val.into()
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
        val.into()
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
        val.into()
    }

    pub fn encode_txnkv_set_data_key(&self, key: &str, members: &str) -> Key {
        let ret = format!("x_{}_D_S_{}_{}", self.instance_id, key, members);
        ret.into()
    }

    pub fn encode_string_end(&self) -> Key {
        let prefix = self.get_prefix(DataType::String);
        let ret = format!("{}`", prefix);
        ret.into()
    }

    pub fn encode_hash(&self, key: &str, field: &str) -> Key {
        let prefix = self.get_prefix(DataType::Hash);
        let ret = format!("{}_D_{}_{}", prefix, key, field);
        ret.into()
    }

    pub fn encode_hash_start(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::Hash);
        let ret = format!("{}_D_{}_", prefix, key);
        ret.into()
    }

    pub fn encode_hash_end(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::Hash);
        let ret = format!("{}_D_{}`", prefix, key);
        ret.into()
    }

    pub fn encode_list_meta_key(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::List);
        let ret = format!("{}_M_{}", prefix, key);
        ret.into()
    }

    pub fn encode_list_elem_key(&self, key: &str, idx: i64) -> Key {
        let prefix = self.get_prefix(DataType::List);
        let mut res = format!("{}_D_{}_", prefix, key).into_bytes();
        res.append(&mut idx.to_be_bytes().to_vec());
        res.into()
    }

    pub fn encode_list_meta(&self, l: i64, r:i64) -> Vec<u8> {
        [l.to_be_bytes(), r.to_be_bytes()].concat().to_vec()
    }

    pub fn encode_list_elem_start(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::List);
        let ret = format!("{}_D_{}_", prefix, key).into_bytes();
        ret.into()
    }

    pub fn encode_list_elem_end(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::List);
        let ret = format!("{}_D_{}`", prefix, key).into_bytes();
        ret.into()
    }

    pub fn encode_set(&self, key: &str, member: &str) -> Key {
        let prefix = self.get_prefix(DataType::Set);
        let ret = format!("{}_D_{}_{}", prefix, key, member);
        ret.into()
    }

    pub fn encode_set_start(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::Set);
        let ret = format!("{}_D_{}_", prefix, key);
        ret.into()
    }

    pub fn encode_set_end(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::Set);
        let ret = format!("{}_D_{}`", prefix, key);
        ret.into()
    }
}