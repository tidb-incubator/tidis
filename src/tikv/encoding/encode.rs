use tikv_client::Key;

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
        };
        format!(
            "x$R_{}_{}",
            self.instance_id,
            dt_prefix
        )
    }

    pub fn encode_string(&self, key: &str) -> Key {
        let prefix = self.get_prefix(DataType::String);
        let ret = format!("{}_{}", prefix, key);
        ret.into()
    }

    pub fn encode_strings(&self, keys: Vec<String>) -> Vec<Key> {
        let prefix = self.get_prefix(DataType::String);
        keys.into_iter()
            .map(|val| format!("{}_{}", prefix, val).into())
            .collect()
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