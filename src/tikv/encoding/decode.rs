use std::convert::TryInto;

use tikv_client::{Key, Value};
use super::DataType;

pub struct KeyDecoder {}

impl KeyDecoder {
    pub fn new() -> Self {
        KeyDecoder{}
    }

    pub fn decode_string(&self, key: Key) -> Vec<u8> {
        let mut bytes: Vec<u8> = key.clone().into();
        bytes.drain(15..).collect()
    }

    pub fn decode_key_type(&self, value: &Vec<u8>) -> DataType {
        match value[0] {
            0 => DataType::String,
            1 => DataType::Hash,
            2 => DataType::List,
            3 => DataType::Set,
            4 => DataType::Zset,
            _ => panic!("no support data type"),
        }
    }

    pub fn decode_key_ttl(&self, value: &Vec<u8>) -> u64 {
        u64::from_be_bytes(value[1..9].try_into().unwrap())
    }

    pub fn decode_key_string_value(&self, value: &Vec<u8>) -> Value {
        value[9..].to_vec().into()
    }

    pub fn decode_key_hash_size(&self, value: &Vec<u8>) -> u64 {
        u64::from_be_bytes(value[9..].try_into().unwrap())
    }

    pub fn decode_key_hash_meta(&self, value: &Vec<u8>) -> (u64, u64) {
        (self.decode_key_ttl(value), self.decode_key_hash_size(value))
    }

    pub fn decode_key_hash_userkey_from_datakey(&self, rkey: &str, key: Key) -> Vec<u8> {
        let key: Vec<u8> = key.into();
        let idx = 16 + rkey.len();
        key[idx..].to_vec()
    }

    /// return (ttl, left, right)
    pub fn decode_key_list_meta(&self, value: &Vec<u8>) -> (u64, u64, u64) {
        (
            u64::from_be_bytes(value[1..9].try_into().unwrap()),
            u64::from_be_bytes(value[9..17].try_into().unwrap()),
            u64::from_be_bytes(value[17..].try_into().unwrap()),
        )
    }

    pub fn decode_key_set_size(&self, value: &Vec<u8>) -> u64 {
        u64::from_be_bytes(value[9..].try_into().unwrap())
    }

    pub fn decode_key_set_meta(&self, value: &Vec<u8>) -> (u64, u64) {
        (self.decode_key_ttl(value), self.decode_key_set_size(value))
    }

    pub fn decode_key_set_member_from_datakey(&self, rkey: &str, key: Key) -> Vec<u8> {
        let key: Vec<u8> = key.into();
        let idx = 16 + rkey.len();
        key[idx..].to_vec()
    }

    pub fn decode_hash_field(&self, rkey: Key, key: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = rkey.clone().into();
        bytes.drain(17 + key.len() + 1..).collect()
    }

    pub fn decode_list_meta(&self, value: Option<Vec<u8>>) -> (i64, i64) {
        match value {
            Some(v) => (
                i64::from_be_bytes(v[0..8].try_into().unwrap()),
                i64::from_be_bytes(v[8..16].try_into().unwrap()),
            ),
            None => (std::u32::MAX as i64, std::u32::MAX as i64),
        }
    }

    pub fn decode_set_member(&self, rkey: Key, key: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = rkey.clone().into();
        bytes.drain(17 + key.len() + 1..).collect()
    }
}