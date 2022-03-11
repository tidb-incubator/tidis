use std::convert::TryInto;

use tikv_client::Key;

pub struct KeyDecoder {}

impl KeyDecoder {
    pub fn new() -> Self {
        KeyDecoder{}
    }

    pub fn decode_string(&self, key: Key) -> Vec<u8> {
        let mut bytes: Vec<u8> = key.clone().into();
        bytes.drain(15..).collect()
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