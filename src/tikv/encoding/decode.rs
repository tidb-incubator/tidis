use std::convert::TryInto;

use crate::tikv::KEY_ENCODER;

use super::{DataType, ENC_GROUP_SIZE, ENC_MARKER, SIGN_MASK};
use tikv_client::{Key, Value};

pub struct KeyDecoder {}

impl KeyDecoder {
    pub fn decode_bytes(data: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(data.len() / (ENC_GROUP_SIZE + 1) * ENC_GROUP_SIZE);
        let mut offset = 0;
        let chunk_len = ENC_GROUP_SIZE + 1;
        loop {
            // everytime make ENC_GROUP_SIZE + 1 elements as a decode unit
            let next_offset = offset + chunk_len;
            let chunk = &data[offset..next_offset];
            offset = next_offset;
            // the last byte in decode unit is for marker which indicates pad size
            let (&marker, bytes) = chunk.split_last().unwrap();
            let pad_size = (ENC_MARKER - marker) as usize;
            // no padding, just push 8 bytes
            if pad_size == 0 {
                key.extend_from_slice(bytes);
                continue;
            }
            let (bytes, _) = bytes.split_at(ENC_GROUP_SIZE - pad_size);
            key.extend_from_slice(bytes);

            return key;
        }
    }

    #[allow(dead_code)]
    pub fn decode_string(key: Key) -> Vec<u8> {
        let mut bytes: Vec<u8> = key.into();
        bytes.drain(15..).collect()
    }

    pub fn decode_key_type(value: &[u8]) -> DataType {
        match value[0] {
            0 => DataType::String,
            1 => DataType::Hash,
            2 => DataType::List,
            3 => DataType::Set,
            4 => DataType::Zset,
            _ => panic!("no support data type"),
        }
    }

    pub fn decode_key_ttl(value: &[u8]) -> u64 {
        u64::from_be_bytes(value[1..9].try_into().unwrap())
    }

    pub fn decode_topo_key_addr(value: &[u8]) -> &[u8] {
        &value[4..]
    }

    pub fn decode_topo_value(value: &[u8]) -> u64 {
        u64::from_be_bytes(value.try_into().unwrap())
    }

    pub fn decode_key_string_value(value: &[u8]) -> Value {
        value[11..].to_vec()
    }

    pub fn decode_key_string_slice(value: &[u8]) -> &[u8] {
        &value[11..]
    }

    pub fn decode_key_version(value: &[u8]) -> u16 {
        u16::from_be_bytes(value[9..11].try_into().unwrap())
    }

    pub fn decode_key_index_size(value: &[u8]) -> u16 {
        u16::from_be_bytes(value[11..].try_into().unwrap())
    }

    pub fn decode_key_meta(value: &[u8]) -> (u64, u16, u16) {
        (
            Self::decode_key_ttl(value),
            Self::decode_key_version(value),
            Self::decode_key_index_size(value),
        )
    }

    pub fn decode_key_hash_userkey_from_datakey(ukey: &str, key: Key) -> Vec<u8> {
        let key: Vec<u8> = key.into();
        let enc_ukey = KEY_ENCODER.encode_bytes(ukey.as_bytes());
        let idx = 8 + enc_ukey.len();
        key[idx..].to_vec()
    }

    /// return (ttl, version, left, right)
    pub fn decode_key_list_meta(value: &[u8]) -> (u64, u16, u64, u64) {
        (
            u64::from_be_bytes(value[1..9].try_into().unwrap()),
            u16::from_be_bytes(value[9..11].try_into().unwrap()),
            u64::from_be_bytes(value[11..19].try_into().unwrap()),
            u64::from_be_bytes(value[19..].try_into().unwrap()),
        )
    }

    pub fn decode_key_list_idx_from_datakey(ukey: &str, key: Key) -> u64 {
        let key: Vec<u8> = key.into();
        let enc_ukey = KEY_ENCODER.encode_bytes(ukey.as_bytes());
        let idx = 8 + enc_ukey.len();
        u64::from_be_bytes(key[idx..].try_into().unwrap())
    }

    pub fn decode_key_set_member_from_datakey(ukey: &str, key: Key) -> Vec<u8> {
        let key: Vec<u8> = key.into();
        let enc_ukey = KEY_ENCODER.encode_bytes(ukey.as_bytes());
        let idx = 8 + enc_ukey.len();
        key[idx..].to_vec()
    }

    pub fn decode_cmp_uint64_to_f64(u: u64) -> f64 {
        let mut score = u;

        if score & SIGN_MASK > 0 {
            score &= !SIGN_MASK;
        } else {
            score = !score;
        }

        f64::from_bits(score)
    }

    pub fn decode_key_zset_score_from_scorekey(ukey: &str, key: Key) -> f64 {
        let key: Vec<u8> = key.into();
        let enc_ukey = KEY_ENCODER.encode_bytes(ukey.as_bytes());
        let idx = 8 + enc_ukey.len();
        Self::decode_cmp_uint64_to_f64(u64::from_be_bytes(key[idx..idx + 8].try_into().unwrap()))
    }

    pub fn decode_key_zset_member_from_scorekey(ukey: &str, key: Key) -> Vec<u8> {
        let key: Vec<u8> = key.into();
        let enc_ukey = KEY_ENCODER.encode_bytes(ukey.as_bytes());
        let idx = 17 + enc_ukey.len();
        key[idx..].to_vec()
    }

    pub fn decode_key_zset_member_from_datakey(ukey: &str, key: Key) -> Vec<u8> {
        let key: Vec<u8> = key.into();
        let enc_ukey = KEY_ENCODER.encode_bytes(ukey.as_bytes());
        let idx = 8 + enc_ukey.len();
        key[idx..].to_vec()
    }

    pub fn decode_key_zset_data_value(value: &[u8]) -> f64 {
        Self::decode_cmp_uint64_to_f64(u64::from_be_bytes(value[..].try_into().unwrap()))
    }

    fn encoded_bytes_len(encoded: &[u8]) -> usize {
        let mut idx = ENC_GROUP_SIZE;
        loop {
            if encoded.len() < idx + 1 {
                return encoded.len();
            }
            let marker = encoded[idx];
            if marker != ENC_MARKER {
                return idx + 1;
            }
            idx += ENC_GROUP_SIZE + 1;
        }
    }

    pub fn decode_key_gc_userkey_version(key: Key) -> (Vec<u8>, u16) {
        let key: Vec<u8> = key.into();
        let enc_key_start = 5;
        let ukey = Self::decode_bytes(&key[enc_key_start..]);
        let idx = 5 + Self::encoded_bytes_len(&key[enc_key_start..]);
        let version = u16::from_be_bytes(key[idx..idx + 2].try_into().unwrap());
        (ukey, version)
    }
}
