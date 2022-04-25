use tokio::{
    time::Duration
};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::frame::Frame;
use mlua::{
    Value as LuaValue,
};

pub fn resp_ok() -> Frame {
    Frame::Simple("OK".to_string())
}

pub fn resp_err(e: &str) -> Frame {
    Frame::Error(e.to_string())
}

pub fn resp_sstr(val: &'static str) -> Frame {
    Frame::Simple(val.to_string())
}

pub fn resp_str(val: &str) -> Frame {
    Frame::Simple(val.to_string())
}

pub fn resp_int(val: i64) -> Frame {
    Frame::Integer(val)
}

pub fn resp_bulk(val: Vec<u8>) -> Frame {
    Frame::Bulk(val.into())
}

pub fn resp_nil() -> Frame {
    Frame::Null
}

pub fn resp_array(val: Vec<Frame>) -> Frame {
    Frame::Array(val.into())
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}



pub fn lua_resp_to_redis_resp(resp: LuaValue) -> Frame {
    match resp {
        LuaValue::String(r) => {
            resp_bulk(r.to_str().unwrap().as_bytes().to_vec())
        },
        LuaValue::Integer(r) => {
            resp_int(r)
        },
        LuaValue::Boolean(r) => {
            let r_int: i64 = if r == false { 0 } else { 1 };
            resp_int(r_int)
        },
        LuaValue::Number(r) => {
            resp_bulk(r.to_string().as_bytes().to_vec())
        },
        LuaValue::Table(r) => {
            let mut resp_arr = vec![];
            for pair in r.pairs::<LuaValue, LuaValue>() {
                let (_, v) = pair.unwrap();
                resp_arr.push(lua_resp_to_redis_resp(v));
            }
            resp_array(resp_arr)
        },
        LuaValue::Nil => {
            resp_nil()
        }
        LuaValue::Error(r) => { resp_err(&r.to_string())},
        _ => {resp_err("panic")},
    }
}

pub fn key_is_expired(ttl: u64) -> bool {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards");
    let ts = d.as_secs()*1000 + d.subsec_millis() as u64;
    if ttl > 0 && ttl < ts {
        true
    } else {
        false
    }
}

pub fn now_timestamp_in_millis() -> u64 {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards");
    return d.as_secs()*1000 + d.subsec_millis() as u64;
}

pub fn timestamp_from_ttl(ttl: u64) -> u64 {
    ttl + now_timestamp_in_millis()
}

pub fn ttl_from_timestamp(timestamp: u64) -> u64 {
    let now = now_timestamp_in_millis();
    if now > timestamp {
        return 0
    } else {
        return timestamp - now;
    }
}