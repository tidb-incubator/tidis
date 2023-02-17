use crate::frame::Frame;
use hex::ToHex;
use mlua::{Lua, Value as LuaValue};
use sha1::{Digest, Sha1};
use std::io;
use std::{
    collections::HashSet,
    convert::TryInto,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::Duration;

use crate::tikv::errors::{RTError, REDIS_LUA_PANIC};
use rustls::{
    internal::pemfile::{certs, rsa_private_keys},
    AllowAnyAuthenticatedClient, RootCertStore,
};
use rustls::{Certificate, NoClientAuth, PrivateKey, ServerConfig};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

const TIMESTAMP_FORMAT: &str = "%Y/%m/%d %H:%M:%S%.3f %:z";

#[derive(PartialEq, Copy, Clone)]
pub enum ReturnOption {
    // Return number (for example number of affected keys)
    Number,
    // Return value (for example previous/overwritten value)
    Previous,
}

pub fn resp_ok() -> Frame {
    Frame::Simple("OK".to_string())
}

pub fn resp_queued() -> Frame {
    Frame::Simple("QUEUED".to_string())
}

pub fn resp_ok_ignore<T>(_: T) -> Frame {
    resp_ok()
}

pub fn resp_invalid_arguments() -> Frame {
    Frame::ErrorString("Invalid arguments")
}

pub fn resp_err(e: RTError) -> Frame {
    e.into()
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
    Frame::Array(val)
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

pub fn lua_resp_to_redis_resp(resp: LuaValue) -> Frame {
    match resp {
        LuaValue::Nil => Frame::Null,
        LuaValue::String(r) => resp_bulk(r.to_str().unwrap().as_bytes().to_vec()),
        LuaValue::Integer(r) => resp_int(r),
        LuaValue::Boolean(r) => {
            if !r {
                resp_nil()
            } else {
                resp_int(1)
            }
        }
        // just return integer part of the float in redis
        //LuaValue::Number(r) => resp_bulk(r.to_string().as_bytes().to_vec()),
        LuaValue::Number(r) => resp_int(r.trunc() as i64),
        LuaValue::Table(r) => {
            // handle error reply
            if let Ok(err_msg) = r.raw_get::<&str, String>("err") {
                return resp_err(RTError::Owned(err_msg));
            }

            // handle status reply
            if let Ok(status_msg) = r.raw_get::<&str, String>("ok") {
                return resp_str(&status_msg);
            }

            // handle array reply
            let len: i64 = r.len().unwrap();
            let mut arr = Vec::with_capacity(len.try_into().unwrap());
            for idx in 0..len {
                // key is start from 1
                let key = idx + 1;
                let v: LuaValue = r.raw_get(key).unwrap();
                arr.push(lua_resp_to_redis_resp(v));
            }
            resp_array(arr)
        }
        LuaValue::Error(r) => resp_err(r.into()),
        _ => resp_err(REDIS_LUA_PANIC),
    }
}

pub fn redis_resp_to_lua_resp(resp: Frame, lua: &Lua) -> LuaValue {
    match resp {
        Frame::Simple(v) => {
            let table = lua.create_table().unwrap();
            table.raw_set("ok", v).unwrap();
            LuaValue::Table(table)
        }
        Frame::Bulk(v) => {
            let str = String::from_utf8_lossy(&v).to_string();
            LuaValue::String(lua.create_string(&str).unwrap())
        }
        Frame::ErrorOwned(e) => {
            let table = lua.create_table().unwrap();
            table.raw_set("err", e).unwrap();
            LuaValue::Table(table)
        }
        Frame::ErrorString(e) => {
            let table = lua.create_table().unwrap();
            table.raw_set("err", e).unwrap();
            LuaValue::Table(table)
        }
        Frame::Integer(i) => LuaValue::Integer(i),
        Frame::Null => LuaValue::Boolean(false),
        Frame::Array(arr) => {
            let table = lua.create_table().unwrap();
            for (idx, value) in arr.iter().enumerate() {
                let v = redis_resp_to_lua_resp(value.clone(), lua);
                // nil will be convert to boolean false
                table.raw_set(idx + 1, v).unwrap();
            }
            LuaValue::Table(table)
        }
    }
}

pub fn key_is_expired(ttl: u64) -> bool {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let ts = d.as_secs() * 1000 + d.subsec_millis() as u64;
    ttl > 0 && ttl < ts
}

pub fn now_timestamp_in_millis() -> u64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    d.as_secs() * 1000 + d.subsec_millis() as u64
}

pub fn timestamp_from_ttl(ttl: u64) -> u64 {
    ttl + now_timestamp_in_millis()
}

pub fn ttl_from_timestamp(timestamp: u64) -> u64 {
    let now = now_timestamp_in_millis();
    if now > timestamp {
        0
    } else {
        timestamp - now
    }
}

/// Load the passed certificates file
fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
}

/// Load the passed keys file
fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
    rsa_private_keys(&mut BufReader::new(File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
}

/// Configure the server using rusttls
/// See https://docs.rs/rustls/0.19.0/rustls/struct.ServerConfig.html for details
///
/// A TLS server needs a certificate and a fitting private key
pub fn load_config(
    cert: &str,
    key: &str,
    auth_client: bool,
    ca_cert: &str,
) -> io::Result<ServerConfig> {
    let certs = load_certs(Path::new(cert))?;
    let mut keys = load_keys(Path::new(key))?;

    let client_auth = if auth_client {
        let ca_certs = load_certs(Path::new(ca_cert)).unwrap();
        let mut client_auth_roots = RootCertStore::empty();
        for ca in ca_certs {
            client_auth_roots.add(&ca).unwrap();
        }
        AllowAnyAuthenticatedClient::new(client_auth_roots)
    } else {
        NoClientAuth::new()
    };

    let mut config = ServerConfig::new(client_auth);
    config
        // set this server to use one cert together with the loaded private key
        .set_single_cert(certs, keys.remove(0))
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    Ok(config)
}

pub fn sha1hex(s: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(s);
    let sha1 = hasher.finalize();
    sha1.encode_hex::<String>()
}

pub fn count_unique_keys<T: std::hash::Hash + std::cmp::Eq>(keys: &[T]) -> usize {
    keys.iter().collect::<HashSet<&T>>().len()
}

pub fn timestamp_local(io: &mut dyn io::Write) -> io::Result<()> {
    let now = chrono::Local::now().format(TIMESTAMP_FORMAT);
    write!(io, "{}", now)
}
