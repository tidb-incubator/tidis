use futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Key, Value, KvPair, BoundRange, Transaction};
use core::ops::RangeFrom;
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use super::{get_txn_client};
use crate::utils::{resp_err, resp_array, resp_bulk, resp_int, resp_nil, resp_ok, resp_str};
use super::errors::*;

use mlua::{
    Lua,
    Value as LuaValue,
    prelude::*,
};

pub struct LuaCommandCtx<'a> {
    txn: Option<&'a mut Transaction>,
}

impl<'a> LuaCommandCtx<'a> {
    pub fn new(txn: Option<&'a mut Transaction>) -> Self {
        LuaCommandCtx {
            txn: txn,
        }
    }

    pub async fn do_async_eval_inner(&self, script: &str, keys: &Vec<String>, args: &Vec<String>) -> LuaResult<Frame> {
        let keys = keys.clone();
        let args = args.clone();
        let lua = Lua::new();

        let globals = lua.globals();

        // Add KEYS and ARGV to lua state
        let keys_table = lua.create_table()?;
        for idx in 0..keys.len() {
            let key = keys[idx].clone();
            keys_table.set(idx+1, key)?;
        }
        let args_table = lua.create_table()?;
        for idx in 0..args.len() {
            let arg = args[idx].clone();
            args_table.set(idx+1, arg)?;
        }

        globals.set("KEYS", keys_table)?;
        globals.set("ARGV", args_table)?;
        
        // Regist redis.call etc to handle redis.* command call in lua

        // TODO cache script
        let resp = lua.load(&script).eval::<LuaValue>()?;
        // convert lua value to redis value
        let redis_resp = self.lua_resp_to_redis_resp(resp);

        Ok(redis_resp)
    }

    pub fn lua_resp_to_redis_resp(&self, resp: LuaValue) -> Frame {
        let mut frame: Frame;
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
                    resp_arr.push(self.lua_resp_to_redis_resp(v));
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

    pub async fn do_async_eval(&self, script: &str, keys: &Vec<String>, args: &Vec<String>) -> AsyncResult<Frame> {
        let lua_resp = self.do_async_eval_inner(script, keys, args).await;
        match lua_resp {
            Ok(resp) => {
                Ok(resp)
            },
            Err(err) => {
                Ok(resp_err(&err.to_string()))
            }
        }
    }
}