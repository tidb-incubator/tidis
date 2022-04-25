use futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Key, Value, KvPair, BoundRange, Transaction};
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError, get_txn_client,
};
use crate::utils::{resp_err, resp_array, resp_bulk, resp_int, resp_nil, resp_ok, resp_str};
use super::errors::*;
use crate::utils::{lua_resp_to_redis_resp};

use mlua::{
    Lua,
    Value as LuaValue,
    prelude::*,
    StdLib,
    LuaOptions,
    Result,
};

pub struct LuaCommandCtx<'a> {
    txn: Option<&'a mut Transaction>,
    lua: Lua,
}

pub async fn redis_call(_lua: &Lua, arg: String) -> Result<String> {
    Ok(arg)
}

pub async fn redis_pcall(_lua: &Lua, arg: String) -> Result<String> {
    Ok(arg)
}

pub fn register_redis_func(lua: &Lua) -> Result<()> {
    let globals = lua.globals();

    // create redis.* commands table
    let redis = lua.create_table()?;
    let redis_call = lua.create_async_function(redis_call)?;
    redis.set("call", redis_call)?;

    let redis_pcall = lua.create_async_function(redis_pcall)?;
    redis.set("pcall", redis_pcall)?;

    // register to global table
    globals.set("redis", redis)?;

    Ok(())
}

impl<'a> LuaCommandCtx<'a> {
    pub fn new(txn: Option<&'a mut Transaction>) -> Self {
        LuaCommandCtx {
            txn: txn,
            lua: Lua::new_with(StdLib::STRING
                |StdLib::TABLE
                |StdLib::IO
                |StdLib::MATH
                |StdLib::OS, 
                LuaOptions::new()).unwrap(),
        }
    }



    pub async fn do_async_eval_inner(self, script: &str, keys: &Vec<String>, args: &Vec<String>) -> LuaResult<Frame> {
        let keys = keys.clone();
        let args = args.clone();
        let lua = &self.lua;

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
        register_redis_func(lua);

        // TODO cache script
        let resp = lua.load(&script).eval::<LuaValue>()?;
        // convert lua value to redis value
        let redis_resp = lua_resp_to_redis_resp(resp);

        // lua clean up

        Ok(redis_resp)
    }

    pub async fn do_async_eval(self, script: &str, keys: &Vec<String>, args: &Vec<String>) -> AsyncResult<Frame> {
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