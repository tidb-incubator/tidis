use std::sync::Arc;

use tokio::sync::Mutex;
use crate::{Frame, Command, utils::resp_invalid_arguments};
use tikv_client::{Transaction};
use super::{
    errors::AsyncResult,
    errors::RTError,
};
use crate::utils::{
    resp_nil,
    lua_resp_to_redis_resp,
    redis_resp_to_lua_resp,
};

use mlua::{
    Lua,
    Value as LuaValue,
    prelude::*,
    StdLib,
    LuaOptions,
    Result, Variadic,
};

#[derive(Clone)]
pub struct LuaCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
    lua: Arc<Mutex<Lua>>,
}


pub async fn redis_pcall(_lua: &Lua, arg: String) -> Result<String> {
    Ok(arg)
}

impl LuaCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        LuaCommandCtx {
            txn: txn,
            lua: Arc::new(Mutex::new(Lua::new_with(StdLib::STRING
                |StdLib::TABLE
                |StdLib::IO
                |StdLib::MATH
                |StdLib::OS, 
                LuaOptions::new()).unwrap())),
        }
    }

    pub async fn do_async_eval_inner(self, script: &str, keys: &Vec<String>, args: &Vec<String>) -> LuaResult<Frame> {
        let keys = keys.clone();
        let args = args.clone();
        let lua_rc = self.lua.clone();
        let lua = lua_rc.lock().await;

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
        // create redis.* commands table
        let redis = lua.create_table()?;
        let txn_rc = self.txn;
        
        let redis_call = lua.create_async_function(move |_lua, args: Variadic<String>| {
            let txn_rc = txn_rc.clone();
            // package arguments(without cmd) to argv
            async move {
                if (&args).len() == 0 {
                    return Ok(LuaValue::String(_lua.create_string("invalid arguments").unwrap()));
                }
                let cmd_name = &args.as_slice()[0];
                let mut argv = vec![];
                for arg in args.clone() {
                    argv.push(arg);
                }

                let cmd = Command::from_argv(cmd_name, &argv).unwrap();
                let result = match cmd {
                    Command::Decr(cmd) => cmd.decr(txn_rc.clone()).await,
                    Command::Incr(cmd) => cmd.incr(txn_rc.clone()).await,
                    Command::Del(cmd) => cmd.del(txn_rc.clone()).await,
                    Command::Exists(cmd) => cmd.exists(txn_rc.clone()).await,
                    Command::Get(cmd) => cmd.get(txn_rc.clone()).await,
                    Command::Set(cmd) => cmd.set(txn_rc.clone()).await,
                    Command::SetNX(cmd) => cmd.put_not_exists(txn_rc.clone()).await,
                    Command::SetEX(cmd) => cmd.setex(txn_rc.clone()).await,
                    Command::Mget(cmd) => cmd.batch_get(txn_rc.clone()).await,
                    Command::Mset(cmd) => cmd.batch_put(txn_rc.clone()).await,
                    Command::TTL(cmd) => cmd.ttl(false, txn_rc.clone()).await,
                    Command::PTTL(cmd) => cmd.ttl(true, txn_rc.clone()).await,
                    Command::Expire(cmd) => cmd.expire(false, false, txn_rc.clone()).await,
                    Command::ExpireAt(cmd) => cmd.expire(false, true, txn_rc.clone()).await,
                    Command::Pexpire(cmd) => cmd.expire(true, false, txn_rc.clone()).await,
                    Command::PexpireAt(cmd) => cmd.expire(true, true, txn_rc.clone()).await,
                    Command::Hset(cmd) => cmd.hset(txn_rc.clone()).await,
                    Command::Hget(cmd) => cmd.hget(txn_rc.clone()).await,
                    Command::Hmget(cmd) => cmd.hmget(txn_rc.clone()).await,
                    Command::Hlen(cmd) => cmd.hlen(txn_rc.clone()).await,
                    Command::Hgetall(cmd) => cmd.hgetall(txn_rc.clone()).await,
                    Command::Hdel(cmd) => cmd.hdel(txn_rc.clone()).await,
                    Command::Hkeys(cmd) => cmd.hkeys(txn_rc.clone()).await,
                    Command::Hvals(cmd) => cmd.hvals(txn_rc.clone()).await,
                    Command::Hincrby(cmd) => cmd.hincrby(txn_rc.clone()).await,
                    Command::Hexists(cmd) => cmd.hexists(txn_rc.clone()).await,
                    Command::Hstrlen(cmd) => cmd.hstrlen(txn_rc.clone()).await,
                    _ => {Ok(resp_invalid_arguments())}
                };
                match result {
                    Ok(resp) => {
                        let lua_resp = redis_resp_to_lua_resp(resp, _lua);
                        return Ok(lua_resp);
                    },
                    Err(e) => {
                        return Ok(LuaValue::String(_lua.create_string(&e.to_string()).unwrap()));
                    }
                }
            }
        })?;
        redis.set("call", redis_call)?;
        let redis_pcall = lua.create_async_function(redis_pcall)?;
        redis.set("pcall", redis_pcall)?;
        // register to global table
        globals.set("redis", redis)?;

        // TODO cache script
        // TODO async call
        let chunk = lua.load(script);
        let resp: LuaValue = chunk.eval_async().await?;
        // convert lua value to redis value
        let redis_resp = lua_resp_to_redis_resp(resp);

        // lua clean up
        Ok(redis_resp)
    }

    pub async fn do_async_eval(self, script: &str, keys: &Vec<String>, args: &Vec<String>) -> AsyncResult<Frame> {
        let lua_resp = self.clone().do_async_eval_inner(script, keys, args).await;
        match lua_resp {
            Ok(resp) => {
                Ok(resp)
            },
            Err(err) => {
                //return Ok(resp_err(&err.to_string()));
                Err(RTError::StringError(err.to_string()))
            }
        }
    }
}