use std::sync::Arc;

use super::errors::AsyncResult;
use crate::db::Db;
use crate::utils::{lua_resp_to_redis_resp, redis_resp_to_lua_resp, resp_err};
use crate::{utils::resp_invalid_arguments, Command, Frame};
use tikv_client::Transaction;
use tokio::sync::Mutex;

use crate::config::LOGGER;
use slog::{debug, error};

use crate::tikv::errors::{REDIS_LUA_CONTEXT_IS_NOT_INITIALIZED_ERR, REDIS_NO_MATCHING_SCRIPT_ERR};
use mlua::{prelude::*, Lua, Value as LuaValue, Variadic};

#[derive(Clone)]
pub struct LuaCommandCtx<'a> {
    txn: Option<Arc<Mutex<Transaction>>>,
    lua: &'a Option<Lua>,
}

impl<'a> LuaCommandCtx<'a> {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>, lua: &'a Option<Lua>) -> Self {
        LuaCommandCtx { txn, lua }
    }

    pub async fn do_async_eval_inner(
        self,
        script: &str,
        keys: &[String],
        args: &[String],
    ) -> LuaResult<Frame> {
        let lua = match self.lua {
            Some(lua) => lua,
            None => return Ok(resp_err(REDIS_LUA_CONTEXT_IS_NOT_INITIALIZED_ERR)),
        };
        //let lua = lua_rc.lock().await;

        let globals = lua.globals();

        // Add KEYS and ARGV to lua state
        let keys_table = lua.create_table()?;
        for (idx, key) in keys.iter().enumerate() {
            keys_table.set(idx + 1, key.clone())?;
        }
        let args_table = lua.create_table()?;
        for (idx, arg) in args.iter().enumerate() {
            args_table.set(idx + 1, arg.clone())?;
        }

        globals.set("KEYS", keys_table)?;
        globals.set("ARGV", args_table)?;

        // Regist redis.call etc to handle redis.* command call in lua
        // create redis.* commands table
        let redis = lua.create_table()?;
        let txn_rc = self.txn;

        let redis_call = lua.create_async_function(move |_lua, args: Variadic<LuaValue>| {
            let txn_rc = txn_rc.clone();
            // package arguments(without cmd) to argv
            async move {
                if (&args).len() == 0 {
                    let table = _lua.create_table().unwrap();
                    table.raw_set("err", "Invalid arguments, please specify at least one argument for redis.call()").unwrap();
                    return Ok(LuaValue::Table(table));
                }
                let cmd_name: String = if let LuaValue::String(cmd) = args[0].clone() {
                    cmd.to_str().unwrap().to_owned()
                } else {
                    "".to_owned()
                };

                let mut argv = vec![];
                for arg in &args.as_slice()[1..] {
                    if let LuaValue::Integer(i) = arg {
                        argv.push(i.to_string());
                    } else if let LuaValue::String(s) = arg{
                        argv.push(s.to_str().unwrap().to_owned());
                    }
                }

                let cmd = Command::from_argv(&cmd_name, &argv).unwrap();
                let txn_rc1 = txn_rc.clone().unwrap();
                let txn = txn_rc1.lock().await;
                let txn_ts = txn.start_timestamp();
                drop(txn);
                debug!(LOGGER, "command call from lua {:?}, txn {:?}", cmd, txn_ts);
                let result = match cmd {
                    Command::Incr(mut cmd) => cmd.incr_by(txn_rc.clone(), true).await,
                    Command::IncrBy(mut cmd) => cmd.incr_by(txn_rc.clone(), true).await,
                    Command::Decr(mut cmd) => cmd.incr_by(txn_rc.clone(), false).await,
                    Command::DecrBy(mut cmd) => cmd.incr_by(txn_rc.clone(), false).await,
                    Command::Strlen(cmd) => cmd.strlen(txn_rc.clone()).await,
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
                    Command::Persist(cmd) => cmd.persist(txn_rc.clone()).await,
                    Command::Hset(cmd) => cmd.hset(txn_rc.clone(), false).await,
                    Command::Hmset(cmd) => cmd.hset(txn_rc.clone(), true).await,
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
                    Command::Lpush(cmd) => cmd.push(txn_rc.clone(), true).await,
                    Command::Rpush(cmd) => cmd.push(txn_rc.clone(), false).await,
                    Command::Lpop(cmd) => cmd.pop(txn_rc.clone(), true).await,
                    Command::Rpop(cmd) => cmd.pop(txn_rc.clone(), false).await,
                    Command::Lrange(cmd) => cmd.lrange(txn_rc.clone()).await,
                    Command::Llen(cmd) => cmd.llen(txn_rc.clone()).await,
                    Command::Lindex(cmd) => cmd.lindex(txn_rc.clone()).await,
                    Command::Lset(cmd) => cmd.lset(txn_rc.clone()).await,
                    Command::Ltrim(cmd) => cmd.ltrim(txn_rc.clone()).await,
                    Command::Lrem(cmd) => cmd.lrem(txn_rc.clone()).await,
                    Command::Linsert(cmd) => cmd.linsert(txn_rc.clone()).await,
                    Command::Sadd(cmd) => cmd.sadd(txn_rc.clone()).await,
                    Command::Scard(cmd) => cmd.scard(txn_rc.clone()).await,
                    Command::Sismember(cmd) => cmd.sismember(txn_rc.clone()).await,
                    Command::Smismember(cmd) => cmd.smismember(txn_rc.clone()).await,
                    Command::Smembers(cmd) => cmd.smembers(txn_rc.clone()).await,
                    Command::Srandmember(cmd) => cmd.srandmember(txn_rc.clone()).await,
                    Command::Spop(cmd) => cmd.spop(txn_rc.clone()).await,
                    Command::Srem(cmd) => cmd.srem(txn_rc.clone()).await,
                    Command::Zadd(cmd) => cmd.zadd(txn_rc.clone()).await,
                    Command::Zcard(cmd) => cmd.zcard(txn_rc.clone()).await,
                    Command::Zscore(cmd) => cmd.zscore(txn_rc.clone()).await,
                    Command::Zrem(cmd) => cmd.zrem(txn_rc.clone()).await,
                    Command::Zremrangebyscore(cmd) => cmd.zremrangebyscore(txn_rc.clone()).await,
                    Command::Zrange(cmd) => cmd.zrange(txn_rc.clone()).await,
                    Command::Zrevrange(cmd) => cmd.zrevrange(txn_rc.clone()).await,
                    Command::Zrangebyscore(cmd) => cmd.zrangebyscore(txn_rc.clone(), false).await,
                    Command::Zrevrangebyscore(cmd) => cmd.zrangebyscore(txn_rc.clone(), true).await,
                    Command::Zcount(cmd) => cmd.zcount(txn_rc.clone()).await,
                    Command::Zpopmin(cmd) => cmd.zpop(txn_rc.clone(), true).await,
                    Command::Zpopmax(cmd) => cmd.zpop(txn_rc.clone(), false).await,
                    Command::Zrank(cmd) => cmd.zrank(txn_rc.clone()).await,
                    Command::Zincryby(cmd) => cmd.zincrby(txn_rc.clone()).await,
                    _ => Ok(resp_invalid_arguments()),
                };
                match result {
                    Ok(resp) => {
                        debug!(LOGGER, "response call from lua {:?}", resp);
                        let lua_resp = redis_resp_to_lua_resp(resp, _lua);
                        Ok(lua_resp)
                    }
                    Err(e) => {
                        error!(LOGGER, "response call from lua failed {}", e);
                        let table = _lua.create_table().unwrap();
                        table.raw_set("err", e.to_string()).unwrap();
                        Ok(LuaValue::Table(table))
                    }
                }
            }
        })?;
        redis.set("call", redis_call.clone())?;
        redis.set("pcall", redis_call)?;
        // register to global table
        globals.set("redis", redis)?;

        let chunk = lua.load(script);
        let resp: LuaValue = chunk.eval_async().await?;

        // convert lua value to redis value
        let redis_resp = lua_resp_to_redis_resp(resp);

        // lua clean up
        Ok(redis_resp)
    }

    pub async fn do_async_eval(
        self,
        script: &str,
        _: &Db,
        keys: &[String],
        args: &[String],
    ) -> AsyncResult<Frame> {
        Ok(self.clone().do_async_eval_inner(script, keys, args).await?)
    }

    pub async fn do_async_evalsha(
        self,
        sha1: &str,
        db: &Db,
        keys: &[String],
        args: &[String],
    ) -> AsyncResult<Frame> {
        // get script from cache with sha1 key
        let script = db.get_script(&sha1.to_lowercase());
        match script {
            Some(script) => Ok(self
                .clone()
                .do_async_eval_inner(&String::from_utf8_lossy(&script.to_vec()), keys, args)
                .await?),
            None => Ok(resp_err(REDIS_NO_MATCHING_SCRIPT_ERR)),
        }
    }
}
