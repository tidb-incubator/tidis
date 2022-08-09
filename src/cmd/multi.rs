use std::sync::Arc;

use slog::{debug, error};
use tokio::sync::Mutex;

use crate::{
    config::LOGGER,
    tikv::{errors::REDIS_EXEC_ERR, get_txn_client},
    utils::{resp_array, resp_err, resp_invalid_arguments, resp_nil},
    Command, Connection, Frame,
};

#[derive(Debug, Clone)]
pub struct Multi {}

impl Multi {
    pub(crate) fn new() -> Multi {
        Multi {}
    }

    pub async fn exec(self, dst: &mut Connection, cmds: Vec<Command>) -> crate::Result<()> {
        let mut resp_arr = Vec::with_capacity(cmds.len());

        // create new txn
        let client = get_txn_client()?;
        let txn = client.begin().await?;
        let txn_rc = Some(Arc::new(Mutex::new(txn)));

        let mut response = resp_nil();
        let mut abort_on_error = false;

        for cmd in cmds {
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
                Command::Type(cmd) => cmd.cmd_type(txn_rc.clone()).await,
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
                Command::Zremrangebyrank(cmd) => cmd.zremrangebyrank(txn_rc.clone()).await,
                Command::Zrange(cmd) => cmd.zrange(txn_rc.clone()).await,
                Command::Zrevrange(cmd) => cmd.zrevrange(txn_rc.clone()).await,
                Command::Zrangebyscore(cmd) => cmd.zrangebyscore(txn_rc.clone(), false).await,
                Command::Zrevrangebyscore(cmd) => cmd.zrangebyscore(txn_rc.clone(), true).await,
                Command::Zcount(cmd) => cmd.zcount(txn_rc.clone()).await,
                Command::Zpopmin(cmd) => cmd.zpop(txn_rc.clone(), true).await,
                Command::Zpopmax(cmd) => cmd.zpop(txn_rc.clone(), false).await,
                Command::Zrank(cmd) => cmd.zrank(txn_rc.clone()).await,
                Command::Zincryby(cmd) => cmd.zincrby(txn_rc.clone()).await,
                Command::Scan(cmd) => cmd.scan(txn_rc.clone()).await,
                _ => Ok(resp_invalid_arguments()),
            };
            match result {
                Ok(resp) => {
                    // check response error
                    match resp {
                        Frame::ErrorOwned(_) | Frame::ErrorString(_) => {
                            response = resp_err(REDIS_EXEC_ERR);
                            abort_on_error = true;
                            break;
                        }
                        _ => resp_arr.push(resp),
                    }
                }
                Err(e) => {
                    error!(LOGGER, "EXECABORT {}", e);
                    response = resp_err(REDIS_EXEC_ERR);
                    abort_on_error = true;
                    break;
                }
            }
        }

        if !abort_on_error {
            response = resp_array(resp_arr);
            txn_rc.unwrap().lock().await.commit().await?;
        } else {
            txn_rc.unwrap().lock().await.rollback().await?;
        }

        debug!(
            LOGGER,
            "res, {} -> {}, {:?}",
            dst.local_addr(),
            dst.peer_addr(),
            response
        );
        dst.write_frame(&response).await?;
        Ok(())
    }
}
