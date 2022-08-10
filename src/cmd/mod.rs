mod get;

use bytes::Bytes;
pub use get::Get;

mod del;
pub use del::Del;

mod mget;
pub use mget::Mget;

mod mset;
use mlua::Lua;
pub use mset::Mset;

mod strlen;
pub use strlen::Strlen;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod setnx;
pub use setnx::SetNX;

mod setex;
pub use setex::SetEX;

mod ttl;
pub use ttl::TTL;

mod cmdtype;
pub use cmdtype::Type;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod expire;
pub use expire::Expire;

mod persist;
pub use persist::Persist;

mod exists;
pub use exists::Exists;

mod incrdecr;
pub use incrdecr::IncrDecr;

mod hset;
pub use hset::Hset;

mod hget;
pub use hget::Hget;

mod hmget;
pub use hmget::Hmget;

mod hlen;
pub use hlen::Hlen;

mod hgetall;
pub use hgetall::Hgetall;

mod hkeys;
pub use hkeys::Hkeys;

mod hvals;
pub use hvals::Hvals;

mod hdel;
pub use hdel::Hdel;

mod hincrby;
pub use hincrby::Hincrby;

mod hexists;
pub use hexists::Hexists;

mod hstrlen;
pub use hstrlen::Hstrlen;

mod push;
pub use push::Push;

mod pop;
pub use pop::Pop;

mod lrange;
pub use lrange::Lrange;

mod llen;
pub use llen::Llen;

mod lindex;
pub use lindex::Lindex;

mod lset;
pub use lset::Lset;

mod ltrim;
pub use ltrim::Ltrim;

mod lrem;
pub use lrem::Lrem;

mod linsert;
pub use linsert::Linsert;

mod eval;
pub use eval::Eval;

mod sadd;
pub use sadd::Sadd;

mod scard;
pub use scard::Scard;

mod sismember;
pub use sismember::Sismember;

mod smismember;
pub use smismember::Smismember;

mod smembers;
pub use smembers::Smembers;

mod srandmember;
pub use srandmember::Srandmember;

mod spop;
pub use spop::Spop;

mod srem;
pub use srem::Srem;

mod zadd;
pub use zadd::Zadd;

mod zcard;
pub use zcard::Zcard;

mod zscore;
pub use zscore::Zscore;

mod zrem;
pub use zrem::Zrem;

mod zremrangebyscore;
pub use zremrangebyscore::Zremrangebyscore;

mod zremrangebyrank;
pub use zremrangebyrank::Zremrangebyrank;

mod zrange;
pub use zrange::Zrange;

mod zrevrange;
pub use zrevrange::Zrevrange;

mod zrangebyscore;
pub use zrangebyscore::Zrangebyscore;

mod zcount;
pub use zcount::Zcount;

mod zpop;
pub use zpop::Zpop;

mod zrank;
pub use zrank::Zrank;

mod zincrby;
pub use zincrby::Zincrby;

mod script;
pub use script::Script;

mod unknown;
pub use unknown::Unknown;

mod auth;
pub use auth::Auth;

mod debug;
pub use debug::Debug;

mod cluster;
pub use cluster::Cluster;

mod fake;
pub use fake::Fake;

mod multi;
pub use multi::Multi;

mod scan;
pub use scan::Scan;

use crate::{cluster::Cluster as Topo, Connection, Db, Frame, Parse, ParseError, Shutdown};

/// All commands should be implement new_invalid() for invalid check
pub trait Invalid {
    fn new_invalid() -> Self;
}

fn transform_parse<T: Invalid>(parse_res: crate::Result<T>, parse: &mut Parse) -> T {
    match parse_res {
        Ok(cmd) => {
            if parse.check_finish() {
                cmd
            } else {
                T::new_invalid()
            }
        }
        Err(_) => T::new_invalid(),
    }
}

/// Enumeration of supported Redis commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug, Clone)]
pub enum Command {
    Del(Del),
    Get(Get),
    Mget(Mget),
    Publish(Publish),
    Set(Set),
    SetNX(SetNX),
    SetEX(SetEX),
    Mset(Mset),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Type(Type),
    TTL(TTL),
    PTTL(TTL),
    Expire(Expire),
    ExpireAt(Expire),
    Pexpire(Expire),
    PexpireAt(Expire),
    Persist(Persist),
    Exists(Exists),
    Incr(IncrDecr),
    Decr(IncrDecr),
    IncrBy(IncrDecr),
    DecrBy(IncrDecr),
    Strlen(Strlen),

    // hash
    Hset(Hset),
    Hmset(Hset),
    Hget(Hget),
    Hmget(Hmget),
    Hlen(Hlen),
    Hgetall(Hgetall),
    Hdel(Hdel),
    Hkeys(Hkeys),
    Hvals(Hvals),
    Hincrby(Hincrby),
    Hexists(Hexists),
    Hstrlen(Hstrlen),
    // list
    Lpush(Push),
    Rpush(Push),
    Lpop(Pop),
    Rpop(Pop),
    Lrange(Lrange),
    Llen(Llen),
    Lindex(Lindex),
    Lset(Lset),
    Ltrim(Ltrim),
    Lrem(Lrem),
    Linsert(Linsert),
    // set
    Sadd(Sadd),
    Scard(Scard),
    Sismember(Sismember),
    Smismember(Smismember),
    Smembers(Smembers),
    Srandmember(Srandmember),
    Spop(Spop),
    Srem(Srem),
    // sorted set
    Zadd(Zadd),
    Zcard(Zcard),
    Zscore(Zscore),
    Zrem(Zrem),
    Zremrangebyscore(Zremrangebyscore),
    Zremrangebyrank(Zremrangebyrank),
    Zrange(Zrange),
    Zrevrange(Zrevrange),
    Zrangebyscore(Zrangebyscore),
    Zrevrangebyscore(Zrangebyscore),
    Zcount(Zcount),
    Zpopmin(Zpop),
    Zpopmax(Zpop),
    Zrank(Zrank),
    Zincryby(Zincrby),

    // scripts
    Eval(Eval),
    Evalsha(Eval),
    Script(Script),

    Auth(Auth),
    Debug(Debug),

    Cluster(Cluster),
    ReadWrite(Fake),
    ReadOnly(Fake),
    Client(Fake),
    Info(Fake),

    // multi/exec/abort
    Multi(Multi),
    Exec(Multi),
    Discard(Multi),

    Scan(Scan),
    // Xscan command is same as scan, for testing purpose, avoid some client decoding the response
    Xscan(Scan),

    Unknown(Unknown),
}

impl Command {
    /// Parse a command from a received frame.
    ///
    /// The `Frame` must represent a Redis command supported and
    /// be the array variant.
    ///
    /// # Returns
    ///
    /// On success, the command value is returned, otherwise, `Err` is returned.
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // The frame  value is decorated with `Parse`. `Parse` provides a
        // "cursor" like API which makes parsing the command easier.
        //
        // The frame value must be an array variant. Any other frame variants
        // result in an error being returned.
        let mut parse = Parse::new(frame)?;

        // All redis commands begin with the command name as a string. The name
        // is read and converted to lower cases in order to do case sensitive
        // matching.
        let command_name = parse.next_string()?.to_lowercase();
        // Match the command name, delegating the rest of the parsing to the
        // specific command.
        let command = match &command_name[..] {
            "del" => Command::Del(transform_parse(Del::parse_frames(&mut parse), &mut parse)),
            "get" => Command::Get(transform_parse(Get::parse_frames(&mut parse), &mut parse)),
            "publish" => Command::Publish(transform_parse(
                Publish::parse_frames(&mut parse),
                &mut parse,
            )),
            "set" => Command::Set(transform_parse(Set::parse_frames(&mut parse), &mut parse)),
            "setnx" => Command::SetNX(transform_parse(SetNX::parse_frames(&mut parse), &mut parse)),
            "setex" => Command::SetEX(transform_parse(SetEX::parse_frames(&mut parse), &mut parse)),
            "subscribe" => Command::Subscribe(transform_parse(
                Subscribe::parse_frames(&mut parse),
                &mut parse,
            )),
            "unsubscribe" => Command::Unsubscribe(transform_parse(
                Unsubscribe::parse_frames(&mut parse),
                &mut parse,
            )),
            "ping" => Command::Ping(transform_parse(Ping::parse_frames(&mut parse), &mut parse)),
            "type" => Command::Type(transform_parse(Type::parse_frames(&mut parse), &mut parse)),
            "mget" => Command::Mget(transform_parse(Mget::parse_frames(&mut parse), &mut parse)),
            "mset" => Command::Mset(transform_parse(Mset::parse_frames(&mut parse), &mut parse)),
            "ttl" => Command::TTL(transform_parse(TTL::parse_frames(&mut parse), &mut parse)),
            "pttl" => Command::PTTL(transform_parse(TTL::parse_frames(&mut parse), &mut parse)),
            "expire" => Command::Expire(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "expireat" => Command::ExpireAt(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "pexpire" => Command::Pexpire(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "pexpireat" => Command::PexpireAt(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "persist" => Command::Persist(transform_parse(
                Persist::parse_frames(&mut parse),
                &mut parse,
            )),
            "exists" => Command::Exists(transform_parse(
                Exists::parse_frames(&mut parse),
                &mut parse,
            )),
            "incr" => Command::Incr(transform_parse(
                IncrDecr::parse_frames(&mut parse, true),
                &mut parse,
            )),
            "decr" => Command::Decr(transform_parse(
                IncrDecr::parse_frames(&mut parse, true),
                &mut parse,
            )),
            "incrby" => Command::IncrBy(transform_parse(
                IncrDecr::parse_frames(&mut parse, false),
                &mut parse,
            )),
            "decrby" => Command::DecrBy(transform_parse(
                IncrDecr::parse_frames(&mut parse, false),
                &mut parse,
            )),
            "strlen" => Command::Strlen(transform_parse(
                Strlen::parse_frames(&mut parse),
                &mut parse,
            )),
            "hset" => Command::Hset(transform_parse(Hset::parse_frames(&mut parse), &mut parse)),
            "hmset" => Command::Hmset(transform_parse(Hset::parse_frames(&mut parse), &mut parse)),
            "hget" => Command::Hget(transform_parse(Hget::parse_frames(&mut parse), &mut parse)),
            "hmget" => Command::Hmget(transform_parse(Hmget::parse_frames(&mut parse), &mut parse)),
            "hlen" => Command::Hlen(transform_parse(Hlen::parse_frames(&mut parse), &mut parse)),
            "hgetall" => Command::Hgetall(transform_parse(
                Hgetall::parse_frames(&mut parse),
                &mut parse,
            )),
            "hdel" => Command::Hdel(transform_parse(Hdel::parse_frames(&mut parse), &mut parse)),
            "hkeys" => Command::Hkeys(transform_parse(Hkeys::parse_frames(&mut parse), &mut parse)),
            "hvals" => Command::Hvals(transform_parse(Hvals::parse_frames(&mut parse), &mut parse)),
            "hincrby" => Command::Hincrby(transform_parse(
                Hincrby::parse_frames(&mut parse),
                &mut parse,
            )),
            "hexists" => Command::Hexists(transform_parse(
                Hexists::parse_frames(&mut parse),
                &mut parse,
            )),
            "hstrlen" => Command::Hstrlen(transform_parse(
                Hstrlen::parse_frames(&mut parse),
                &mut parse,
            )),
            "lpush" => Command::Lpush(transform_parse(Push::parse_frames(&mut parse), &mut parse)),
            "rpush" => Command::Rpush(transform_parse(Push::parse_frames(&mut parse), &mut parse)),
            "lpop" => Command::Lpop(transform_parse(Pop::parse_frames(&mut parse), &mut parse)),
            "rpop" => Command::Rpop(transform_parse(Pop::parse_frames(&mut parse), &mut parse)),
            "lrange" => Command::Lrange(transform_parse(
                Lrange::parse_frames(&mut parse),
                &mut parse,
            )),
            "llen" => Command::Llen(transform_parse(Llen::parse_frames(&mut parse), &mut parse)),
            "lindex" => Command::Lindex(transform_parse(
                Lindex::parse_frames(&mut parse),
                &mut parse,
            )),
            "lset" => Command::Lset(transform_parse(Lset::parse_frames(&mut parse), &mut parse)),
            "ltrim" => Command::Ltrim(transform_parse(Ltrim::parse_frames(&mut parse), &mut parse)),
            "lrem" => Command::Lrem(transform_parse(Lrem::parse_frames(&mut parse), &mut parse)),
            "linsert" => Command::Linsert(transform_parse(
                Linsert::parse_frames(&mut parse),
                &mut parse,
            )),
            "eval" => Command::Eval(transform_parse(Eval::parse_frames(&mut parse), &mut parse)),
            "evalsha" => {
                Command::Evalsha(transform_parse(Eval::parse_frames(&mut parse), &mut parse))
            }
            "script" => Command::Script(transform_parse(
                Script::parse_frames(&mut parse),
                &mut parse,
            )),
            "sadd" => Command::Sadd(transform_parse(Sadd::parse_frames(&mut parse), &mut parse)),
            "scard" => Command::Scard(transform_parse(Scard::parse_frames(&mut parse), &mut parse)),
            "sismember" => Command::Sismember(transform_parse(
                Sismember::parse_frames(&mut parse),
                &mut parse,
            )),
            "smismember" => Command::Smismember(transform_parse(
                Smismember::parse_frames(&mut parse),
                &mut parse,
            )),
            "smembers" => Command::Smembers(transform_parse(
                Smembers::parse_frames(&mut parse),
                &mut parse,
            )),
            "srandmember" => Command::Srandmember(transform_parse(
                Srandmember::parse_frames(&mut parse),
                &mut parse,
            )),
            "spop" => Command::Spop(transform_parse(Spop::parse_frames(&mut parse), &mut parse)),
            "srem" => Command::Srem(transform_parse(Srem::parse_frames(&mut parse), &mut parse)),
            "zadd" => Command::Zadd(transform_parse(Zadd::parse_frames(&mut parse), &mut parse)),
            "zcard" => Command::Zcard(transform_parse(Zcard::parse_frames(&mut parse), &mut parse)),
            "zscore" => Command::Zscore(transform_parse(
                Zscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrem" => Command::Zrem(transform_parse(Zrem::parse_frames(&mut parse), &mut parse)),
            "zremrangebyscore" => Command::Zremrangebyscore(transform_parse(
                Zremrangebyscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zremrangebyrank" => Command::Zremrangebyrank(transform_parse(
                Zremrangebyrank::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrange" => Command::Zrange(transform_parse(
                Zrange::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrevrange" => Command::Zrevrange(transform_parse(
                Zrevrange::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrangebyscore" => Command::Zrangebyscore(transform_parse(
                Zrangebyscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrevrangebyscore" => Command::Zrevrangebyscore(transform_parse(
                Zrangebyscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zcount" => Command::Zcount(transform_parse(
                Zcount::parse_frames(&mut parse),
                &mut parse,
            )),
            "zpopmin" => {
                Command::Zpopmin(transform_parse(Zpop::parse_frames(&mut parse), &mut parse))
            }
            "zpopmax" => {
                Command::Zpopmax(transform_parse(Zpop::parse_frames(&mut parse), &mut parse))
            }
            "zrank" => Command::Zrank(transform_parse(Zrank::parse_frames(&mut parse), &mut parse)),
            "zincrby" => Command::Zincryby(transform_parse(
                Zincrby::parse_frames(&mut parse),
                &mut parse,
            )),
            "auth" => Command::Auth(transform_parse(Auth::parse_frames(&mut parse), &mut parse)),
            "debug" => Command::Debug(transform_parse(Debug::parse_frames(&mut parse), &mut parse)),
            "cluster" => Command::Cluster(transform_parse(
                Cluster::parse_frames(&mut parse),
                &mut parse,
            )),
            "readwrite" => Command::ReadWrite(transform_parse(
                Fake::parse_frames(&mut parse, "readwrite"),
                &mut parse,
            )),
            "readonly" => Command::ReadOnly(transform_parse(
                Fake::parse_frames(&mut parse, "readonly"),
                &mut parse,
            )),
            "client" => Command::Client(transform_parse(
                Fake::parse_frames(&mut parse, "client"),
                &mut parse,
            )),
            "info" => Command::Info(transform_parse(
                Fake::parse_frames(&mut parse, "info"),
                &mut parse,
            )),
            "multi" => Command::Multi(Multi::new()),
            "exec" => Command::Exec(Multi::new()),
            "discard" => Command::Discard(Multi::new()),
            "scan" => Command::Scan(transform_parse(Scan::parse_frames(&mut parse), &mut parse)),
            "xscan" => Command::Scan(transform_parse(Scan::parse_frames(&mut parse), &mut parse)),
            _ => {
                // The command is not recognized and an Unknown command is
                // returned.
                //
                // `return` is called here to skip the `finish()` call below. As
                // the command is not recognized, there is most likely
                // unconsumed fields remaining in the `Parse` instance.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.check_and_ensure_finish();

        // The command has been successfully parsed
        Ok(command)
    }

    pub fn from_argv(cmd_name: &str, argv: &Vec<Bytes>) -> crate::Result<Command> {
        let command_name = cmd_name.to_owned().to_lowercase();
        // Match the command name, delegating the rest of the parsing to the
        // specific command.
        let command = match &command_name[..] {
            "decr" => Command::Decr(IncrDecr::parse_argv(argv, true)?),
            "incr" => Command::Incr(IncrDecr::parse_argv(argv, true)?),
            "incrby" => Command::IncrBy(IncrDecr::parse_argv(argv, false)?),
            "decrby" => Command::DecrBy(IncrDecr::parse_argv(argv, false)?),
            "strlen" => Command::Strlen(Strlen::parse_argv(argv)?),
            "del" => Command::Del(Del::parse_argv(argv)?),
            "type" => Command::Type(Type::parse_argv(argv)?),
            "exists" => Command::Exists(Exists::parse_argv(argv)?),
            "get" => Command::Get(Get::parse_argv(argv)?),
            "set" => Command::Set(Set::parse_argv(argv)?),
            "setnx" => Command::SetNX(SetNX::parse_argv(argv)?),
            "setex" => Command::SetEX(SetEX::parse_argv(argv)?),
            "mget" => Command::Mget(Mget::parse_argv(argv)?),
            "mset" => Command::Mset(Mset::parse_argv(argv)?),
            "ttl" => Command::TTL(TTL::parse_argv(argv)?),
            "pttl" => Command::PTTL(TTL::parse_argv(argv)?),
            "expire" => Command::Expire(Expire::parse_argv(argv)?),
            "expireat" => Command::ExpireAt(Expire::parse_argv(argv)?),
            "pexpire" => Command::Pexpire(Expire::parse_argv(argv)?),
            "pexpireat" => Command::PexpireAt(Expire::parse_argv(argv)?),
            "persist" => Command::Persist(Persist::parse_argv(argv)?),
            "hset" => Command::Hset(Hset::parse_argv(argv)?),
            "hmset" => Command::Hmset(Hset::parse_argv(argv)?),
            "hget" => Command::Hget(Hget::parse_argv(argv)?),
            "hmget" => Command::Hmget(Hmget::parse_argv(argv)?),
            "hlen" => Command::Hlen(Hlen::parse_argv(argv)?),
            "hgetall" => Command::Hgetall(Hgetall::parse_argv(argv)?),
            "hdel" => Command::Hdel(Hdel::parse_argv(argv)?),
            "hkeys" => Command::Hkeys(Hkeys::parse_argv(argv)?),
            "hvals" => Command::Hvals(Hvals::parse_argv(argv)?),
            "hincrby" => Command::Hincrby(Hincrby::parse_argv(argv)?),
            "hexists" => Command::Hexists(Hexists::parse_argv(argv)?),
            "hstrlen" => Command::Hstrlen(Hstrlen::parse_argv(argv)?),
            "lpush" => Command::Lpush(Push::parse_argv(argv)?),
            "rpush" => Command::Rpush(Push::parse_argv(argv)?),
            "lpop" => Command::Lpop(Pop::parse_argv(argv)?),
            "rpop" => Command::Rpop(Pop::parse_argv(argv)?),
            "lrange" => Command::Lrange(Lrange::parse_argv(argv)?),
            "llen" => Command::Llen(Llen::parse_argv(argv)?),
            "lindex" => Command::Lindex(Lindex::parse_argv(argv)?),
            "lset" => Command::Lset(Lset::parse_argv(argv)?),
            "ltrim" => Command::Ltrim(Ltrim::parse_argv(argv)?),
            "lrem" => Command::Lrem(Lrem::parse_argv(argv)?),
            "linsert" => Command::Linsert(Linsert::parse_argv(argv)?),
            "sadd" => Command::Sadd(Sadd::parse_argv(argv)?),
            "scard" => Command::Scard(Scard::parse_argv(argv)?),
            "sismember" => Command::Sismember(Sismember::parse_argv(argv)?),
            "smismember" => Command::Smismember(Smismember::parse_argv(argv)?),
            "smembers" => Command::Smembers(Smembers::parse_argv(argv)?),
            "srandmember" => Command::Srandmember(Srandmember::parse_argv(argv)?),
            "spop" => Command::Spop(Spop::parse_argv(argv)?),
            "srem" => Command::Srem(Srem::parse_argv(argv)?),
            "zadd" => Command::Zadd(Zadd::parse_argv(argv)?),
            "zcard" => Command::Zcard(Zcard::parse_argv(argv)?),
            "zscore" => Command::Zscore(Zscore::parse_argv(argv)?),
            "zrem" => Command::Zrem(Zrem::parse_argv(argv)?),
            "zremrangebyscore" => Command::Zremrangebyscore(Zremrangebyscore::parse_argv(argv)?),
            "zremrangebyrank" => Command::Zremrangebyrank(Zremrangebyrank::parse_argv(argv)?),
            "zrange" => Command::Zrange(Zrange::parse_argv(argv)?),
            "zrevrange" => Command::Zrevrange(Zrevrange::parse_argv(argv)?),
            "zrangebyscore" => Command::Zrangebyscore(Zrangebyscore::parse_argv(argv)?),
            "zrevrangebyscore" => Command::Zrevrangebyscore(Zrangebyscore::parse_argv(argv)?),
            "zcount" => Command::Zcount(Zcount::parse_argv(argv)?),
            "zpopmin" => Command::Zpopmin(Zpop::parse_argv(argv)?),
            "zpopmax" => Command::Zpopmax(Zpop::parse_argv(argv)?),
            "zrank" => Command::Zrank(Zrank::parse_argv(argv)?),
            "zincrby" => Command::Zincryby(Zincrby::parse_argv(argv)?),
            "scan" => Command::Scan(Scan::parse_argv(argv)?),
            "xscan" => Command::Scan(Scan::parse_argv(argv)?),
            _ => {
                // The command is not recognized and an Unknown command is
                // returned.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        Ok(command)
    }

    /// Apply the command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(
        self,
        db: &Db,
        topo: &Topo,
        dst: &mut Connection,
        lua: &mut Option<Lua>,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Del(cmd) => cmd.apply(dst).await,
            Get(cmd) => cmd.apply(dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(dst).await,
            SetNX(cmd) => cmd.apply(dst).await,
            SetEX(cmd) => cmd.apply(dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
            Type(cmd) => cmd.apply(dst).await,
            Mget(cmd) => cmd.apply(dst).await,
            Mset(cmd) => cmd.apply(dst).await,
            TTL(cmd) => cmd.apply(dst, false).await,
            PTTL(cmd) => cmd.apply(dst, true).await,
            Expire(cmd) => cmd.apply(dst, false, false).await,
            ExpireAt(cmd) => cmd.apply(dst, false, true).await,
            Pexpire(cmd) => cmd.apply(dst, true, false).await,
            PexpireAt(cmd) => cmd.apply(dst, true, true).await,
            Persist(cmd) => cmd.apply(dst).await,
            Exists(cmd) => cmd.apply(dst).await,
            Incr(cmd) => cmd.apply(dst, true).await,
            Decr(cmd) => cmd.apply(dst, false).await,
            IncrBy(cmd) => cmd.apply(dst, true).await,
            DecrBy(cmd) => cmd.apply(dst, false).await,
            Strlen(cmd) => cmd.apply(dst).await,
            Hset(cmd) => cmd.apply(dst, false).await,
            Hmset(cmd) => cmd.apply(dst, true).await,
            Hget(cmd) => cmd.apply(dst).await,
            Hmget(cmd) => cmd.apply(dst).await,
            Hlen(cmd) => cmd.apply(dst).await,
            Hgetall(cmd) => cmd.apply(dst).await,
            Hdel(cmd) => cmd.apply(dst).await,
            Hkeys(cmd) => cmd.apply(dst).await,
            Hvals(cmd) => cmd.apply(dst).await,
            Hincrby(cmd) => cmd.apply(dst).await,
            Hexists(cmd) => cmd.apply(dst).await,
            Hstrlen(cmd) => cmd.apply(dst).await,
            Lpush(cmd) => cmd.apply(dst, true).await,
            Rpush(cmd) => cmd.apply(dst, false).await,
            Lpop(cmd) => cmd.apply(dst, true).await,
            Rpop(cmd) => cmd.apply(dst, false).await,
            Lrange(cmd) => cmd.apply(dst).await,
            Llen(cmd) => cmd.apply(dst).await,
            Lindex(cmd) => cmd.apply(dst).await,
            Lset(cmd) => cmd.apply(dst).await,
            Ltrim(cmd) => cmd.apply(dst).await,
            Lrem(cmd) => cmd.apply(dst).await,
            Linsert(cmd) => cmd.apply(dst).await,
            Eval(cmd) => cmd.apply(dst, false, db, lua).await,
            Evalsha(cmd) => cmd.apply(dst, true, db, lua).await,
            Script(cmd) => cmd.apply(dst, db).await,
            Sadd(cmd) => cmd.apply(dst).await,
            Scard(cmd) => cmd.apply(dst).await,
            Sismember(cmd) => cmd.apply(dst).await,
            Smismember(cmd) => cmd.apply(dst).await,
            Smembers(cmd) => cmd.apply(dst).await,
            Srandmember(cmd) => cmd.apply(dst).await,
            Spop(cmd) => cmd.apply(dst).await,
            Srem(cmd) => cmd.apply(dst).await,
            Zadd(cmd) => cmd.apply(dst).await,
            Zcard(cmd) => cmd.apply(dst).await,
            Zscore(cmd) => cmd.apply(dst).await,
            Zrem(cmd) => cmd.apply(dst).await,
            Zremrangebyscore(cmd) => cmd.apply(dst).await,
            Zremrangebyrank(cmd) => cmd.apply(dst).await,
            Zrange(cmd) => cmd.apply(dst).await,
            Zrevrange(cmd) => cmd.apply(dst).await,
            Zrangebyscore(cmd) => cmd.apply(dst, false).await,
            Zrevrangebyscore(cmd) => cmd.apply(dst, true).await,
            Zcount(cmd) => cmd.apply(dst).await,
            Zpopmin(cmd) => cmd.apply(dst, true).await,
            Zpopmax(cmd) => cmd.apply(dst, false).await,
            Zrank(cmd) => cmd.apply(dst).await,
            Zincryby(cmd) => cmd.apply(dst).await,

            Debug(cmd) => cmd.apply(dst).await,

            Cluster(cmd) => cmd.apply(topo, dst).await,
            ReadWrite(cmd) => cmd.apply("readwrite", dst).await,
            ReadOnly(cmd) => cmd.apply("readonly", dst).await,
            Client(cmd) => cmd.apply("client", dst).await,
            Info(cmd) => cmd.apply("info", dst).await,

            Scan(cmd) => cmd.apply(dst).await,
            Xscan(cmd) => cmd.apply(dst).await,

            Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),

            _ => Ok(()),
        }
    }

    /// Returns the command name
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Del(_) => "del",
            Command::Get(_) => "get",
            Command::Publish(_) => "pub",
            Command::Set(_) => "set",
            Command::SetNX(_) => "setnx",
            Command::SetEX(_) => "setex",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Ping(_) => "ping",
            Command::Type(_) => "type",
            Command::Mget(_) => "mget",
            Command::Mset(_) => "mset",
            Command::TTL(_) => "ttl",
            Command::PTTL(_) => "pttl",
            Command::Expire(_) => "expire",
            Command::ExpireAt(_) => "expireat",
            Command::Pexpire(_) => "pexpire",
            Command::PexpireAt(_) => "pexpireat",
            Command::Persist(_) => "persist",
            Command::Exists(_) => "exists",
            Command::Incr(_) => "incr",
            Command::Decr(_) => "decr",
            Command::IncrBy(_) => "incrby",
            Command::DecrBy(_) => "decrby",
            Command::Strlen(_) => "strlen",
            Command::Hset(_) => "hset",
            Command::Hmset(_) => "hmset",
            Command::Hget(_) => "hget",
            Command::Hmget(_) => "hmget",
            Command::Hlen(_) => "hlen",
            Command::Hgetall(_) => "hgetall",
            Command::Hdel(_) => "hdel",
            Command::Hkeys(_) => "hkeys",
            Command::Hvals(_) => "hvals",
            Command::Hincrby(_) => "hincrby",
            Command::Hexists(_) => "hexists",
            Command::Hstrlen(_) => "hstrlen",
            Command::Lpush(_) => "lpush",
            Command::Rpush(_) => "rpush",
            Command::Lpop(_) => "lpop",
            Command::Rpop(_) => "rpop",
            Command::Lrange(_) => "lrange",
            Command::Llen(_) => "llen",
            Command::Lindex(_) => "lindex",
            Command::Lset(_) => "lset",
            Command::Ltrim(_) => "ltrim",
            Command::Lrem(_) => "lrem",
            Command::Linsert(_) => "linsert",
            Command::Eval(_) => "eval",
            Command::Evalsha(_) => "evalsha",
            Command::Script(_) => "script",
            Command::Sadd(_) => "sadd",
            Command::Scard(_) => "scard",
            Command::Sismember(_) => "sismember",
            Command::Smismember(_) => "smismember",
            Command::Smembers(_) => "smembers",
            Command::Srandmember(_) => "srandmember",
            Command::Spop(_) => "spop",
            Command::Srem(_) => "srem",
            Command::Zadd(_) => "zadd",
            Command::Zcard(_) => "zcard",
            Command::Zscore(_) => "zscore",
            Command::Zrem(_) => "zrem",
            Command::Zremrangebyscore(_) => "zremrangebyscore",
            Command::Zremrangebyrank(_) => "zremrangebyrank",
            Command::Zrange(_) => "zrange",
            Command::Zrevrange(_) => "zrevrange",
            Command::Zrangebyscore(_) => "zrangebyscore",
            Command::Zrevrangebyscore(_) => "zrevrangebyscore",
            Command::Zcount(_) => "zcount",
            Command::Zpopmin(_) => "zpopmin",
            Command::Zpopmax(_) => "zpopmax",
            Command::Zrank(_) => "zrank",
            Command::Zincryby(_) => "zincrby",
            Command::Auth(_) => "auth",
            Command::Debug(_) => "debug",
            Command::Cluster(_) => "cluster",
            Command::ReadWrite(_) => "readwrite",
            Command::ReadOnly(_) => "readonly",
            Command::Client(_) => "client",
            Command::Info(_) => "info",
            Command::Multi(_) => "multi",
            Command::Exec(_) => "exec",
            Command::Discard(_) => "discard",
            Command::Scan(_) => "scan",
            Command::Xscan(_) => "xscan",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
