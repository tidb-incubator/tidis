mod get;

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

use crate::{cluster::Cluster as Topo, Connection, Db, Frame, Parse, ParseError, Shutdown};

/// Enumeration of supported Redis commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug)]
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
    Zrange(Zrange),
    Zrevrange(Zrevrange),
    Zrangebyscore(Zrangebyscore),
    Zrevrangebyscore(Zrangebyscore),
    Zcount(Zcount),
    Zpopmin(Zpop),
    Zrank(Zrank),

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
            "del" => Command::Del(Del::parse_frames(&mut parse)?),
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "setnx" => Command::SetNX(SetNX::parse_frames(&mut parse)?),
            "setex" => Command::SetEX(SetEX::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "mget" => Command::Mget(Mget::parse_frames(&mut parse)?),
            "mset" => Command::Mset(Mset::parse_frames(&mut parse)?),
            "ttl" => Command::TTL(TTL::parse_frames(&mut parse)?),
            "pttl" => Command::PTTL(TTL::parse_frames(&mut parse)?),
            "expire" => Command::Expire(Expire::parse_frames(&mut parse)?),
            "expireat" => Command::ExpireAt(Expire::parse_frames(&mut parse)?),
            "pexpire" => Command::Pexpire(Expire::parse_frames(&mut parse)?),
            "pexpireat" => Command::PexpireAt(Expire::parse_frames(&mut parse)?),
            "persist" => Command::Persist(Persist::parse_frames(&mut parse)?),
            "exists" => Command::Exists(Exists::parse_frames(&mut parse)?),
            "incr" => Command::Incr(IncrDecr::parse_frames(&mut parse, true)?),
            "decr" => Command::Decr(IncrDecr::parse_frames(&mut parse, true)?),
            "incrby" => Command::IncrBy(IncrDecr::parse_frames(&mut parse, false)?),
            "decrby" => Command::DecrBy(IncrDecr::parse_frames(&mut parse, false)?),
            "strlen" => Command::Strlen(Strlen::parse_frames(&mut parse)?),
            "hset" => Command::Hset(Hset::parse_frames(&mut parse)?),
            "hmset" => Command::Hmset(Hset::parse_frames(&mut parse)?),
            "hget" => Command::Hget(Hget::parse_frames(&mut parse)?),
            "hmget" => Command::Hmget(Hmget::parse_frames(&mut parse)?),
            "hlen" => Command::Hlen(Hlen::parse_frames(&mut parse)?),
            "hgetall" => Command::Hgetall(Hgetall::parse_frames(&mut parse)?),
            "hdel" => Command::Hdel(Hdel::parse_frames(&mut parse)?),
            "hkeys" => Command::Hkeys(Hkeys::parse_frames(&mut parse)?),
            "hvals" => Command::Hvals(Hvals::parse_frames(&mut parse)?),
            "hincrby" => Command::Hincrby(Hincrby::parse_frames(&mut parse)?),
            "hexists" => Command::Hexists(Hexists::parse_frames(&mut parse)?),
            "hstrlen" => Command::Hstrlen(Hstrlen::parse_frames(&mut parse)?),
            "lpush" => Command::Lpush(Push::parse_frames(&mut parse)?),
            "rpush" => Command::Rpush(Push::parse_frames(&mut parse)?),
            "lpop" => Command::Lpop(Pop::parse_frames(&mut parse)?),
            "rpop" => Command::Rpop(Pop::parse_frames(&mut parse)?),
            "lrange" => Command::Lrange(Lrange::parse_frames(&mut parse)?),
            "llen" => Command::Llen(Llen::parse_frames(&mut parse)?),
            "lindex" => Command::Lindex(Lindex::parse_frames(&mut parse)?),
            "lset" => Command::Lset(Lset::parse_frames(&mut parse)?),
            "ltrim" => Command::Ltrim(Ltrim::parse_frames(&mut parse)?),
            "eval" => Command::Eval(Eval::parse_frames(&mut parse)?),
            "evalsha" => Command::Evalsha(Eval::parse_frames(&mut parse)?),
            "script" => Command::Script(Script::parse_frames(&mut parse)?),
            "sadd" => Command::Sadd(Sadd::parse_frames(&mut parse)?),
            "scard" => Command::Scard(Scard::parse_frames(&mut parse)?),
            "sismember" => Command::Sismember(Sismember::parse_frames(&mut parse)?),
            "smismember" => Command::Smismember(Smismember::parse_frames(&mut parse)?),
            "smembers" => Command::Smembers(Smembers::parse_frames(&mut parse)?),
            "srandmember" => Command::Srandmember(Srandmember::parse_frames(&mut parse)?),
            "spop" => Command::Spop(Spop::parse_frames(&mut parse)?),
            "srem" => Command::Srem(Srem::parse_frames(&mut parse)?),
            "zadd" => Command::Zadd(Zadd::parse_frames(&mut parse)?),
            "zcard" => Command::Zcard(Zcard::parse_frames(&mut parse)?),
            "zscore" => Command::Zscore(Zscore::parse_frames(&mut parse)?),
            "zrem" => Command::Zrem(Zrem::parse_frames(&mut parse)?),
            "zremrangebyscore" => {
                Command::Zremrangebyscore(Zremrangebyscore::parse_frames(&mut parse)?)
            }
            "zrange" => Command::Zrange(Zrange::parse_frames(&mut parse)?),
            "zrevrange" => Command::Zrevrange(Zrevrange::parse_frames(&mut parse)?),
            "zrangebyscore" => Command::Zrangebyscore(Zrangebyscore::parse_frames(&mut parse)?),
            "zrevrangebyscore" => {
                Command::Zrevrangebyscore(Zrangebyscore::parse_frames(&mut parse)?)
            }
            "zcount" => Command::Zcount(Zcount::parse_frames(&mut parse)?),
            "zpopmin" => Command::Zpopmin(Zpop::parse_frames(&mut parse)?),
            "zrank" => Command::Zrank(Zrank::parse_frames(&mut parse)?),
            "auth" => Command::Auth(Auth::parse_frames(&mut parse)?),
            "debug" => Command::Debug(Debug::parse_frames(&mut parse)?),
            "cluster" => Command::Cluster(Cluster::parse_frames(&mut parse)?),
            "readwrite" => Command::ReadWrite(Fake::parse_frames(&mut parse, "readwrite")?),
            "readonly" => Command::ReadOnly(Fake::parse_frames(&mut parse, "readonly")?),
            "client" => Command::Client(Fake::parse_frames(&mut parse, "client")?),
            "info" => Command::Info(Fake::parse_frames(&mut parse, "info")?),
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

        // Check if there is any remaining unconsumed fields in the `Parse`
        // value. If fields remain, this indicates an unexpected frame format
        // and an error is returned.
        parse.finish()?;

        // The command has been successfully parsed
        Ok(command)
    }

    pub fn from_argv(cmd_name: &str, argv: &Vec<String>) -> crate::Result<Command> {
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
            "zrange" => Command::Zrange(Zrange::parse_argv(argv)?),
            "zrevrange" => Command::Zrevrange(Zrevrange::parse_argv(argv)?),
            "zrangebyscore" => Command::Zrangebyscore(Zrangebyscore::parse_argv(argv)?),
            "zrevrangebyscore" => Command::Zrevrangebyscore(Zrangebyscore::parse_argv(argv)?),
            "zcount" => Command::Zcount(Zcount::parse_argv(argv)?),
            "zpopmin" => Command::Zpopmin(Zpop::parse_argv(argv)?),
            "zrank" => Command::Zrank(Zrank::parse_argv(argv)?),

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
            Zrange(cmd) => cmd.apply(dst).await,
            Zrevrange(cmd) => cmd.apply(dst).await,
            Zrangebyscore(cmd) => cmd.apply(dst, false).await,
            Zrevrangebyscore(cmd) => cmd.apply(dst, true).await,
            Zcount(cmd) => cmd.apply(dst).await,
            Zpopmin(cmd) => cmd.apply(dst, true).await,
            Zrank(cmd) => cmd.apply(dst).await,

            Debug(cmd) => cmd.apply(dst).await,

            Cluster(cmd) => cmd.apply(topo, dst).await,
            ReadWrite(cmd) => cmd.apply("readwrite", dst).await,
            ReadOnly(cmd) => cmd.apply("readonly", dst).await,
            Client(cmd) => cmd.apply("client", dst).await,
            Info(cmd) => cmd.apply("info", dst).await,

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
            Command::Zrange(_) => "zrange",
            Command::Zrevrange(_) => "zrevrange",
            Command::Zrangebyscore(_) => "zrangebyscore",
            Command::Zrevrangebyscore(_) => "zrevrangebyscore",
            Command::Zcount(_) => "zcount",
            Command::Zpopmin(_) => "zpopmin",
            Command::Zrank(_) => "zrank",
            Command::Auth(_) => "auth",
            Command::Debug(_) => "debug",
            Command::Cluster(_) => "cluster",
            Command::ReadWrite(_) => "readwrite",
            Command::ReadOnly(_) => "readonly",
            Command::Client(_) => "client",
            Command::Info(_) => "info",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
