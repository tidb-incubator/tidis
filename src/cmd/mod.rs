mod get;
pub use get::Get;

mod mget;
pub use mget::Mget;

mod mset;
pub use mset::Mset;

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

mod exists;
pub use exists::Exists;

mod incr;
pub use incr::Incr;

mod decr;
pub use decr::Decr;

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

mod spop;
pub use spop::Spop;

mod srem;
pub use srem::Srem;

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

/// Enumeration of supported Redis commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug)]
pub enum Command {
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
    Expire(Expire),
    Exists(Exists),
    Incr(Incr),
    Decr(Decr),
    
    // hash
    Hset(Hset),
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
    // set
    Sadd(Sadd),
    Scard(Scard),
    Sismember(Sismember),
    Smismember(Smismember),
    Smembers(Smembers),
    Spop(Spop),
    Srem(Srem),
    // sorted set

    // scripts
    Eval(Eval),
    Unknown(Unknown),
}

impl Command {
    /// Parse a command from a received frame.
    ///
    /// The `Frame` must represent a Redis command supported by `mini-redis` and
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
            "expire" => Command::Expire(Expire::parse_frames(&mut parse)?),
            "exists" => Command::Exists(Exists::parse_frames(&mut parse)?),
            "incr" => Command::Incr(Incr::parse_frames(&mut parse)?),
            "decr" => Command::Decr(Decr::parse_frames(&mut parse)?),
            "hset" => Command::Hset(Hset::parse_frames(&mut parse)?),
            "hmset" => Command::Hset(Hset::parse_frames(&mut parse)?),
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
            "eval" => Command::Eval(Eval::parse_frames(&mut parse)?),
            "sadd" => Command::Sadd(Sadd::parse_frames(&mut parse)?),
            "scard" => Command::Scard(Scard::parse_frames(&mut parse)?),
            "sismember" => Command::Sismember(Sismember::parse_frames(&mut parse)?),
            "smismember" => Command::Smismember(Smismember::parse_frames(&mut parse)?),
            "smembers" => Command::Smembers(Smembers::parse_frames(&mut parse)?),
            "spop" => Command::Spop(Spop::parse_frames(&mut parse)?),
            "srem" => Command::Srem(Srem::parse_frames(&mut parse)?),
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

    /// Apply the command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(dst).await,
            SetNX(cmd) => cmd.apply(dst).await,
            SetEX(cmd) => cmd.apply(dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
            Mget(cmd) => cmd.apply(dst).await,
            Mset(cmd) => cmd.apply(dst).await,
            TTL(cmd) => cmd.apply(dst).await,
            Expire(cmd) => cmd.apply(dst).await,
            Exists(cmd) => cmd.apply(dst).await,
            Incr(cmd) => cmd.apply(dst).await,
            Decr(cmd) => cmd.apply(dst).await,
            Hset(cmd) => cmd.apply(dst).await,
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
            Eval(cmd) => cmd.apply(dst).await,
            Sadd(cmd) => cmd.apply(dst).await,
            Scard(cmd) => cmd.apply(dst).await,
            Sismember(cmd) => cmd.apply(dst).await,
            Smismember(cmd) => cmd.apply(dst).await,
            Smembers(cmd) => cmd.apply(dst).await,
            Spop(cmd) => cmd.apply(dst).await,
            Srem(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }

    /// Returns the command name
    pub(crate) fn get_name(&self) -> &str {
        match self {
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
            Command::Expire(_) => "expire",
            Command::Exists(_) => "exists",
            Command::Incr(_) => "incr",
            Command::Decr(_) => "decr",
            Command::Hset(_) => "hset",
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
            Command::Eval(_) => "eval",
            Command::Sadd(_) => "sadd",
            Command::Scard(_) => "scard",
            Command::Sismember(_) => "sismember",
            Command::Smismember(_) => "smismember",
            Command::Smembers(_) => "smembers",
            Command::Spop(_) => "spop",
            Command::Srem(_) => "srem",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
