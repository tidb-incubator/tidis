use std::sync::Arc;

use crate::config::is_use_txn_api;
use crate::db::Db;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::get_txn_client;
use crate::tikv::lua::LuaCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame, Parse};

use bytes::Bytes;
use mlua::Lua;
use tokio::sync::Mutex;

use crate::cmd::Invalid;
use crate::config::LOGGER;
use slog::debug;

#[derive(Debug, Clone)]
pub struct Eval {
    script: String,
    numkeys: i64,
    keys: Vec<String>,
    args: Vec<Bytes>,
    valid: bool,
}

impl Eval {
    pub fn new(script: &str, numkeys: i64) -> Eval {
        Eval {
            script: script.to_owned(),
            numkeys,
            keys: vec![],
            args: vec![],
            valid: true,
        }
    }

    /// Get the key
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub fn add_arg(&mut self, arg: Bytes) {
        self.args.push(arg);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Eval> {
        let script = parse.next_string()?;
        let numkeys = parse.next_int()?;
        if numkeys < 0 {
            return Ok(Self::new_invalid());
        }
        let mut eval = Eval::new(&script, numkeys);

        for _ in 0..eval.numkeys {
            if let Ok(key) = parse.next_string() {
                eval.add_key(key);
            } else {
                break;
            }
        }

        while let Ok(arg) = parse.next_bytes() {
            eval.add_arg(arg);
        }

        Ok(eval)
    }

    pub(crate) async fn apply(
        self,
        dst: &mut Connection,
        is_sha: bool,
        db: &Db,
        lua: &Option<Lua>,
    ) -> crate::Result<()> {
        let response = self.eval(is_sha, db, lua).await?;

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

    async fn eval(&self, is_sha: bool, db: &Db, lua: &Option<Lua>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if !is_use_txn_api() {
            return Ok(resp_err(REDIS_NOT_SUPPORTED_ERR));
        }

        // create new txn
        let client = get_txn_client()?;
        let txn = client.begin().await?;
        let txn_rc = Arc::new(Mutex::new(txn));

        let ctx = LuaCommandCtx::new(Some(txn_rc.clone()), lua);

        let resp = if is_sha {
            ctx.do_async_evalsha(&self.script, db, &self.keys, &self.args)
                .await
        } else {
            ctx.do_async_eval(&self.script, db, &self.keys, &self.args)
                .await
        };
        match resp {
            Ok(r) => {
                txn_rc.lock().await.commit().await?;
                Ok(r)
            }
            Err(e) => {
                txn_rc.lock().await.rollback().await?;
                Ok(resp_err(e))
            }
        }
    }
}

impl Invalid for Eval {
    fn new_invalid() -> Eval {
        Eval {
            script: "".to_owned(),
            numkeys: 0,
            keys: vec![],
            args: vec![],
            valid: false,
        }
    }
}
