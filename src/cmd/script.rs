use std::sync::atomic::{AtomicBool, Ordering};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::db::Db;
use crate::tikv::errors::AsyncResult;
use crate::utils::{resp_array, resp_bulk, resp_int, resp_invalid_arguments, resp_ok, sha1hex};
use crate::{Connection, Frame, Parse};
use bytes::Bytes;
use slog::debug;

static SCRIPT_KILLED: AtomicBool = AtomicBool::new(false);

pub fn script_interuptted() -> bool {
    SCRIPT_KILLED.load(Ordering::Relaxed)
}

pub fn script_set_killed() {
    SCRIPT_KILLED.store(true, Ordering::Relaxed)
}

pub fn script_clear_killed() {
    SCRIPT_KILLED.store(false, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub struct Script {
    script: String,
    is_load: bool,
    sha1_vec: Vec<String>,
    is_exists: bool,
    is_flush: bool,
    is_kill: bool,
    valid: bool,
}

impl Script {
    pub fn new(subcommand: &str) -> Script {
        let mut is_load = false;
        let mut is_exists = false;
        let mut is_flush = false;
        let mut is_kill = false;
        match subcommand.to_uppercase().as_str() {
            "LOAD" => {
                is_load = true;
            }
            "EXISTS" => {
                is_exists = true;
            }
            "FLUSH" => {
                is_flush = true;
            }
            "KILL" => {
                is_kill = true;
            }
            _ => {}
        }
        Script {
            script: "".to_owned(),
            is_load,
            sha1_vec: vec![],
            is_exists,
            is_flush,
            is_kill,
            valid: true,
        }
    }

    pub fn set_script(&mut self, script: &str) {
        self.script = script.to_owned();
    }

    pub fn add_sha1(&mut self, sha1: &str) {
        self.sha1_vec.push(sha1.to_owned());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Script> {
        let subcommand = parse.next_string()?;
        let mut cmd = Script::new(&subcommand);
        match subcommand.to_uppercase().as_str() {
            "LOAD" => {
                let script = parse.next_string()?;
                cmd.set_script(&script);
            }
            "EXISTS" => {
                while let Ok(sha1) = parse.next_string() {
                    cmd.add_sha1(&sha1);
                }
            }
            "FLUSH" => {}
            _ => {}
        }

        Ok(cmd)
    }

    pub(crate) async fn apply(self, dst: &mut Connection, db: &Db) -> crate::Result<()> {
        let response = self.script(db).await.unwrap_or_else(Into::into);

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

    pub async fn script(&self, db: &Db) -> AsyncResult<Frame> {
        // check argument parse validation
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if self.is_load {
            // calculate script sha1
            let sha1_str = sha1hex(&self.script);
            db.set_script(sha1_str.clone(), Bytes::from(self.script.clone()));
            return Ok(resp_bulk(sha1_str.as_bytes().to_vec()));
        } else if self.is_flush {
            db.flush_script();
            return Ok(resp_ok());
        } else if self.is_exists {
            let mut resp = vec![];
            for sha1 in &self.sha1_vec {
                resp.push(resp_int(if db.get_script(sha1).is_some() { 1 } else { 0 }));
            }
            return Ok(resp_array(resp));
        } else if self.is_kill {
            script_set_killed();
        }
        Ok(resp_ok())
    }
}

impl Invalid for Script {
    fn new_invalid() -> Script {
        Script {
            script: "".to_owned(),
            is_load: false,
            sha1_vec: vec![],
            is_exists: false,
            is_flush: false,
            is_kill: false,
            valid: true,
        }
    }
}
