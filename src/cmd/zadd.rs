use std::sync::Arc;

use crate::cmd::Parse;
use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Zadd {
    key: String,
    members: Vec<String>,
    scores: Vec<i64>,
    exists: Option<bool>,
    changed_only: bool,
    valid: bool,
}

impl Zadd {
    pub fn new(key: &str) -> Zadd {
        Zadd {
            key: key.to_string(),
            members: vec![],
            scores: vec![],
            exists: None,
            changed_only: false,
            valid: true,
        }
    }

    pub fn new_invalid() -> Zadd {
        Zadd {
            key: "".to_string(),
            members: vec![],
            scores: vec![],
            exists: None,
            changed_only: false,
            valid: false,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    pub fn set_exists(&mut self, exists: bool) {
        self.exists = Some(exists);
    }

    pub fn set_changed_only(&mut self, changed_only: bool) {
        self.changed_only = changed_only;
    }

    pub fn add_member(&mut self, member: &str) {
        self.members.push(member.to_string());
    }

    pub fn add_score(&mut self, score: i64) {
        self.scores.push(score);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zadd> {
        let key = parse.next_string()?;
        let mut zadd = Zadd::new(&key);
        let mut first_score: Option<i64>;

        // try to parse the flag
        loop {
            match parse.next_string() {
                Ok(s) if s.to_uppercase() == "NX" => {
                    zadd.set_exists(false);
                }
                Ok(s) if s.to_uppercase() == "XX" => {
                    zadd.set_exists(true);
                }
                Ok(s) if s.to_uppercase() == "CH" => zadd.set_changed_only(true),
                Ok(s) if s.to_uppercase() == "GT" => {
                    // TODO:
                }
                Ok(s) if s.to_uppercase() == "LT" => {
                    // TODO:
                }
                Ok(s) if s.to_uppercase() == "INCR" => {
                    // TODO:
                }
                Ok(s) => {
                    // check if this is a score args
                    match String::from_utf8_lossy(&s.as_bytes().to_vec()).parse::<i64>() {
                        Ok(score) => {
                            first_score = Some(score);
                            // flags parse done
                            break;
                        }
                        Err(err) => {
                            // not support flags
                            return Err(err.into());
                        }
                    }
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }

        // parse the score and member
        loop {
            if let Some(score) = first_score {
                // consume the score in last parse
                zadd.add_score(score);
                // reset first_score to None
                first_score = None;

                // parse next member
                let member = parse.next_string()?;
                zadd.add_member(&member);
            } else {
                if let Ok(score) = parse.next_int() {
                    let member = parse.next_string()?;
                    zadd.add_score(score);
                    zadd.add_member(&member);
                } else {
                    break;
                }
            }
        }

        Ok(zadd)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zadd> {
        if argv.len() == 0 {
            return Ok(Zadd::new_invalid());
        }
        let mut zadd = Zadd::new(&argv[0]);
        let mut first_score: Option<i64>;

        // try to parse the flag
        let mut idx = 1;
        loop {
            let arg = argv[idx].to_uppercase();
            if idx >= argv.len() {
                return Ok(Zadd::new_invalid());
            }
            match arg.as_str() {
                "NX" => {
                    zadd.set_exists(false);
                }
                "XX" => {
                    zadd.set_exists(true);
                }
                "CH" => zadd.set_changed_only(true),
                "GT" => {
                    // TODO:
                }
                "LT" => {
                    // TODO:
                }
                "INCR" => {
                    // TODO:
                }
                _ => {
                    // check if this is a score args
                    match String::from_utf8_lossy(&arg.as_bytes().to_vec()).parse::<i64>() {
                        Ok(score) => {
                            first_score = Some(score);
                            // flags parse done
                            break;
                        }
                        Err(_) => {
                            // not support flags
                            return Ok(Zadd::new_invalid());
                        }
                    }
                }
            }
            idx += 1;
        }

        // parse the score and member
        loop {
            if let Some(score) = first_score {
                // consume the score in last parse
                zadd.add_score(score);
                // reset first_score to None
                first_score = None;

                // parse next member
                idx += 1;
                if idx >= argv.len() {
                    return Ok(Zadd::new_invalid());
                }
                let member = &argv[idx];
                zadd.add_member(member);
            } else {
                idx += 1;
                if idx == argv.len() {
                    break;
                }
                if let Ok(score) = argv[idx].parse::<i64>() {
                    idx += 1;
                    if idx >= argv.len() {
                        return Ok(Zadd::new_invalid());
                    }
                    let member = &argv[idx];
                    zadd.add_score(score);
                    zadd.add_member(member);
                } else {
                    return Ok(Zadd::new_invalid());
                }
            }
        }

        Ok(zadd)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zadd(None).await?;
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

    pub async fn zadd(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zadd(
                    &self.key,
                    &self.members,
                    &self.scores,
                    self.exists,
                    self.changed_only,
                    false,
                )
                .await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
