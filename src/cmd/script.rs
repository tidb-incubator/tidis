use crate::config::LOGGER;
use crate::db::Db;
use crate::tikv::errors::AsyncResult;
use crate::utils::{resp_array, resp_bulk, resp_int, resp_invalid_arguments, resp_ok};
use crate::{Connection, Frame, Parse};
use bytes::Bytes;
use hex::ToHex;
use sha1::{Digest, Sha1};
use slog::debug;

#[derive(Debug)]
pub struct Script {
    script: String,
    is_load: bool,
    sha1_vec: Vec<String>,
    is_exists: bool,
    is_flush: bool,
    valid: bool,
}

impl Script {
    pub fn new(subcommand: &str) -> Script {
        let mut is_load = false;
        let mut is_exists = false;
        let mut is_flush = false;
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
            _ => {}
        }
        Script {
            script: "".to_owned(),
            is_load,
            sha1_vec: vec![],
            is_exists,
            is_flush,
            valid: true,
        }
    }

    pub fn new_invalid() -> Script {
        Script {
            script: "".to_owned(),
            is_load: false,
            sha1_vec: vec![],
            is_exists: false,
            is_flush: false,
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
        let response = match self.script(db).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

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
            let mut hasher = Sha1::new();
            hasher.update(&self.script);
            let sha1 = hasher.finalize();
            let sha1_str = sha1.encode_hex::<String>();
            db.set_script(sha1_str.clone(), Bytes::from(self.script.clone()));
            return Ok(resp_bulk(sha1_str.as_bytes().to_vec()));
        }
        if self.is_flush {
            db.flush_script();
            return Ok(resp_ok());
        }
        if self.is_exists {
            let mut resp = vec![];
            for sha1 in &self.sha1_vec {
                if let Some(_) = db.get_script(sha1) {
                    resp.push(resp_int(1));
                } else {
                    resp.push(resp_int(0));
                }
            }
            return Ok(resp_array(resp));
        }
        Ok(resp_ok())
    }
}
