use crate::cmd::{Invalid, Parse, ParseError};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};
use bytes::Bytes;
use slog::debug;
use std::convert::TryInto;
use std::sync::Arc;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use crate::config::LOGGER;

#[derive(Debug, Clone)]
pub struct Scan {
    start: String,
    count: u32,
    regex: String,
    valid: bool,
}

impl Scan {
    pub fn new(start: String, count: i64, regex: String) -> Scan {
        let count = match count.try_into() {
            Ok(count) if count > 0 => count,
            _ => return Scan::new_invalid(),
        };
        Scan {
            start,
            count,
            regex,
            valid: true,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scan> {
        let start = parse.next_string()?;
        let mut count = 10;
        let mut regex = ".*?".to_owned();
        loop {
            match parse.next_string() {
                Ok(flag) if flag.eq_ignore_ascii_case("COUNT") => {
                    count = parse.next_int()?;
                    if count <= 0 || count > u32::MAX as i64 {
                        return Err("invalid SCAN count".into());
                    }
                }
                Ok(flag) if flag.eq_ignore_ascii_case("MATCH") => {
                    regex = parse.next_string()?;
                }
                Ok(_) => return Err("invalid SCAN option".into()),
                Err(ParseError::EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Scan::new(start, count, regex))
    }

    pub(crate) fn parse_argv(argv: &[Bytes]) -> crate::Result<Scan> {
        let frame = Frame::Array(argv.iter().cloned().map(Frame::Bulk).collect());
        let mut parse = Parse::new(frame)?;
        // Lua consumes invalid commands through the same response path as TCP.
        Ok(match Scan::parse_frames(&mut parse) {
            Ok(scan) => scan,
            Err(_) => Scan::new_invalid(),
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.scan(None).await?;
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

    pub async fn scan(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_scan(&self.start, self.count, &self.regex)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Scan {
    fn new_invalid() -> Scan {
        Scan {
            start: "".to_owned(),
            count: 0,
            regex: "".to_owned(),
            valid: false,
        }
    }
}
