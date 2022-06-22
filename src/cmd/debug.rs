use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::tikv::errors::REDIS_NOT_SUPPORTED_DEBUG_SUB_COMMAND_ERR;
use crate::tikv::{start_profiler, stop_profiler};
use crate::utils::{resp_err, resp_invalid_arguments, resp_ok};
use crate::{Connection, Parse};
use slog::debug;

#[derive(Debug)]
pub struct Debug {
    subcommand: String,
    valid: bool,
}

impl Debug {
    pub fn new(subcommand: impl ToString) -> Debug {
        Debug {
            subcommand: subcommand.to_string(),
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Debug> {
        let subcommand = parse.next_string()?;

        Ok(Debug::new(subcommand))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if !self.valid {
            dst.write_frame(&resp_invalid_arguments()).await?;
            return Ok(());
        }

        let response = match self.subcommand.to_lowercase().as_str() {
            "profiler_start" => {
                start_profiler();
                resp_ok()
            }
            "profiler_stop" => {
                stop_profiler();
                resp_ok()
            }
            _ => resp_err(REDIS_NOT_SUPPORTED_DEBUG_SUB_COMMAND_ERR),
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
}

impl Invalid for Debug {
    fn new_invalid() -> Debug {
        Debug {
            subcommand: "".to_owned(),
            valid: false,
        }
    }
}
