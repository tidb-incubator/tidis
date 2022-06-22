use slog::debug;

use crate::cmd::Invalid;
use crate::{
    config::LOGGER,
    tikv::errors::REDIS_UNKNOWN_SUBCOMMAND,
    utils::{resp_bulk, resp_err, resp_invalid_arguments, resp_nil, resp_ok},
    Connection, Frame, Parse,
};

#[derive(Debug)]
pub struct Fake {
    args: Vec<String>,
    valid: bool,
}

impl Fake {
    pub(crate) fn parse_frames(parse: &mut Parse, command: &str) -> crate::Result<Fake> {
        let mut args = vec![];
        while let Ok(arg) = parse.next_string() {
            args.push(arg);
        }
        if (command.to_uppercase().as_str() == "CLIENT"
            || command.to_uppercase().as_str() == "INFO")
            && args.is_empty()
        {
            return Ok(Fake::new_invalid());
        }
        Ok(Fake { args, valid: true })
    }

    pub(crate) async fn apply(self, command: &str, dst: &mut Connection) -> crate::Result<()> {
        let response = self.do_apply(command, dst).await;

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

    async fn do_apply(self, command: &str, dst: &mut Connection) -> Frame {
        if !self.valid {
            return resp_invalid_arguments();
        }
        let response = match command.to_uppercase().as_str() {
            "READWRITE" => resp_ok(),
            "READONLY" => resp_ok(),
            "CLIENT" => {
                // TODO client more management will be added later
                match self.args[0].clone().to_uppercase().as_str() {
                    "LIST" => {
                        let fake_list = format!("id=0 addr={} laddr={} fd=0 name= age=0 idle=0 flags=N \
                        db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=10 obl=0 oll=0 omem=0 \
                        tot-mem=0 events=r cmd=client user=default redir=-1\r\n", dst.peer_addr(), dst.local_addr());
                        resp_bulk(fake_list.into_bytes())
                    }
                    "SETNAME" => resp_ok(),
                    _ => resp_err(REDIS_UNKNOWN_SUBCOMMAND),
                }
            }
            "INFO" => {
                match self.args[0].clone().to_uppercase().as_str() {
                    "CLIENTS" => {
                        let fake_info = "connected_clients:1\r\n".to_string();
                        resp_bulk(fake_info.into_bytes())
                    }
                    // TODO support more info command for admin
                    _ => resp_err(REDIS_UNKNOWN_SUBCOMMAND),
                }
            }
            // can not reached here
            _ => resp_nil(),
        };
        response
    }
}

impl Invalid for Fake {
    fn new_invalid() -> Fake {
        Fake {
            args: vec![],
            valid: false,
        }
    }
}
