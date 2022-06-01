use crate::utils::{
    resp_ok,
    resp_err,
};
use crate::{Connection, Parse};
use crate::config::LOGGER;
use slog::debug;

#[derive(Debug)]
pub struct Cluster {
    subcommand: String,
}

impl Cluster {
    pub fn new(subcommand: impl ToString) -> Cluster {
        Cluster {
            subcommand: subcommand.to_string(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Cluster> {
        let subcommand = parse.next_string()?;

        Ok(Debug::new(subcommand))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.subcommand.to_uppercase().as_str() {
            "INFO" => {
            },
            "SLOTS" => {
            },
            "NODES" => {},
            _ => {
                resp_err("not supported CLUSTER subcommand")
            }
        };

        debug!(LOGGER, "res, {} -> {}, {:?}", dst.local_addr(), dst.peer_addr(), response);

        dst.write_frame(&response).await?;

        Ok(())
    }

}
