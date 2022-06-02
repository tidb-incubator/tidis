use crate::cluster::Cluster as Topo;
use crate::config::LOGGER;
use crate::tikv::errors::REDIS_UNKNOWN_SUBCOMMAND;
use crate::utils::resp_err;
use crate::{Connection, Parse};
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

        Ok(Cluster::new(subcommand))
    }

    pub(crate) async fn apply(self, topo: &Topo, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.subcommand.to_uppercase().as_str() {
            "INFO" => topo.cluster_info(),
            "SLOTS" => topo.cluster_slots(),
            "NODES" => topo.cluster_nodes(),
            _ => resp_err(REDIS_UNKNOWN_SUBCOMMAND),
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
