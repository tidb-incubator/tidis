use slog::debug;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::client::Client;
use crate::cmd::Invalid;
use crate::tikv::errors::{
    REDIS_DUMPING_ERR, REDIS_INVALID_CLIENT_ID_ERR, REDIS_NOT_SUPPORTED_ERR,
    REDIS_NO_SUCH_CLIENT_ERR, REDIS_VALUE_IS_NOT_INTEGER_ERR,
};
use crate::{
    config::LOGGER,
    tikv::errors::REDIS_UNKNOWN_SUBCOMMAND,
    utils::{resp_bulk, resp_err, resp_int, resp_invalid_arguments, resp_nil, resp_ok},
    Connection, Frame, Parse, RDB,
};

#[derive(Debug, Clone)]
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

    pub(crate) async fn apply(
        self,
        command: &str,
        dst: &mut Connection,
        cur_client: Arc<Mutex<Client>>,
        clients: Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>,
    ) -> crate::Result<()> {
        let response = self.do_apply(command, cur_client, clients).await;

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

    async fn do_apply(
        self,
        command: &str,
        cur_client: Arc<Mutex<Client>>,
        clients: Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>,
    ) -> Frame {
        if !self.valid {
            return resp_invalid_arguments();
        }
        let response = match command.to_uppercase().as_str() {
            "READWRITE" => resp_ok(),
            "READONLY" => resp_ok(),
            "CLIENT" => {
                // TODO client more management will be added later
                match self.args[0].clone().to_uppercase().as_str() {
                    "ID" => resp_int(cur_client.lock().await.id() as i64),
                    "LIST" => {
                        if self.args.len() == 1 {
                            return resp_bulk(
                                encode_clients_info(
                                    clients.lock().await.clone().into_values().collect(),
                                )
                                .await,
                            );
                        }

                        return match self.args[1].clone().to_uppercase().as_str() {
                            "ID" => {
                                let mut match_clients = vec![];
                                for i in 2..self.args.len() {
                                    match self.args[i].parse::<u64>() {
                                        Ok(client_id) => {
                                            if let Some(client) =
                                                clients.lock().await.get(&client_id)
                                            {
                                                match_clients.push(client.clone());
                                            }
                                        }
                                        Err(_) => return resp_err(REDIS_INVALID_CLIENT_ID_ERR),
                                    }
                                }

                                return resp_bulk(encode_clients_info(match_clients).await);
                            }
                            _ => resp_err(REDIS_NOT_SUPPORTED_ERR),
                        };
                    }
                    "KILL" => {
                        if self.args.len() < 2 {
                            return resp_invalid_arguments();
                        }

                        // three arguments format (old format)
                        if self.args.len() == 2 {
                            let mut target_client = None;
                            {
                                let lk_clients = clients.lock().await;
                                for client in lk_clients.values() {
                                    let lk_client = client.lock().await;
                                    if lk_client.peer_addr() == self.args[1] {
                                        target_client = Some(client.clone());
                                        break;
                                    }
                                }
                            }

                            return match target_client {
                                Some(client) => {
                                    let lk_client = client.lock().await;
                                    lk_client.kill().await;
                                    resp_ok()
                                }
                                None => resp_err(REDIS_NO_SUCH_CLIENT_ERR),
                            };
                        }

                        // not match <filter> <value> format
                        if ((self.args.len() - 1) & 1) != 0 {
                            return resp_invalid_arguments();
                        }

                        let mut filter_peer_addr = "".to_owned();
                        let mut filter_local_addr = "".to_owned();
                        let mut filter_id = 0;
                        // skipme is set to yes by default in redis
                        let mut filter_skipme = true;

                        for i in (1..self.args.len()).step_by(2) {
                            let value = self.args[i + 1].clone();
                            match self.args[i].clone().to_uppercase().as_str() {
                                "ID" => match value.parse::<u64>() {
                                    Ok(client_id) => filter_id = client_id,
                                    // not REDIS_INVALID_CLIENT_ID_ERR, to be compatible with redis
                                    Err(_) => return resp_err(REDIS_VALUE_IS_NOT_INTEGER_ERR),
                                },
                                "ADDR" => filter_peer_addr = value,
                                "LADDR" => filter_local_addr = value,
                                "SKIPME" => match value.to_uppercase().as_str() {
                                    "YES" => filter_skipme = true,
                                    "NO" => filter_skipme = false,
                                    _ => return resp_invalid_arguments(),
                                },
                                _ => return resp_err(REDIS_NOT_SUPPORTED_ERR),
                            }
                        }

                        // retrieve current client id in advance for preventing dead lock during clients traverse
                        let cur_client_id = cur_client.lock().await.id();
                        let mut eligible_clients: Vec<Arc<Mutex<Client>>> = vec![];
                        {
                            let lk_clients = clients.lock().await;
                            for client in lk_clients.values() {
                                let lk_client = client.lock().await;
                                if !filter_peer_addr.is_empty()
                                    && lk_client.peer_addr() != filter_peer_addr
                                {
                                    continue;
                                }
                                if !filter_local_addr.is_empty()
                                    && lk_client.local_addr() != filter_local_addr
                                {
                                    continue;
                                }
                                if filter_id != 0 && lk_client.id() != filter_id {
                                    continue;
                                }
                                if cur_client_id == lk_client.id() && filter_skipme {
                                    continue;
                                }

                                eligible_clients.push(client.clone());
                            }
                        }

                        let killed = eligible_clients.len() as i64;
                        for eligible_client in eligible_clients {
                            let lk_eligible_client = eligible_client.lock().await;
                            lk_eligible_client.kill().await;
                        }

                        resp_int(killed)
                    }
                    "SETNAME" => {
                        if self.args.len() != 2 {
                            return resp_invalid_arguments();
                        }

                        let mut w_cur_client = cur_client.lock().await;
                        w_cur_client.set_name(&self.args[1]);
                        resp_ok()
                    }
                    "GETNAME" => {
                        let r_cur_client = cur_client.lock().await;
                        let name = r_cur_client.name().to_owned();
                        if name.is_empty() {
                            return resp_nil();
                        }

                        resp_bulk(name.into_bytes())
                    }
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
            "SAVE" => match RDB::dump().await {
                // TODO make it possible to disable this command from configuration file
                Ok(()) => resp_ok(),
                Err(e) => {
                    if e != REDIS_DUMPING_ERR {
                        RDB::reset_dumping();
                    }
                    resp_err(e)
                }
            },
            // can not reached here
            _ => resp_nil(),
        };
        response
    }
}

#[inline]
async fn encode_clients_info(clients: Vec<Arc<Mutex<Client>>>) -> Vec<u8> {
    let mut resp_list = String::new();
    for client in clients {
        let r_client = client.lock().await;
        resp_list.push_str(&r_client.to_string());
        resp_list.push('\n');
    }

    resp_list.into_bytes()
}

impl Invalid for Fake {
    fn new_invalid() -> Fake {
        Fake {
            args: vec![],
            valid: false,
        }
    }
}
