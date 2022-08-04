use slog::debug;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::client::Client;
use crate::cmd::Invalid;
use crate::tikv::errors::{
    RTError, REDIS_INVALID_CLIENT_ID_ERR, REDIS_NOT_SUPPORTED_ERR, REDIS_NO_SUCH_CLIENT_ERR,
};
use crate::utils::{resp_int, resp_str};
use crate::{
    config::LOGGER,
    tikv::errors::REDIS_UNKNOWN_SUBCOMMAND,
    utils::{resp_bulk, resp_err, resp_invalid_arguments, resp_nil, resp_ok},
    Connection, Frame, Parse,
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
        cur_client: Arc<RwLock<Client>>,
        clients: Arc<RwLock<HashMap<u64, Arc<RwLock<Client>>>>>,
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
        cur_client: Arc<RwLock<Client>>,
        clients: Arc<RwLock<HashMap<u64, Arc<RwLock<Client>>>>>,
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
                    "ID" => resp_int(cur_client.read().unwrap().id() as i64),
                    "LIST" => {
                        if self.args.len() == 1 {
                            let r_clients = clients.read().unwrap();
                            return resp_str(&generate_clients_string(
                                r_clients.clone().into_values().collect(),
                            ));
                        }

                        return match self.args[1].clone().to_uppercase().as_str() {
                            "ID" => {
                                let mut match_clients = vec![];
                                let r_clients = clients.read().unwrap();
                                for i in 2..self.args.len() {
                                    match self.args[i].parse::<u64>() {
                                        Ok(client_id) => {
                                            if let Some(client) = r_clients.get(&client_id) {
                                                match_clients.push(client.clone());
                                            }
                                        }
                                        Err(_) => return resp_err(REDIS_INVALID_CLIENT_ID_ERR),
                                    }
                                }
                                return resp_str(&generate_clients_string(match_clients));
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
                            let r_clients = clients.read().unwrap();
                            for client in r_clients.values() {
                                let r_client = client.read().unwrap();
                                if r_client.peer_addr() == self.args[1] {
                                    target_client = Some(client.clone());
                                    break;
                                }
                            }
                            drop(r_clients);

                            return match kill_client(target_client, clients).await {
                                Ok(()) => resp_ok(),
                                Err(e) => resp_err(e),
                            };
                        }

                        // not match <filter> <value> format
                        if ((self.args.len() - 1) & 1) != 0 {
                            return resp_invalid_arguments();
                        }

                        let mut killed = 0;
                        for i in (1..self.args.len()).step_by(2) {
                            match self.args[i].clone().to_uppercase().as_str() {
                                "ID" => match self.args[i + 1].parse::<u64>() {
                                    Ok(client_id) => {
                                        let r_clients = clients.read().unwrap();
                                        let client = r_clients.get(&client_id);
                                        if client.is_none() {
                                            return resp_err(REDIS_NO_SUCH_CLIENT_ERR);
                                        }

                                        let target_client = client.unwrap().clone();
                                        drop(r_clients);

                                        let mut w_clients = clients.write().unwrap();
                                        let w_client = target_client.write().unwrap();
                                        w_client.kill().await;
                                        w_clients.remove(&client_id);
                                        killed += 1;
                                    }
                                    Err(_) => return resp_err(REDIS_INVALID_CLIENT_ID_ERR),
                                },
                                "ADDR" => {
                                    let mut target_client = None;
                                    let r_clients = clients.read().unwrap();
                                    for client in r_clients.values() {
                                        let r_client = client.read().unwrap();
                                        if r_client.peer_addr() == self.args[i + 1] {
                                            target_client = Some(client.clone());
                                            break;
                                        }
                                    }
                                    drop(r_clients);

                                    if kill_client(target_client, clients.clone()).await.is_ok() {
                                        killed += 1;
                                    }
                                }
                                "LADDR" => {
                                    let mut target_client = None;
                                    let r_clients = clients.read().unwrap();
                                    for client in r_clients.values() {
                                        let r_client = client.read().unwrap();
                                        if r_client.local_addr() == self.args[i + 1] {
                                            target_client = Some(client.clone());
                                            break;
                                        }
                                    }
                                    drop(r_clients);

                                    if kill_client(target_client, clients.clone()).await.is_ok() {
                                        killed += 1;
                                    }
                                }
                                _ => return resp_err(REDIS_NOT_SUPPORTED_ERR),
                            }
                        }

                        resp_int(killed)
                    }
                    "SETNAME" => {
                        if self.args.len() != 2 {
                            return resp_invalid_arguments();
                        }

                        let mut w_cur_client = cur_client.write().unwrap();
                        w_cur_client.set_name(&self.args[1]);
                        resp_ok()
                    }
                    "GETNAME" => {
                        let r_cur_client = cur_client.read().unwrap();
                        let name = r_cur_client.name();
                        if name.is_empty() {
                            return resp_nil();
                        }

                        resp_str(name)
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
            // can not reached here
            _ => resp_nil(),
        };
        response
    }
}

#[inline]
fn generate_clients_string(clients: Vec<Arc<RwLock<Client>>>) -> String {
    let mut resp_list = String::new();
    for client in clients {
        let r_client = client.read().unwrap();
        resp_list.push_str(&r_client.to_string());
        resp_list.push_str("\n");
    }

    resp_list
}

#[inline]
async fn kill_client(
    target_client: Option<Arc<RwLock<Client>>>,
    clients: Arc<RwLock<HashMap<u64, Arc<RwLock<Client>>>>>,
) -> Result<(), RTError> {
    match target_client {
        Some(client) => {
            let mut w_clients = clients.write().unwrap();
            let w_client = client.write().unwrap();
            w_clients.remove(&w_client.id());
            w_client.kill().await;
            Ok(())
        }
        None => Err(REDIS_NO_SUCH_CLIENT_ERR),
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
