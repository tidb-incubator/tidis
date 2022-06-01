use std::sync::{Arc, RwLock};

use hex::ToHex;
use sha1::{Digest, Sha1};

use crate::{
    utils::{resp_array, resp_bulk, resp_int, resp_nil, resp_str},
    Frame,
};

#[derive(Debug, Clone)]
pub struct Cluster {
    nodes: Arc<RwLock<Vec<Node>>>,
}

#[derive(Debug, Clone)]
pub struct Node {
    id: String,
    ip: String,
    port: u64,
    slots: String,
    role: String,
    flags: Option<String>,
}

impl Node {
    fn new(id: &str, ip: &str, port: u64, slots: &str, role: &str, flags: Option<String>) -> Node {
        Node {
            id: id.to_owned(),
            ip: ip.to_owned(),
            port,
            slots: slots.to_owned(),
            role: role.to_owned(),
            flags,
        }
    }
}

impl Cluster {
    pub fn new(nodes: &[Node]) -> Cluster {
        Cluster {
            nodes: Arc::new(RwLock::new(nodes.to_owned().to_vec())),
        }
    }

    pub fn build_myself(addr: &str) -> Self {
        let mut addrs = Vec::new();
        addrs.push(addr.to_owned());
        Self::build_from_meta(&addrs, addr)
    }

    fn build_from_meta(addrs: &[String], my_addr: &str) -> Self {
        let mut addrs = addrs.to_owned();
        addrs.sort();
        assert!(addrs.len() > 0);

        let slots_step = 16384 / addrs.len();
        let mut slot_start = 0;
        let nodes = addrs
            .iter()
            .map(|addr| {
                // generate id from address
                let mut hasher = Sha1::new();
                hasher.update(addr);
                let sha1 = hasher.finalize();
                let id = sha1.encode_hex::<String>();

                let mut addr_part = addr.split(":");

                let mut slot_end = slot_start + slots_step;
                if slot_end + slots_step > 16383 {
                    slot_end = 16383;
                }

                let flags = if my_addr == addr {
                    Some("myself".to_owned())
                } else {
                    None
                };

                let slots_range = format!("{}-{}", slot_start.to_string(), slot_end.to_string());

                slot_start = slot_end + 1;

                Node {
                    id,
                    ip: addr_part.next().unwrap().to_owned(),
                    port: addr_part.next().unwrap().parse::<u64>().unwrap(),
                    slots: slots_range,
                    role: "master".to_owned(),
                    flags,
                }
            })
            .collect::<Vec<Node>>();
        Self::new(&nodes)
    }

    pub fn update_topo(&mut self, addrs: &[String], my_addr: &str) {
        let mut addrs = addrs.to_owned();
        addrs.sort();
        assert!(addrs.len() > 0);

        let slots_step = 16384 / addrs.len();
        let mut slot_start = 0;
        let mut nodes = addrs
            .iter()
            .map(|addr| {
                // generate id from address
                let mut hasher = Sha1::new();
                hasher.update(addr);
                let sha1 = hasher.finalize();
                let id = sha1.encode_hex::<String>();

                let mut addr_part = addr.split(":");

                let mut slot_end = slot_start + slots_step;
                if slot_end + slots_step > 16383 {
                    slot_end = 16383;
                }

                let flags = if matches!(my_addr, addr) {
                    Some("myself".to_owned())
                } else {
                    None
                };

                let slots_range = format!("{}-{}", slot_start.to_string(), slot_end.to_string());

                slot_start = slot_end + 1;

                Node {
                    id,
                    ip: addr_part.next().unwrap().to_owned(),
                    port: addr_part.next().unwrap().parse::<u64>().unwrap(),
                    slots: slots_range,
                    role: "master".to_owned(),
                    flags,
                }
            })
            .collect::<Vec<Node>>();

        let mut nodes_guard = self.nodes.write().unwrap();
        nodes_guard.clear();
        nodes_guard.append(&mut nodes);
    }

    pub fn cluster_member_changed(&self, addrs: &[String]) -> bool {
        let mut addrs = addrs.to_owned();
        addrs.sort();

        let nodes_guard = self.nodes.read().unwrap();
        // generate addrs from myself node
        let mut local_addrs: Vec<String> = nodes_guard
            .iter()
            .map(|node| format!("{}:{}", node.ip, node.port))
            .collect();
        local_addrs.sort();

        addrs != local_addrs
    }

    fn cluster_nodes(&self) -> Frame {
        let nodes_guard = self.nodes.read().unwrap();

        let node_strs: Vec<String> = nodes_guard
            .iter()
            .map(|node| {
                let flag_and_role = if node.flags.is_none() {
                    node.role.clone()
                } else {
                    format!("{},{}", node.flags.clone().unwrap(), node.role)
                };

                let node_str = format!(
                    "{} {}:{}@0 {} - 0 0 0 connected {}",
                    node.id, node.ip, node.port, flag_and_role, node.slots
                );
                node_str
            })
            .collect();

        resp_bulk(node_strs.join("\r\n").into_bytes())
    }

    fn cluster_slots(&self) -> Frame {
        let nodes_guard = self.nodes.read().unwrap();

        let slot_ranges: Vec<Frame> = nodes_guard
            .iter()
            .map(|node| {
                let mut slot_range = Vec::with_capacity(3);

                let slots_clone = node.slots.clone();
                let mut slot = slots_clone.split("-");
                let slot_start = slot.next().unwrap().parse::<i64>().unwrap();
                let slot_end = slot.next().unwrap().parse::<i64>().unwrap();

                slot_range.push(resp_int(slot_start));
                slot_range.push(resp_int(slot_end));

                let mut node_info = Vec::with_capacity(3);
                node_info.push(resp_str(&node.ip));
                node_info.push(resp_int(node.port as i64));
                node_info.push(resp_bulk(node.id.clone().into_bytes()));

                slot_range.push(resp_array(node_info));
                resp_array(slot_range)
            })
            .collect();
        resp_array(slot_ranges)
    }
}
