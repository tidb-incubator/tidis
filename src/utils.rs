use tokio::{
    time::Duration
};
use crate::frame::Frame;

pub fn resp_ok() -> Frame {
    Frame::Simple("OK".to_string())
}

pub fn resp_err(e: &str) -> Frame {
    Frame::Error(e.to_string())
}

pub fn resp_sstr(val: &'static str) -> Frame {
    Frame::Simple(val.to_string())
}

pub fn resp_int(val: i64) -> Frame {
    Frame::Integer(val)
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}
