use prometheus::{IntCounter, IntCounterVec, IntGauge, HistogramVec, exponential_buckets};

mod http;

pub use self::http::PrometheusServer;

lazy_static! {
    pub static ref INSTANCE_ID_GAUGER: IntGauge =
        register_int_gauge!("redistikv_instance_id", "Instance ID").unwrap();
    pub static ref TIKV_CLIENT_RETRIES: IntGauge =
        register_int_gauge!("redistikv_tikv_client_retries", "Client retries").unwrap();
    pub static ref REQUEST_COUNTER: IntCounter =
        register_int_counter!("redistikv_requests", "Request counter").unwrap();
    pub static ref CURRENT_CONNECTION_COUNTER: IntGauge =
        register_int_gauge!("redistikv_current_connections", "Current connection counter").unwrap();
    pub static ref REQUEST_CMD_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redistikv_command_requests",
        "Request command counter",
        &["cmd"]
    )
    .unwrap();
    pub static ref REQUEST_CMD_FINISH_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redistikv_command_requests_finish",
        "Request command finish counter",
        &["cmd"]
    )
    .unwrap();
    pub static ref REQUEST_CMD_HANDLE_TIME: HistogramVec = register_histogram_vec!(
        "redistikv_command_handle_time",
        "Bucketed histogram of command handle duration",
        &["cmd"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
}