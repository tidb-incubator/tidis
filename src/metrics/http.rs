use crate::config::LOGGER;
use crate::Result;
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use prometheus::{Encoder, TextEncoder};
use slog::{error, info};

use crate::metrics::{
    CURRENT_CONNECTION_COUNTER, INSTANCE_ID_GAUGER, REQUEST_COUNTER, TIKV_CLIENT_RETRIES,
};

pub struct PrometheusServer {
    listen_addr: String,
}

impl PrometheusServer {
    pub fn new(listen_addr: String, instance_id: i64) -> PrometheusServer {
        INSTANCE_ID_GAUGER.set(instance_id);
        TIKV_CLIENT_RETRIES.get();
        REQUEST_COUNTER.get();
        CURRENT_CONNECTION_COUNTER.get();

        PrometheusServer { listen_addr }
    }

    pub async fn run(&self) {
        info!(LOGGER, "Prometheus Server Listen on: {}", &self.listen_addr);
        match self.serve().await {
            Ok(_) => {}
            Err(e) => {
                error!(LOGGER, "Prometheus Got Error: {}", e.to_string());
            }
        }
    }

    async fn serve(&self) -> Result<()> {
        let addr = self.listen_addr.parse()?;
        let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
            Ok::<_, hyper::Error>(service_fn(Self::serve_req))
        }));
        serve_future.await?;
        Ok(())
    }

    async fn serve_req(_r: Request<Body>) -> Result<Response<Body>> {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();

        let response = Response::builder()
            .status(200)
            .header(CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap();

        Ok(response)
    }
}
