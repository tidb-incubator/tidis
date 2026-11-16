//! Region boundaries only: transaction reads, buffering and retries stay in the
//! pinned Transaction API. Reuse its PD transport, TLS and MVCC key encoding.

use std::sync::Arc;
use std::time::Duration;
use tikv_client::{Config, Error, Key, Result, SecurityManager};
use tikv_client_pd::{Cluster, Connection};
use tokio::sync::RwLock;

struct Locator {
    connection: Connection,
    cluster: RwLock<Cluster>,
    timeout: Duration,
}

lazy_static! {
    static ref LOCATOR: RwLock<Option<Arc<Locator>>> = RwLock::new(None);
}

pub(crate) async fn connect(addrs: &[String], config: &Config) -> Result<()> {
    let security = match (&config.ca_path, &config.cert_path, &config.key_path) {
        (Some(ca), Some(cert), Some(key)) => SecurityManager::load(ca, cert, key.clone())?,
        _ => SecurityManager::default(),
    };
    let connection = Connection::new(Arc::new(grpcio::Environment::new(1)), Arc::new(security));
    let cluster = connection.connect_cluster(addrs, config.timeout).await?;
    *LOCATOR.write().await = Some(Arc::new(Locator {
        connection,
        cluster: RwLock::new(cluster),
        timeout: config.timeout,
    }));
    Ok(())
}

pub(super) async fn end_key(start: &Key) -> Result<Option<Key>> {
    let locator = LOCATOR
        .read()
        .await
        .clone()
        .ok_or_else(|| Error::StringError("scan region locator is not connected".into()))?;
    let encoded: Vec<u8> = start.to_encoded().into();
    let response = locator
        .cluster
        .read()
        .await
        .get_region(encoded.clone(), locator.timeout)
        .await;
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            let mut cluster = locator.cluster.write().await;
            locator
                .connection
                .reconnect(&mut cluster, locator.timeout)
                .await?;
            cluster.get_region(encoded, locator.timeout).await?
        }
    };
    if !response.has_region() {
        return Err(Error::StringError("PD returned no scan region".into()));
    }
    let encoded_end = response.get_region().get_end_key();
    if encoded_end.is_empty() {
        return Ok(None);
    }
    let end = decode_key(encoded_end)?;
    if end <= *start {
        return Err(Error::StringError("PD scan region does not advance".into()));
    }
    Ok(Some(end))
}

fn decode_key(encoded: &[u8]) -> Result<Key> {
    let mut decoded = Vec::new();
    for (index, chunk) in encoded.chunks_exact(9).enumerate() {
        let padding = (255 - chunk[8]) as usize;
        if padding > 8 || chunk[8 - padding..8].iter().any(|b| *b != 0) {
            break;
        }
        decoded.extend_from_slice(&chunk[..8 - padding]);
        if padding > 0 {
            if (index + 1) * 9 == encoded.len() {
                return Ok(decoded.into());
            }
            break;
        }
    }
    Err(Error::StringError("invalid PD region key encoding".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_key_codec() {
        for n in 0..40 {
            let key = Key::from((0..n).collect::<Vec<u8>>());
            let encoded: Vec<u8> = key.to_encoded().into();
            assert_eq!(decode_key(&encoded).unwrap(), key);
            assert!(decode_key(&encoded[..encoded.len() - 1]).is_err());
        }
        assert!(decode_key(&[255; 9]).is_err());
        assert!(decode_key(&[0; 9]).is_err());
    }
}
