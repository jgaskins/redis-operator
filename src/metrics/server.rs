//! The operator's HTTP surface: `/metrics`, `/healthz`, and `/readyz`.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context as _;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use tracing::{info, warn};

use crate::metrics::{Metrics, shutdown_signal};

/// The OpenMetrics text exposition content type. Prometheus negotiates it, and
/// serving it (rather than the older `text/plain` format) is what makes the
/// `# EOF` terminator and the `_info` metric type legal.
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

fn router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        // The Deployment had no probes at all before this server existed. These
        // exist so the kubelet can notice a process that is alive but whose
        // runtime has wedged — the failure mode where the controllers quietly
        // stop reconciling and nothing restarts them.
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz_handler))
        .with_state(metrics)
}

pub async fn run(metrics: Arc<Metrics>, addr: SocketAddr) -> anyhow::Result<()> {
    let app = router(metrics);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind metrics server on {addr}"))?;
    info!(%addr, "metrics server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    info!("metrics server shut down");
    Ok(())
}

/// Renders the cached snapshot. Does no network I/O of its own — a Redis pod
/// that hangs can make this data one poll interval stale, but it can never make
/// a Prometheus scrape slow or fail.
async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    match metrics.render() {
        Ok(body) => (StatusCode::OK, [(CONTENT_TYPE, OPENMETRICS)], body).into_response(),
        Err(err) => {
            warn!(?err, "failed to encode metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Ready once the poller has listed pods successfully at least once, which is
/// the point at which the kube client is known to work.
async fn readyz_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    if metrics.is_ready() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "waiting for the first pod poll")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Serve the real router on an ephemeral port and issue one raw HTTP/1.1
    /// request, so the assertions cover the wire bytes a Prometheus scrape
    /// would actually see.
    async fn get(metrics: Arc<Metrics>, path: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(axum::serve(listener, router(metrics)).into_future());

        let mut sock = TcpStream::connect(addr).await.expect("connect");
        sock.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
            .await
            .expect("write");
        let mut response = Vec::new();
        sock.read_to_end(&mut response).await.expect("read");
        server.abort();
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_openmetrics() {
        let response = get(Arc::new(Metrics::new()), "/metrics").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: application/openmetrics-text"),
            "{response}"
        );
        assert!(response.contains("redis_operator_build_info"), "{response}");
        assert!(response.trim_end().ends_with("# EOF"), "{response}");
    }

    #[tokio::test]
    async fn healthz_is_always_ok() {
        let response = get(Arc::new(Metrics::new()), "/healthz").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    }

    #[tokio::test]
    async fn readyz_waits_for_the_first_poll() {
        let metrics = Arc::new(Metrics::new());
        let response = get(metrics.clone(), "/readyz").await;
        assert!(
            response.starts_with("HTTP/1.1 503 Service Unavailable"),
            "{response}"
        );

        metrics.observe_poll(0, 0);
        let response = get(metrics, "/readyz").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    }
}
