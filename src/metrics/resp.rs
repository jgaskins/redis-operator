//! A deliberately tiny Redis client: open a socket, send one command, read one
//! reply, close.
//!
//! It speaks RESP2 and nothing else. It never sends `HELLO`, so the server can
//! never switch it to RESP3 and out-of-band push frames cannot appear on the
//! wire. It never sends `AUTH` or `CLIENT SETINFO` either, which is what lets
//! the same code path work unchanged against a Sentinel — Sentinel runs a
//! restricted command table that rejects most of what a general-purpose client
//! wants to say on connect.
//!
//! That is only safe because the CRDs expose no way to configure a password or
//! TLS (see `crate::crd::redis::RedisSpec`). If either ever lands, this module
//! is what has to change, and reaching for the `redis` crate at that point is
//! the right call.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::error::{Error, Result};

/// Refuse to buffer a reply larger than this. A full `INFO` is a few kilobytes;
/// anything approaching a megabyte means we are not talking to Redis, and we
/// would rather fail one scrape than let a bad target chew through the
/// operator's 256Mi memory limit.
const MAX_REPLY_BYTES: i64 = 8 * 1024 * 1024;

/// `INFO` with no section argument, as a RESP array.
///
/// No argument on purpose. `INFO all` and `INFO everything` add the
/// `# Commandstats` and `# Latencystats` sections, which carry one entry per
/// command the server has actually executed — an unbounded, workload-shaped key
/// space that would turn every scrape into a cardinality bomb. The default
/// sections have a fixed field set.
const INFO_CMD: &[u8] = b"*1\r\n$4\r\nINFO\r\n";

/// Fetch `INFO` from `addr`.
///
/// The timeout covers connect, write, and read together: a target that accepts
/// the connection and then goes silent is the failure mode most worth bounding,
/// and splitting the budget across phases would only make the worst case longer.
pub async fn info(addr: SocketAddr, timeout: Duration) -> Result<String> {
    match tokio::time::timeout(timeout, info_inner(addr)).await {
        Ok(result) => result,
        Err(_) => Err(Error::Timeout(timeout)),
    }
}

async fn info_inner(addr: SocketAddr) -> Result<String> {
    let stream = TcpStream::connect(addr).await?;
    // One small request, one reply, then close — Nagle could only ever add
    // latency here.
    stream.set_nodelay(true)?;
    let mut conn = BufReader::new(stream);
    conn.get_mut().write_all(INFO_CMD).await?;
    read_bulk_string(&mut conn).await
}

/// Read a single RESP bulk string (`$<len>\r\n<len bytes>\r\n`).
///
/// Only two reply types are reachable: a bulk string on success and a simple
/// error on failure. Anything else means the peer is not Redis, so it is a
/// protocol error rather than something to skip past.
async fn read_bulk_string(conn: &mut BufReader<TcpStream>) -> Result<String> {
    let mut header = String::new();
    if conn.read_line(&mut header).await? == 0 {
        return Err(Error::Resp("connection closed before any reply".into()));
    }
    let header = header.trim_end_matches(['\r', '\n']);
    let Some(rest) = header.get(1..) else {
        return Err(Error::Resp("empty reply header".into()));
    };

    match header.as_bytes()[0] {
        b'-' => Err(Error::Resp(rest.to_string())),
        b'$' => {
            let len: i64 = rest
                .parse()
                .map_err(|_| Error::Resp(format!("bad bulk length {rest:?}")))?;
            // A nil bulk string is a legal reply that carries no INFO. Treat it
            // as an empty body; the allowlist then exports nothing but `up`.
            if len < 0 {
                return Ok(String::new());
            }
            if len > MAX_REPLY_BYTES {
                return Err(Error::Resp(format!("reply of {len} bytes exceeds cap")));
            }
            // Payload plus the trailing CRLF, read in one shot so a reply split
            // across TCP segments still arrives whole.
            let mut buf = vec![0u8; len as usize + 2];
            conn.read_exact(&mut buf).await?;
            buf.truncate(len as usize);
            Ok(String::from_utf8_lossy(&buf).into_owned())
        }
        _ => Err(Error::Resp(format!("unexpected reply type in {header:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::net::TcpListener;

    /// Serve one connection with `reply`, and hand back what the client sent.
    ///
    /// Binding port 0 lets the OS pick, so these tests can run in parallel.
    async fn serve(reply: &'static [u8]) -> (SocketAddr, tokio::task::JoinHandle<Vec<u8>>) {
        serve_with(move |mut sock| async move {
            let mut request = vec![0u8; INFO_CMD.len()];
            sock.read_exact(&mut request).await.ok();
            sock.write_all(reply).await.ok();
            request
        })
        .await
    }

    async fn serve_with<F, Fut>(handler: F) -> (SocketAddr, tokio::task::JoinHandle<Vec<u8>>)
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Vec<u8>> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let handle = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.expect("accept");
            handler(sock).await
        });
        (addr, handle)
    }

    fn timeout() -> Duration {
        Duration::from_secs(5)
    }

    #[tokio::test]
    async fn reads_a_bulk_string_reply() {
        let (addr, _) = serve(b"$15\r\n# Server\r\nhz:10\r\n").await;
        let body = info(addr, timeout()).await.expect("info");
        assert_eq!(body, "# Server\r\nhz:10");
    }

    #[tokio::test]
    async fn sends_a_bare_info_command() {
        // Guards against anyone "improving" this to `INFO all`, which would add
        // the per-command Commandstats section.
        let (addr, server) = serve(b"$0\r\n\r\n").await;
        info(addr, timeout()).await.expect("info");
        assert_eq!(server.await.expect("join"), INFO_CMD);
    }

    #[tokio::test]
    async fn reassembles_a_reply_split_across_writes() {
        let (addr, _) = serve_with(|mut sock| async move {
            let mut request = vec![0u8; INFO_CMD.len()];
            sock.read_exact(&mut request).await.ok();
            sock.write_all(b"$8\r\nhz:").await.ok();
            tokio::task::yield_now().await;
            sock.write_all(b"10\r\nx\r\n").await.ok();
            request
        })
        .await;
        assert_eq!(info(addr, timeout()).await.expect("info"), "hz:10\r\nx");
    }

    #[tokio::test]
    async fn maps_an_error_reply_to_a_protocol_error() {
        let (addr, _) = serve(b"-NOPERM this user has no permissions\r\n").await;
        let err = info(addr, timeout()).await.expect_err("should fail");
        assert!(
            matches!(&err, Error::Resp(msg) if msg.starts_with("NOPERM")),
            "unexpected error: {err:?}",
        );
    }

    #[tokio::test]
    async fn treats_a_nil_bulk_string_as_empty() {
        let (addr, _) = serve(b"$-1\r\n").await;
        assert_eq!(info(addr, timeout()).await.expect("info"), "");
    }

    #[tokio::test]
    async fn rejects_a_reply_over_the_size_cap() {
        let (addr, _) = serve(b"$9999999999\r\n").await;
        let err = info(addr, timeout()).await.expect_err("should fail");
        assert!(
            matches!(&err, Error::Resp(msg) if msg.contains("exceeds cap")),
            "unexpected error: {err:?}",
        );
    }

    #[tokio::test]
    async fn rejects_an_unexpected_reply_type() {
        let (addr, _) = serve(b"+PONG\r\n").await;
        let err = info(addr, timeout()).await.expect_err("should fail");
        assert!(
            matches!(&err, Error::Resp(msg) if msg.contains("unexpected reply type")),
            "unexpected error: {err:?}",
        );
    }

    #[tokio::test]
    async fn reports_a_closed_connection() {
        // Consume the request, then hang up without replying. Reading first
        // makes this a clean FIN: dropping the socket with the request still
        // unread sends an RST instead, which the client would surface as an
        // I/O error rather than an end-of-stream.
        let (addr, _) = serve_with(|mut sock| async move {
            let mut request = vec![0u8; INFO_CMD.len()];
            sock.read_exact(&mut request).await.ok();
            drop(sock);
            request
        })
        .await;
        let err = info(addr, timeout()).await.expect_err("should fail");
        assert!(
            matches!(&err, Error::Resp(msg) if msg.contains("closed before any reply")),
            "unexpected error: {err:?}",
        );
    }

    #[tokio::test]
    async fn times_out_on_a_server_that_never_replies() {
        let (addr, _) = serve_with(|sock| async move {
            // Hold the connection open, silent, past the client's deadline.
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(sock);
            Vec::new()
        })
        .await;
        let err = info(addr, Duration::from_millis(50))
            .await
            .expect_err("should fail");
        assert!(matches!(err, Error::Timeout(_)), "unexpected error: {err:?}");
    }

    #[tokio::test]
    async fn reports_io_error_for_a_closed_port() {
        // Bind then drop, so the port is almost certainly unbound.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let err = info(addr, timeout()).await.expect_err("should fail");
        assert!(matches!(err, Error::Io(_)), "unexpected error: {err:?}");
    }
}
