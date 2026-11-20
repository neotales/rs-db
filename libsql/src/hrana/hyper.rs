use crate::connection::{BatchRows, Conn};
use crate::hrana::connection::HttpConnection;
use crate::hrana::proto::{Batch, Stmt};
use crate::hrana::stream::HranaStream;
use crate::hrana::transaction::{HttpTransaction, TxScopeCounter};
use crate::hrana::{bind_params, unwrap_err, HranaError, HttpSend, Result};
use crate::params::Params;
use crate::transaction::Tx;
use crate::util::ConnectorService;
use crate::{Error, Rows, Statement};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{Stream, TryStreamExt};
use http::header::AUTHORIZATION;
use http::{HeaderValue, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Body as _;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use std::sync::Arc;
use std::time::Duration;

use super::StmtResultRows;

pub type ByteStream = Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + Unpin>;

fn same_origin(origin: &url::Url, target: &url::Url) -> bool {
    let permitted_scheme = match (origin.scheme(), origin.host()) {
        ("https", _) => true,
        ("http", Some(url::Host::Ipv4(ip))) => ip.is_loopback(),
        ("http", Some(url::Host::Ipv6(ip))) => ip.is_loopback(),
        _ => false,
    };
    permitted_scheme
        && target.scheme() == origin.scheme()
        && target.host() == origin.host()
        && target.host().is_some()
        && target.port_or_known_default() == origin.port_or_known_default()
        && target.username().is_empty()
        && target.password().is_none()
}

#[cfg(test)]
mod origin_tests {
    use super::{same_origin, HttpSender};
    use crate::util::{ConnectorService, Socket};
    use std::{sync::Arc, time::Duration};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tower::ServiceExt;
    use url::Url;

    #[test]
    fn destinations_must_match_original_origin() {
        let original = Url::parse("https://example.test:443").unwrap();
        for good in [
            "https://example.test/v3/pipeline",
            "https://EXAMPLE.test:443/v3/cursor",
        ] {
            assert!(same_origin(&original, &Url::parse(good).unwrap()), "{good}");
        }
        for bad in [
            "http://example.test/v3/pipeline",
            "https://other.test/v3/pipeline",
            "https://example.test:444/v3/pipeline",
            "https://attacker@example.test/v3/pipeline",
        ] {
            match Url::parse(bad) {
                Ok(target) => assert!(!same_origin(&original, &target), "{bad}"),
                Err(_) => continue,
            }
        }
        let remote_http = Url::parse("http://example.test").unwrap();
        assert!(!same_origin(&remote_http, &remote_http));
        let loopback = Url::parse("http://127.0.0.1:8080").unwrap();
        assert!(same_origin(&loopback, &loopback));
    }

    #[tokio::test]
    async fn no_token_is_sent_to_cross_origin_or_http_redirect() {
        let origin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", origin_listener.local_addr().unwrap());
        let target = format!("http://{}", target_listener.local_addr().unwrap());

        let origin_task = tokio::spawn({
            let target = target.clone();
            async move {
                let (mut socket, _) = origin_listener.accept().await.unwrap();
                let mut request = vec![0; 8192];
                let mut n = 0;
                while !request[..n].windows(4).any(|part| part == b"\r\n\r\n") {
                    let read = socket.read(&mut request[n..]).await.unwrap();
                    assert!(read > 0 && n + read < request.len());
                    n += read;
                }
                assert!(String::from_utf8_lossy(&request[..n]).contains("Bearer test-token"));
                socket.write_all(format!(
                    "HTTP/1.1 302 Found\r\nLocation: {target}/v3/pipeline\r\nContent-Length: 0\r\n\r\n"
                ).as_bytes()).await.unwrap();
            }
        });

        let connector = hyper_util::client::legacy::connect::HttpConnector::new()
            .map_err(|e| e.into())
            .map_response(|socket| Box::new(socket) as Box<dyn Socket>);
        let sender = HttpSender::new(ConnectorService::new(connector), &origin, None, None, None);
        assert!(sender
            .clone()
            .send(
                Arc::from(format!("{origin}/v3/pipeline")),
                Arc::from("Bearer test-token"),
                "{}".into()
            )
            .await
            .is_err());
        assert!(sender
            .send(
                Arc::from(format!("{target}/v3/pipeline")),
                Arc::from("Bearer test-token"),
                r#"{"baton":"test-baton"}"#.into()
            )
            .await
            .is_err());
        origin_task.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), target_listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn non_success_does_not_wait_for_unbounded_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 500 Error\r\nTransfer-Encoding: chunked\r\n\r\n1000000\r\n")
                .await
                .unwrap();
            let _ = socket.write_all(&vec![b'x'; 1024 * 1024]).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let connector = hyper_util::client::legacy::connect::HttpConnector::new()
            .map_err(|e| e.into())
            .map_response(|socket| Box::new(socket) as Box<dyn Socket>);
        let sender = HttpSender::new(ConnectorService::new(connector), &origin, None, None, None);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            sender.send(
                Arc::from(format!("{origin}/v3/pipeline")),
                Arc::from("Bearer test"),
                "{}".into(),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(result, Ok(Err(_))),
            "non-success response must return without reading the body"
        );
    }

    #[tokio::test]
    async fn verified_tls_accepts_expected_ca_and_rejects_wrong_ca_and_hostname() {
        use tokio_rustls::rustls::{
            pki_types::PrivatePkcs8KeyDer, ClientConfig, RootCertStore, ServerConfig,
        };

        // Force a genuinely ambiguous feature graph, not a single-provider
        // test that succeeds only because rustls installs a process default.
        let _ = rustls::crypto::aws_lc_rs::default_provider();
        let _ = rustls::crypto::ring::default_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());
        for (trusted, matching_name) in [(true, true), (false, true), (true, false)] {
            let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert = generated.cert.der().clone();
            let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der());
            let server_config =
                ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert.clone()], key.into())
                .unwrap();
            let mut roots = RootCertStore::empty();
            if trusted {
                roots.add(cert).unwrap();
            } else {
                roots
                    .add(
                        rcgen::generate_simple_self_signed(vec!["localhost".into()])
                            .unwrap()
                            .cert
                            .der()
                            .clone(),
                    )
                    .unwrap();
            }
            let client_config =
                ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
                if let Ok(mut socket) = acceptor.accept(socket).await {
                    let mut request = [0; 4096];
                    let _ = socket.read(&mut request).await;
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                        .await;
                }
            });
            let connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(client_config)
                .https_only()
                .enable_http1()
                .build()
                .map_response(|socket| Box::new(socket) as Box<dyn Socket>);
            let host = if matching_name {
                "localhost"
            } else {
                "127.0.0.1"
            };
            let origin = format!("https://{host}:{port}");
            let sender =
                HttpSender::new(ConnectorService::new(connector), &origin, None, None, None);
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                sender.send(
                    Arc::from(format!("{origin}/v3/pipeline")),
                    Arc::from("Bearer synthetic"),
                    "{}".into(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(result.is_ok(), trusted && matching_name);
            server.await.unwrap();
        }
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    }

    #[tokio::test]
    async fn server_base_url_cannot_redirect_baton_or_bearer() {
        use crate::hrana::{
            proto::{Batch, Stmt},
            stream::HranaStream,
        };

        for cursor in [false, true] {
            let origin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", origin_listener.local_addr().unwrap());
            let target = format!("http://{}", target_listener.local_addr().unwrap());
            let response = if cursor {
                format!(
                    "{}\n",
                    serde_json::json!({"baton":"test-baton", "base_url":target})
                )
            } else {
                serde_json::json!({
                    "baton":"test-baton",
                    "base_url":target,
                    "results":[{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}}]
                }).to_string()
            };
            let server = tokio::spawn(async move {
                let (mut socket, _) = origin_listener.accept().await.unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).await.unwrap();
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                            response.len(),
                            response
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            });

            let connector = hyper_util::client::legacy::connect::HttpConnector::new()
                .map_err(|e| e.into())
                .map_response(|socket| Box::new(socket) as Box<dyn Socket>);
            let sender =
                HttpSender::new(ConnectorService::new(connector), &origin, None, None, None);
            let stream = HranaStream::open(
                sender,
                Arc::from(format!("{origin}/v3/pipeline")),
                Arc::from(format!("{origin}/v3/cursor")),
                Arc::from("Bearer test-token"),
            );
            if cursor {
                assert!(stream
                    .cursor(Batch::single(Stmt::new("SELECT 1", true)))
                    .await
                    .is_ok());
            } else {
                assert!(stream.get_autocommit().await.is_ok());
            }
            assert!(stream.get_autocommit().await.is_err());
            drop(stream);
            server.await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(100), target_listener.accept())
                    .await
                    .is_err()
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpSender {
    inner: Client<ConnectorService, Full<Bytes>>,
    origin: Option<url::Url>,
    version: HeaderValue,
    namespace: Option<HeaderValue>,
    #[cfg(any(feature = "remote", feature = "sync"))]
    remote_encryption: Option<crate::database::EncryptionContext>,
}

impl HttpSender {
    pub fn new(
        connector: ConnectorService,
        base_url: &str,
        version: Option<&str>,
        namespace: Option<&str>,
        #[cfg(any(feature = "remote", feature = "sync"))] remote_encryption: Option<
            crate::database::EncryptionContext,
        >,
    ) -> Self {
        let ver = version.unwrap_or(env!("CARGO_PKG_VERSION"));

        let version = HeaderValue::try_from(format!("libsql-remote-{ver}")).unwrap();
        let namespace = namespace.map(|v| HeaderValue::try_from(v).unwrap());

        let inner = Client::builder(TokioExecutor::new()).build(connector);
        Self {
            inner,
            origin: url::Url::parse(&crate::util::coerce_url_scheme(base_url.to_owned())).ok(),
            version,
            namespace,
            #[cfg(any(feature = "remote", feature = "sync"))]
            remote_encryption,
        }
    }

    async fn send(
        self,
        url: Arc<str>,
        auth: Arc<str>,
        body: String,
    ) -> Result<super::HttpBody<ByteStream>> {
        // Hrana responses can supply a new base_url. Never send the bearer token
        // or baton to a different origin, even if the server requested it.
        let target =
            url::Url::parse(&url).map_err(|_| HranaError::Http("invalid Hrana URL".into()))?;
        let allowed = self
            .origin
            .as_ref()
            .is_some_and(|origin| same_origin(origin, &target));
        if !allowed {
            return Err(HranaError::Http("cross-origin Hrana URL rejected".into()));
        }
        // Send the parsed, canonical URL that was checked above, rather than
        // allowing the URL and HTTP URI parsers to interpret different strings.
        let mut req_builder = hyper::Request::post(target.as_str())
            .header(AUTHORIZATION, auth.as_ref())
            .header("x-libsql-client-version", self.version.clone());

        if let Some(namespace) = self.namespace {
            req_builder = req_builder.header("x-namespace", namespace);
        }

        #[cfg(any(feature = "remote", feature = "sync"))]
        if let Some(remote_encryption) = &self.remote_encryption {
            req_builder =
                req_builder.header("x-turso-encryption-key", remote_encryption.key.as_string());
        }

        let req = req_builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|err| HranaError::Http(format!("{:?}", err)))?;

        let resp = self
            .inner
            .request(req)
            .await
            .map_err(|err| HranaError::Http(err.to_string()))?;

        let status = resp.status();
        if status != StatusCode::OK {
            // Drop the response instead of buffering an untrusted, potentially
            // endless error body or exposing its contents to callers.
            return Err(HranaError::Api(format!("status={status}")));
        }

        let body: super::HttpBody<ByteStream> = if resp.is_end_stream() {
            let body = resp
                .into_body()
                .collect()
                .await
                .map_err(HranaError::from)?
                .to_bytes();
            super::HttpBody::from(body)
        } else {
            let stream = resp
                .into_body()
                .into_data_stream()
                .map_err(std::io::Error::other);
            super::HttpBody::Stream(Box::new(stream))
        };

        Ok(body)
    }
}

impl HttpSend for HttpSender {
    type Stream = super::HttpBody<ByteStream>;
    type Result = BoxFuture<'static, Result<Self::Stream>>;

    fn http_send(&self, url: Arc<str>, auth: Arc<str>, body: String) -> Self::Result {
        let fut = self.clone().send(url, auth, body);
        Box::pin(fut)
    }

    fn oneshot(self, url: Arc<str>, auth: Arc<str>, body: String) {
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(self.send(url, auth, body));
        } else {
            tracing::warn!("could not send request because no runtime was available");
        }
    }
}

impl From<hyper::Error> for HranaError {
    fn from(value: hyper::Error) -> Self {
        HranaError::Http(value.to_string())
    }
}

impl HttpConnection<HttpSender> {
    pub(crate) fn new_with_connector(
        url: impl Into<String>,
        token: impl Into<String>,
        connector: ConnectorService,
        version: Option<&str>,
        namespace: Option<&str>,
        #[cfg(any(feature = "remote", feature = "sync"))] remote_encryption: Option<
            crate::database::EncryptionContext,
        >,
    ) -> Self {
        let url = url.into();
        let inner = HttpSender::new(
            connector,
            &url,
            version,
            namespace,
            #[cfg(any(feature = "remote", feature = "sync"))]
            remote_encryption,
        );
        Self::new(url, token.into(), inner)
    }
}

#[async_trait::async_trait]
impl Conn for HttpConnection<HttpSender> {
    async fn execute(&self, sql: &str, params: Params) -> crate::Result<u64> {
        self.current_stream().execute(sql, params).await
    }

    async fn execute_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        self.current_stream().execute_batch(sql).await
    }

    async fn execute_transactional_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        self.current_stream().execute_transactional_batch(sql).await
    }

    async fn prepare(&self, sql: &str) -> crate::Result<Statement> {
        let stream = self.current_stream().clone();
        let stmt = crate::hrana::Statement::new(stream, sql.to_string(), true).await?;
        Ok(Statement {
            inner: Box::new(stmt),
        })
    }

    async fn transaction(
        &self,
        tx_behavior: crate::TransactionBehavior,
    ) -> crate::Result<crate::transaction::Transaction> {
        let stream = self.open_stream();
        let mut tx = HttpTransaction::open(stream, tx_behavior)
            .await
            .map_err(|e| crate::Error::Hrana(Box::new(e)))?;
        Ok(crate::Transaction {
            inner: Box::new(tx.clone()),
            conn: crate::Connection {
                conn: Arc::new(tx.stream().clone()),
            },
            close: Some(Box::new(|| {
                // make sure that Hrana connection is closed and all uncommitted changes
                // are rolled back when we're about to drop the transaction
                if let Ok(rt) = tokio::runtime::Handle::try_current() {
                    // transaction will rollback automatically after timeout on the server side
                    // this is gracefull rollback on best-effort basis
                    rt.spawn(async move {
                        let success = tx.rollback().await.is_ok();
                        drop(tx);
                        if success {
                            tracing::trace!("transaction rollback cleanup finished");
                        } else {
                            tracing::trace!("transaction rollback cleanup failed");
                        }
                    });
                }
            })),
        })
    }

    fn interrupt(&self) -> crate::Result<()> {
        // Interrupt is a no-op for remote connections.
        Ok(())
    }

    fn busy_timeout(&self, _timeout: Duration) -> crate::Result<()> {
        // Busy timeout is a no-op for remote connections.
        Ok(())
    }

    fn is_autocommit(&self) -> bool {
        self.is_autocommit()
    }

    fn changes(&self) -> u64 {
        self.affected_row_count()
    }

    fn total_changes(&self) -> u64 {
        self.total_changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid()
    }

    async fn reset(&self) {
        self.current_stream().reset().await;
    }
}

#[async_trait::async_trait]
impl crate::statement::Stmt for crate::hrana::Statement<HttpSender> {
    fn finalize(&mut self) {}

    async fn execute(&self, params: &Params) -> crate::Result<usize> {
        self.execute(params).await
    }

    async fn query(&self, params: &Params) -> crate::Result<Rows> {
        self.query(params).await
    }

    async fn run(&self, params: &Params) -> crate::Result<()> {
        self.run(params).await
    }

    fn interrupt(&self) -> crate::Result<()> {
        Err(crate::Error::Misuse(
            "interrupt is not supported for remote connections".to_string(),
        ))
    }

    fn reset(&self) {}

    fn parameter_count(&self) -> usize {
        let stmt = &self.inner;
        stmt.args.len() + stmt.named_args.len()
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        //FIXME: actual rules of named args are pretty convoluted and may require full AST parsing. Here we basically
        //       assume, that if one needs a param name, they don't use named and un-named params mixed in.
        if !self.inner.args.is_empty() {
            return None;
        }
        let named_param = self.inner.named_args.get(idx as usize)?;
        Some(&named_param.name)
    }

    fn column_count(&self) -> usize {
        self.cols.len()
    }

    fn columns(&self) -> Vec<crate::Column<'_>> {
        //FIXME: there are several blockers here:
        // 1. We cannot know the column types before sending a query, so this method will never return results right
        //    away.
        // 2. Even if we do execute query, Hrana doesn't return all info that Column exposes.
        // 3. Even if we would like to return some of the column info ie. column [ValueType], this information is not
        //    present in Hrana [Col] but rather inferred from the row cell type.
        self.cols
            .iter()
            .map(|name| crate::Column {
                name,
                origin_name: None,
                table_name: None,
                database_name: None,
                decl_type: None,
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl Tx for HttpTransaction<HttpSender> {
    async fn commit(&mut self) -> crate::Result<()> {
        self.commit()
            .await
            .map_err(|e| crate::Error::Hrana(Box::new(e)))?;
        Ok(())
    }

    async fn rollback(&mut self) -> crate::Result<()> {
        self.rollback()
            .await
            .map_err(|e| crate::Error::Hrana(Box::new(e)))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Conn for HranaStream<HttpSender> {
    async fn execute(&self, sql: &str, params: Params) -> crate::Result<u64> {
        // SQLite: execute() will only execute a single SQL statement
        let mut parsed = crate::parser::Statement::parse(sql);
        let mut c = TxScopeCounter::default();
        if let Some(s) = parsed.next() {
            let s = s?;
            c.count(s.kind);
            let in_tx_scope = !self.is_autocommit() || c.begin_tx();
            let close = !in_tx_scope || c.end_tx();
            let mut stmt = Stmt::new(s.stmt, false);
            bind_params(params, &mut stmt);
            let result = self
                .execute_inner(stmt, close)
                .await
                .map_err(|e| crate::Error::Hrana(e.into()))?;
            Ok(result.affected_row_count)
        } else {
            Err(crate::Error::Misuse(
                "no SQL statement provided".to_string(),
            ))
        }
    }

    async fn execute_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        let mut stmts = Vec::new();
        let parse = crate::parser::Statement::parse(sql);
        let mut c = TxScopeCounter::default();
        for s in parse {
            let s = s?;
            c.count(s.kind);
            stmts.push(Stmt::new(s.stmt, false));
        }
        let in_tx_scope = !self.is_autocommit() || c.begin_tx();
        let close = !in_tx_scope || c.end_tx();
        let res = self
            .batch_inner(Batch::from_iter(stmts), close)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        unwrap_err(&res)?;
        let rows = res
            .step_results
            .into_iter()
            .map(|r| r.map(StmtResultRows::new).map(Rows::new))
            .collect::<Vec<_>>();

        Ok(BatchRows::new(rows))
    }

    async fn execute_transactional_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        let mut stmts = Vec::new();
        let parse = crate::parser::Statement::parse(sql);
        for s in parse {
            let s = s?;

            use crate::parser::StmtKind;
            if matches!(
                s.kind,
                StmtKind::TxnBegin | StmtKind::TxnBeginReadOnly | StmtKind::TxnEnd
            ) {
                return Err(Error::TransactionalBatchError(
                    "Transactions forbidden inside transactional batch".to_string(),
                ));
            }

            stmts.push(Stmt::new(s.stmt, false));
        }
        let res = self
            .batch_inner(Batch::transactional(stmts), true)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        unwrap_err(&res)?;
        let rows = res
            .step_results
            .into_iter()
            // skip the first row since this is related to the already injected
            // BEGIN statement.
            .skip(1)
            .map(|r| r.map(StmtResultRows::new).map(Rows::new))
            .collect::<Vec<_>>();

        // Skip the last row as well since this corresponds to the injected commit statement
        // that the user never sees.
        Ok(BatchRows::new_skip_last(rows, 2))
    }

    async fn prepare(&self, sql: &str) -> crate::Result<Statement> {
        let stmt = crate::hrana::Statement::new(self.clone(), sql.to_string(), true).await?;
        Ok(Statement {
            inner: Box::new(stmt),
        })
    }

    async fn transaction(
        &self,
        _tx_behavior: crate::TransactionBehavior,
    ) -> crate::Result<crate::transaction::Transaction> {
        todo!("sounds like nested transactions innit?")
    }

    fn interrupt(&self) -> crate::Result<()> {
        // Interrupt is a no-op for remote connections.
        Ok(())
    }

    fn busy_timeout(&self, _timeout: Duration) -> crate::Result<()> {
        // Busy timeout is a no-op for remote connections.
        Ok(())
    }

    fn is_autocommit(&self) -> bool {
        false // for streams this method is callable only when we're within explicit transaction
    }

    fn changes(&self) -> u64 {
        self.affected_row_count()
    }

    fn total_changes(&self) -> u64 {
        self.total_changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid()
    }

    async fn reset(&self) {
        self.reset().await;
    }
}
