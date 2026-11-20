use crate::{
    KlickhouseError, Result,
    block::Block,
    io::ClickhouseWrite,
    protocol::{
        self, CompressionMethod, DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH,
        DBMS_MIN_REVISION_WITH_CLIENT_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET,
        DBMS_MIN_REVISION_WITH_OPENTELEMETRY, DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO,
        DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS,
        DBMS_MIN_REVISION_WITH_VERSION_PATCH, ServerHello,
    },
};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub(crate) const CLIENT_NAME: &str =
    concat!("ClickHouse Rust-Klickhouse ", env!("CARGO_PKG_VERSION"));

pub(crate) fn check_settings_revision(revision: u64, nonempty: bool) -> Result<()> {
    if nonempty && revision < DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
        return Err(KlickhouseError::ProtocolError(
            "query settings require protocol revision 54429".to_string(),
        ));
    }
    Ok(())
}

pub struct InternalClientOut<W: ClickhouseWrite> {
    writer: W,
    pub server_hello: ServerHello,
}

pub struct ClientHello<'a> {
    pub default_database: &'a str,
    pub username: &'a str,
    pub password: &'a str,
}

#[repr(u8)]
#[derive(PartialEq, Clone, Copy)]
#[allow(unused, clippy::enum_variant_names)]
pub enum QueryKind {
    NoQuery,
    InitialQuery,
    SecondaryQuery,
}

pub struct ClientInfo<'a> {
    pub kind: QueryKind,
    pub initial_user: &'a str,
    pub initial_query_id: &'a str,
    pub initial_address: &'a str,
    // interface = TCP = 1
    pub os_user: &'a str,
    pub client_hostname: &'a str,
    pub client_name: &'a str,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_tcp_protocol_version: u64,

    // if DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
    pub quota_key: &'a str,
    // if DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH
    pub distributed_depth: u64,
    // if DBMS_MIN_REVISION_WITH_VERSION_PATCH
    pub client_version_patch: u64,
    // if DBMS_MIN_REVISION_WITH_OPENTELEMETRY
    pub open_telemetry: Option<OpenTelemetry>,
}

impl ClientInfo<'_> {
    pub async fn write<W: ClickhouseWrite>(&self, to: &mut W, revision: u64) -> Result<()> {
        to.write_u8(self.kind as u8).await?;
        if self.kind == QueryKind::NoQuery {
            return Ok(());
        }
        to.write_string(self.initial_user).await?;
        to.write_string(self.initial_query_id).await?;
        to.write_string(self.initial_address).await?;
        if revision >= protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
            to.write_u64_le(0).await?;
        }
        to.write_u8(1).await?;
        to.write_string(self.os_user).await?;
        to.write_string(self.client_hostname).await?;
        to.write_string(self.client_name).await?;
        to.write_var_uint(self.client_version_major).await?;
        to.write_var_uint(self.client_version_minor).await?;
        to.write_var_uint(self.client_tcp_protocol_version).await?;
        if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            to.write_string(self.quota_key).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
            to.write_var_uint(self.distributed_depth).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            to.write_var_uint(self.client_version_patch).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
            if let Some(telemetry) = &self.open_telemetry {
                to.write_u8(1u8).await?;
                to.write_all(&telemetry.trace_id.as_bytes()[..]).await?;
                to.write_u64(telemetry.span_id).await?;
                to.write_string(&telemetry.tracestate).await?;
                to.write_u8(telemetry.trace_flags).await?;
            } else {
                to.write_u8(0u8).await?;
            }
        }
        if revision >= protocol::DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
            to.write_var_uint(0).await?;
            to.write_var_uint(0).await?;
            to.write_var_uint(0).await?;
        }

        Ok(())
    }
}

pub struct OpenTelemetry {
    pub(crate) trace_id: Uuid,
    pub(crate) span_id: u64,
    pub(crate) tracestate: String,
    pub(crate) trace_flags: u8,
}

impl OpenTelemetry {
    /// Parse a W3C `traceparent` header (format: `00-{trace_id}-{span_id}-{trace_flags}`)
    /// and construct an `OpenTelemetry` context for the ClickHouse native protocol.
    pub fn from_traceparent(traceparent: &str, tracestate: &str) -> Result<Self> {
        let parts: Vec<&str> = traceparent.split('-').collect();
        if parts.len() != 4 {
            return Err(KlickhouseError::ProtocolError(format!(
                "invalid traceparent: expected 4 parts, got {}",
                parts.len()
            )));
        }
        if parts[0] != "00" {
            return Err(KlickhouseError::ProtocolError(format!(
                "unsupported traceparent version: {}",
                parts[0]
            )));
        }
        if parts[1].len() != 32 || parts[2].len() != 16 || parts[3].len() != 2 {
            return Err(KlickhouseError::ProtocolError(
                "invalid traceparent field lengths".into(),
            ));
        }

        let trace_id_128 = u128::from_str_radix(parts[1], 16).map_err(|e| {
            KlickhouseError::ProtocolError(format!("invalid traceparent trace_id: {e}"))
        })?;

        let trace_id_64_high = (trace_id_128 >> 64) as u64;
        let trace_id_64_low = trace_id_128 as u64;

        let trace_id =
            Uuid::from_u64_pair(trace_id_64_high.swap_bytes(), trace_id_64_low.swap_bytes());

        let span_id = u64::from_str_radix(parts[2], 16).map_err(|e| {
            KlickhouseError::ProtocolError(format!("invalid traceparent span_id: {e}"))
        })?;

        let trace_flags = u8::from_str_radix(parts[3], 16).map_err(|e| {
            KlickhouseError::ProtocolError(format!("invalid traceparent trace_flags: {e}"))
        })?;

        Ok(OpenTelemetry {
            trace_id,
            span_id,
            tracestate: tracestate.to_string(),
            trace_flags,
        })
    }
}

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub enum QueryProcessingStage {
    FetchColumns,
    WithMergeableState,
    Complete,
    WithMergableStateAfterAggregation,
}

pub struct Query<'a> {
    pub id: &'a str,
    pub info: ClientInfo<'a>,
    pub settings: &'a [(String, String)],
    pub parameters: &'a [(String, String)],
    //todo: interserver secret
    pub stage: QueryProcessingStage,
    pub compression: CompressionMethod,
    pub query: &'a str,
    //todo: data
}

impl<W: ClickhouseWrite> InternalClientOut<W> {
    pub fn new(writer: W) -> Self {
        InternalClientOut {
            writer,
            server_hello: ServerHello::default(),
        }
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn send_query<'a>(&mut self, params: Query<'a>) -> Result<()> {
        if !matches!(params.compression, CompressionMethod::None) {
            return Err(KlickhouseError::ProtocolError(
                "native compression is disabled".to_string(),
            ));
        }
        check_settings_revision(
            self.server_hello.revision_version,
            !params.settings.is_empty(),
        )?;
        if !params.parameters.is_empty()
            && self.server_hello.revision_version
                < protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
        {
            return Err(KlickhouseError::ProtocolError(
                "native parameters require protocol revision 54459".to_string(),
            ));
        }
        self.writer
            .write_var_uint(protocol::ClientPacketId::Query as u64)
            .await?;
        self.writer.write_string(params.id).await?;
        if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
            params
                .info
                .write(&mut self.writer, self.server_hello.revision_version)
                .await?;
        }
        if self.server_hello.revision_version
            >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        {
            for (key, value) in params.settings {
                self.writer.write_string(key.as_bytes()).await?;
                self.writer.write_u8(0u8).await?;
                self.writer.write_string(value.as_bytes()).await?;
            }
        }
        self.writer.write_string("").await?;
        if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
            //todo interserver secret
            self.writer.write_string("").await?;
        }
        self.writer.write_var_uint(params.stage as u64).await?;
        self.writer
            .write_u8(if matches!(params.compression, CompressionMethod::None) {
                0
            } else {
                1
            })
            .await?;
        self.writer.write_string(params.query).await?;
        if self.server_hello.revision_version >= protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
        {
            for (name, value) in params.parameters {
                self.writer.write_string(name).await?;
                self.writer.write_var_uint(2).await?; // BaseSettings::CUSTOM.
                self.writer.write_string(value).await?;
            }
            self.writer.write_string("").await?;
        }

        self.writer.flush().await?;
        Ok(())
    }

    #[cfg(feature = "compression")]
    async fn compress_data(&mut self, byte: u8, block: Block) -> Result<()> {
        let (out, decompressed_size) =
            crate::compression::compress_block(block, self.server_hello.revision_version).await?;
        let mut new_out = Vec::with_capacity(out.len() + 5);
        new_out.push(byte);
        new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
        new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
        new_out.extend(out);

        let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
        self.writer.write_u64_le((hash >> 64) as u64).await?;
        self.writer.write_u64_le(hash as u64).await?;
        // self.writer.write_u8(byte).await?;
        // self.writer.write_u32_le(out.len() as u32).await?;
        self.writer.write_all(&new_out[..]).await?;
        Ok(())
    }

    #[cfg(not(feature = "compression"))]
    async fn compress_data(&mut self, _byte: u8, _block: Block) -> Result<()> {
        Err(crate::KlickhouseError::ProtocolError(
            "compression support is disabled".to_string(),
        ))
    }

    pub async fn send_data(
        &mut self,
        block: Block,
        compression: CompressionMethod,
        name: &str,
        scalar: bool,
    ) -> Result<()> {
        if !matches!(compression, CompressionMethod::None) {
            return Err(KlickhouseError::ProtocolError(
                "native compression is disabled".to_string(),
            ));
        }
        if scalar {
            self.writer
                .write_var_uint(protocol::ClientPacketId::Scalar as u64)
                .await?;
        } else {
            self.writer
                .write_var_uint(protocol::ClientPacketId::Data as u64)
                .await?;
        }
        self.writer.write_string(name).await?;
        match compression {
            CompressionMethod::None => {
                block
                    .write(&mut self.writer, self.server_hello.revision_version)
                    .await?;
            }
            CompressionMethod::LZ4 => {
                self.compress_data(CompressionMethod::LZ4.byte(), block)
                    .await?;
            }
        }

        self.writer.flush().await?;

        Ok(())
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn send_hello<'a>(&mut self, params: ClientHello<'a>) -> Result<()> {
        self.writer
            .write_var_uint(protocol::ClientPacketId::Hello as u64)
            .await?;
        self.writer.write_string(CLIENT_NAME).await?;
        self.writer.write_var_uint(crate::VERSION_MAJOR).await?;
        self.writer.write_var_uint(crate::VERSION_MINOR).await?;
        self.writer
            .write_var_uint(protocol::DBMS_TCP_PROTOCOL_VERSION)
            .await?;
        self.writer.write_string(params.default_database).await?;
        self.writer.write_string(params.username).await?;
        self.writer.write_string(params.password).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn send_addendum(&mut self) -> Result<()> {
        if self.server_hello.revision_version >= protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
            self.writer.write_string("").await?;
            self.writer.flush().await?;
        }
        Ok(())
    }

    // pub async fn send_ping(&mut self) -> Result<()> {
    //     self.writer.write_var_uint(protocol::ClientPacketId::Ping as u64).await?;
    //     Ok(())
    // }
}

#[cfg(test)]
mod revision_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn info() -> ClientInfo<'static> {
        ClientInfo {
            kind: QueryKind::InitialQuery,
            initial_user: "",
            initial_query_id: "",
            initial_address: "",
            os_user: "",
            client_hostname: "",
            client_name: "",
            client_version_major: 1,
            client_version_minor: 2,
            client_tcp_protocol_version: 3,
            quota_key: "",
            distributed_depth: 0,
            client_version_patch: 0,
            open_telemetry: None,
        }
    }

    async fn encoded_info(revision: u64) -> Vec<u8> {
        let (mut reader, mut writer) = tokio::io::duplex(256);
        info().write(&mut writer, revision).await.unwrap();
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    #[tokio::test]
    async fn golden_client_info_revision_boundaries() {
        let old = vec![1, 0, 0, 0, 1, 0, 0, 0, 1, 2, 3, 0, 0, 0, 0];
        assert_eq!(encoded_info(54448).await, old);
        let mut time = old.clone();
        time.splice(4..4, [0; 8]);
        assert_eq!(encoded_info(54449).await, time);
        assert_eq!(encoded_info(54452).await, time);
        time.extend_from_slice(&[0, 0, 0]);
        assert_eq!(encoded_info(54453).await, time);
        assert_eq!(encoded_info(54459).await, time);
    }

    async fn encoded_query(revision: u64) -> Vec<u8> {
        let (mut reader, writer) = tokio::io::duplex(256);
        let mut output = InternalClientOut::new(writer);
        output.server_hello.revision_version = revision;
        let mut info = info();
        info.kind = QueryKind::NoQuery;
        output
            .send_query(Query {
                id: "",
                info,
                settings: &[],
                parameters: &[],
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::None,
                query: "X",
            })
            .await
            .unwrap();
        drop(output);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    #[tokio::test]
    async fn golden_empty_parameters_and_addendum_boundaries() {
        let before = vec![1, 0, 0, 0, 0, 2, 0, 1, b'X'];
        assert_eq!(encoded_query(54458).await, before);
        let mut after = before;
        after.push(0);
        assert_eq!(encoded_query(54459).await, after);
        for (revision, expected) in [(54457, vec![]), (54458, vec![0])] {
            let (mut reader, writer) = tokio::io::duplex(16);
            let mut output = InternalClientOut::new(writer);
            output.server_hello.revision_version = revision;
            output.send_addendum().await.unwrap();
            drop(output);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
        }
    }

    #[tokio::test]
    async fn old_revision_rejects_settings_before_writing_any_bytes() {
        let (mut reader, writer) = tokio::io::duplex(256);
        let mut output = InternalClientOut::new(writer);
        output.server_hello.revision_version = 54428;
        let mut info = info();
        info.kind = QueryKind::NoQuery;
        let settings = [("max_execution_time".to_string(), "1".to_string())];
        let result = output
            .send_query(Query {
                id: "",
                info,
                settings: &settings,
                parameters: &[],
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::None,
                query: "SELECT 1",
            })
            .await;
        assert!(
            result.is_err(),
            "must reject settings instead of silently ignoring them"
        );
        drop(output);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty(), "nothing may be dispatched");
    }

    #[tokio::test]
    async fn settings_revision_boundary_writes_exact_frame() {
        for revision in [54428, 54429] {
            let (mut reader, writer) = tokio::io::duplex(256);
            let mut output = InternalClientOut::new(writer);
            output.server_hello.revision_version = revision;
            let mut info = info();
            info.kind = QueryKind::NoQuery;
            let settings = [("foo".to_string(), "1".to_string())];
            output
                .send_query(Query {
                    id: "",
                    info,
                    settings: if revision == 54428 { &[] } else { &settings },
                    parameters: &[],
                    stage: QueryProcessingStage::Complete,
                    compression: CompressionMethod::None,
                    query: "X",
                })
                .await
                .unwrap();
            drop(output);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            let expected = if revision == 54428 {
                vec![1, 0, 0, 0, 2, 0, 1, b'X']
            } else {
                vec![1, 0, 0, 3, b'f', b'o', b'o', 0, 1, b'1', 0, 2, 0, 1, b'X']
            };
            assert_eq!(bytes, expected);
        }
    }

    #[tokio::test]
    async fn golden_native_parameter_custom_field_dump() {
        let (mut reader, writer) = tokio::io::duplex(256);
        let mut output = InternalClientOut::new(writer);
        output.server_hello.revision_version = 54459;
        let mut info = info();
        info.kind = QueryKind::NoQuery;
        let parameters = [("ntp_a".to_string(), "'-1'".to_string())];
        output
            .send_query(Query {
                id: "",
                info,
                settings: &[],
                parameters: &parameters,
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::None,
                query: "X",
            })
            .await
            .unwrap();
        drop(output);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        // ClickHouse 24.8 BaseSettings STRINGS_WITH_FLAGS: name, CUSTOM=2,
        // Field string dump, then an empty-name terminator.
        assert_eq!(
            bytes,
            vec![
                1, 0, 0, 0, 0, 2, 0, 1, b'X', 5, b'n', b't', b'p', b'_', b'a', 2, 4, b'\'', b'-',
                b'1', b'\'', 0,
            ]
        );
    }

    #[tokio::test]
    async fn golden_nullable_parameter_field_dump() {
        let (mut reader, writer) = tokio::io::duplex(256);
        let mut output = InternalClientOut::new(writer);
        output.server_hello.revision_version = 54459;
        let mut info = info();
        info.kind = QueryKind::NoQuery;
        let parameters = crate::client::encode_native_parameters(&[(
            "p1",
            crate::client::NativeParameter::NullableStringNull,
        )])
        .unwrap();
        output
            .send_query(Query {
                id: "",
                info,
                settings: &[],
                parameters: &parameters,
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::None,
                query: "X",
            })
            .await
            .unwrap();
        drop(output);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(
            bytes,
            vec![
                1, 0, 0, 0, 0, 2, 0, 1, b'X', 2, b'p', b'1', 2, 5, b'\'', b'\\', b'\\', b'N',
                b'\'', 0
            ]
        );
    }

    #[tokio::test]
    async fn golden_bool_and_float_native_parameter_fields() {
        let (mut reader, writer) = tokio::io::duplex(256);
        let mut output = InternalClientOut::new(writer);
        output.server_hello.revision_version = 54459;
        let mut info = info();
        info.kind = QueryKind::NoQuery;
        let parameters = [
            ("ntp_b".to_string(), "'1'".to_string()),
            ("ntp_f".to_string(), "'-0'".to_string()),
        ];
        output
            .send_query(Query {
                id: "",
                info,
                settings: &[],
                parameters: &parameters,
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::None,
                query: "X",
            })
            .await
            .unwrap();
        drop(output);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(
            bytes,
            vec![
                1, 0, 0, 0, 0, 2, 0, 1, b'X', 5, b'n', b't', b'p', b'_', b'b', 2, 3, b'\'', b'1',
                b'\'', 5, b'n', b't', b'p', b'_', b'f', 2, 4, b'\'', b'-', b'0', b'\'', 0,
            ]
        );
    }

    #[tokio::test]
    async fn golden_binary_parameter_double_escaping() {
        let (mut reader, writer) = tokio::io::duplex(256);
        let mut output = InternalClientOut::new(writer);
        output.server_hello.revision_version = 54459;
        let mut info = info();
        info.kind = QueryKind::NoQuery;
        let parameters = [("ntp_s".to_string(), r"'\\x00\\x27\\x41'".to_string())];
        output
            .send_query(Query {
                id: "",
                info,
                settings: &[],
                parameters: &parameters,
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::None,
                query: "X",
            })
            .await
            .unwrap();
        drop(output);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        let mut expected = vec![
            1, 0, 0, 0, 0, 2, 0, 1, b'X', 5, b'n', b't', b'p', b'_', b's', 2, 17,
        ];
        expected.extend_from_slice(br"'\\x00\\x27\\x41'");
        expected.push(0);
        assert_eq!(bytes, expected);
    }

    #[tokio::test]
    async fn compressed_query_is_rejected_before_writing() {
        let (mut reader, writer) = tokio::io::duplex(128);
        let mut output = InternalClientOut::new(writer);
        output.server_hello.revision_version = 54459;
        let result = output
            .send_query(Query {
                id: "",
                info: info(),
                settings: &[],
                parameters: &[],
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::LZ4,
                query: "SELECT 1",
            })
            .await;
        assert!(matches!(result, Err(KlickhouseError::ProtocolError(_))));
        drop(output);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes.len(), 0);
    }
}
