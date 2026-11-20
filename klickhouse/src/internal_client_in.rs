use crate::Result;
use crate::{
    KlickhouseError,
    block::Block,
    io::ClickhouseRead,
    progress::Progress,
    protocol::{
        self, BlockStreamProfileInfo, CompressionMethod, DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO,
        DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME, DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
        DBMS_MIN_REVISION_WITH_VERSION_PATCH, ServerData, ServerException, ServerHello,
        ServerPacket, TableColumns,
    },
};
use log::trace;
use protocol::ServerPacketId;
use tokio::io::AsyncReadExt;

#[cfg(feature = "compression")]
#[allow(dead_code)]
pub(crate) const MAX_COMPRESSION_SIZE: u32 = 0x40000000;

pub struct InternalClientIn<R: ClickhouseRead> {
    reader: R,
    pub server_hello: ServerHello,
}

impl<R: ClickhouseRead + 'static> InternalClientIn<R> {
    pub fn new(reader: R) -> Self {
        InternalClientIn {
            reader,
            server_hello: ServerHello::default(),
        }
    }

    async fn read_exception(&mut self) -> Result<ServerException> {
        let code = self.reader.read_i32_le().await?;
        let name = self.reader.read_utf8_string().await?;
        let message = self.reader.read_utf8_string().await?;
        let stack_trace = self.reader.read_utf8_string().await?;
        let has_nested = self.reader.read_u8().await?;
        if has_nested != 0 {
            // The nested payload remains unread. Never treat its bytes as another packet or
            // permit connection reuse; the controller closes on this protocol error.
            return Err(KlickhouseError::ProtocolError(
                "nested server exceptions are unsupported".to_string(),
            ));
        }

        Ok(ServerException {
            code,
            name,
            message,
            stack_trace,
            has_nested: false,
        })
    }

    async fn receive_data(&mut self, compression: CompressionMethod) -> Result<ServerData> {
        if !matches!(compression, CompressionMethod::None) {
            return Err(KlickhouseError::ProtocolError(
                "native compression is disabled".to_string(),
            ));
        }
        let table_name = self.reader.read_utf8_string().await?;

        let block = Block::read(&mut self.reader, self.server_hello.revision_version).await?;

        Ok(ServerData { table_name, block })
    }

    async fn receive_log_data(&mut self) -> Result<ServerData> {
        Err(KlickhouseError::ProtocolError(
            "server log packets are unsupported".to_string(),
        ))
    }

    pub async fn receive_packet(&mut self) -> Result<ServerPacket> {
        let packet_id = ServerPacketId::from_u64(self.reader.read_var_uint().await?)?;
        let packet: Result<ServerPacket> = match packet_id {
            ServerPacketId::Hello => {
                let server_name = self.reader.read_utf8_string().await?;
                let major_version = self.reader.read_var_uint().await?;
                let minor_version = self.reader.read_var_uint().await?;
                let revision_version = self
                    .reader
                    .read_var_uint()
                    .await?
                    .min(protocol::DBMS_TCP_PROTOCOL_VERSION);
                let timezone = if revision_version >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
                    Some(self.reader.read_utf8_string().await?)
                } else {
                    None
                };
                let display_name = if revision_version >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
                {
                    Some(self.reader.read_utf8_string().await?)
                } else {
                    None
                };
                let patch_version = if revision_version >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
                    self.reader.read_var_uint().await?
                } else {
                    revision_version
                };
                Ok(ServerPacket::Hello(ServerHello {
                    server_name,
                    major_version,
                    minor_version,
                    revision_version,
                    timezone,
                    display_name,
                    patch_version,
                }))
            }
            ServerPacketId::Data => Ok(ServerPacket::Data(
                self.receive_data(CompressionMethod::None).await?,
            )),
            ServerPacketId::Exception => Ok(ServerPacket::Exception(self.read_exception().await?)),
            ServerPacketId::Progress => {
                let read_rows = self.reader.read_var_uint().await?;
                let read_bytes = self.reader.read_var_uint().await?;
                let new_total_rows_to_read = self.reader.read_var_uint().await?;
                let new_written_rows = if self.server_hello.revision_version
                    >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
                {
                    Some(self.reader.read_var_uint().await?)
                } else {
                    None
                };
                let new_written_bytes = if self.server_hello.revision_version
                    >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
                {
                    Some(self.reader.read_var_uint().await?)
                } else {
                    None
                };
                Ok(ServerPacket::Progress(Progress {
                    read_rows,
                    read_bytes,
                    new_total_rows_to_read,
                    new_written_rows,
                    new_written_bytes,
                }))
            }
            ServerPacketId::Pong => Ok(ServerPacket::Pong),
            ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
            ServerPacketId::ProfileInfo => {
                let rows = self.reader.read_var_uint().await?;
                let blocks = self.reader.read_var_uint().await?;
                let bytes = self.reader.read_var_uint().await?;
                let applied_limit = self.reader.read_u8().await? != 0;
                let rows_before_limit = self.reader.read_var_uint().await?;
                let calculated_rows_before_limit = self.reader.read_u8().await? != 0;
                Ok(ServerPacket::ProfileInfo(BlockStreamProfileInfo {
                    rows,
                    blocks,
                    bytes,
                    applied_limit,
                    rows_before_limit,
                    calculated_rows_before_limit,
                }))
            }
            ServerPacketId::Totals => Ok(ServerPacket::Totals(
                self.receive_data(CompressionMethod::None).await?,
            )),
            ServerPacketId::Extremes => Ok(ServerPacket::Extremes(
                self.receive_data(CompressionMethod::None).await?,
            )),
            ServerPacketId::TablesStatusResponse => Err(KlickhouseError::ProtocolError(
                "table status packets are unsupported".to_string(),
            )),
            ServerPacketId::Log => Ok(ServerPacket::Log(self.receive_log_data().await?)),
            ServerPacketId::TableColumns => {
                let name = self.reader.read_utf8_string().await?;
                let description = self.reader.read_utf8_string().await?;
                Ok(ServerPacket::TableColumns(TableColumns {
                    name,
                    description,
                }))
            }
            ServerPacketId::PartUUIDs => Err(KlickhouseError::ProtocolError(
                "part UUID packets are unsupported".to_string(),
            )),
            ServerPacketId::ReadTaskRequest => Err(KlickhouseError::ProtocolError(
                "server read-task packets are unsupported".to_string(),
            )),
            ServerPacketId::ProfileEvents => {
                if self.server_hello.revision_version
                    < protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS
                {
                    return Err(KlickhouseError::ProtocolError(
                        "unexpected profile events packet".to_string(),
                    ));
                }
                Ok(ServerPacket::ProfileEvents(
                    self.receive_data(CompressionMethod::None).await?,
                ))
            }
        };
        let packet = packet?;

        trace!("clickhouse packet received: {packet:?}");
        Ok(packet)
    }

    pub async fn receive_hello(&mut self) -> Result<ServerHello> {
        match self.receive_packet().await? {
            ServerPacket::Hello(hello) => Ok(hello),
            ServerPacket::Exception(e) => Err(e.emit()),
            packet => Err(KlickhouseError::ProtocolError(format!(
                "unexpected packet {packet:?}, expected server hello"
            ))),
        }
    }
}

#[cfg(test)]
mod revision_tests {
    use super::*;
    use crate::io::ClickhouseWrite;

    #[tokio::test]
    async fn hello_boundaries_and_negotiation_preserve_packet_alignment() {
        for reported in [54057, 54058, 54371, 54372, 54400, 54401, 54459, 54469] {
            let negotiated = reported.min(protocol::DBMS_TCP_PROTOCOL_VERSION);
            let (reader, mut writer) = tokio::io::duplex(256);
            writer.write_var_uint(0).await.unwrap();
            writer.write_string("test").await.unwrap();
            writer.write_var_uint(1).await.unwrap();
            writer.write_var_uint(2).await.unwrap();
            writer.write_var_uint(reported).await.unwrap();
            if negotiated >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
                writer.write_string("UTC").await.unwrap();
            }
            if negotiated >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
                writer.write_string("display").await.unwrap();
            }
            if negotiated >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
                writer.write_var_uint(9).await.unwrap();
            }
            writer.write_var_uint(5).await.unwrap();
            drop(writer);
            let mut input = InternalClientIn::new(reader);
            let hello = input.receive_hello().await.unwrap();
            assert_eq!(hello.revision_version, negotiated);
            assert_eq!(
                hello.timezone.is_some(),
                negotiated >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
            );
            assert_eq!(
                hello.display_name.is_some(),
                negotiated >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
            );
            assert_eq!(
                hello.patch_version,
                if negotiated >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
                    9
                } else {
                    negotiated
                }
            );
            assert!(matches!(
                input.receive_packet().await.unwrap(),
                ServerPacket::EndOfStream
            ));
        }
    }

    #[tokio::test]
    async fn profile_events_boundary_and_packet_alignment() {
        for revision in [54450, 54451, 54456, 54459] {
            let (reader, mut writer) = tokio::io::duplex(256);
            writer.write_var_uint(14).await.unwrap();
            writer.write_string("").await.unwrap();
            writer.write_var_uint(0).await.unwrap(); // BlockInfo end.
            writer.write_var_uint(0).await.unwrap(); // columns
            writer.write_var_uint(0).await.unwrap(); // rows
            writer.write_var_uint(5).await.unwrap();
            drop(writer);
            let mut input = InternalClientIn::new(reader);
            input.server_hello.revision_version = revision;
            if revision < 54451 {
                assert!(input.receive_packet().await.is_err());
            } else {
                assert!(matches!(
                    input.receive_packet().await.unwrap(),
                    ServerPacket::ProfileEvents(_)
                ));
                assert!(matches!(
                    input.receive_packet().await.unwrap(),
                    ServerPacket::EndOfStream
                ));
            }
        }
    }

    #[tokio::test]
    async fn compressed_data_is_rejected_before_reading() {
        let (reader, _writer) = tokio::io::duplex(16);
        let mut input = InternalClientIn::new(reader);
        assert!(matches!(
            input.receive_data(CompressionMethod::LZ4).await,
            Err(KlickhouseError::ProtocolError(_))
        ));
    }

    #[tokio::test]
    async fn unsupported_packets_are_rejected_before_body_allocation() {
        for id in [9, 10, 12, 13] {
            let (reader, mut writer) = tokio::io::duplex(16);
            writer.write_var_uint(id).await.unwrap();
            drop(writer);
            let mut input = InternalClientIn::new(reader);
            assert!(
                matches!(
                    input.receive_packet().await,
                    Err(KlickhouseError::ProtocolError(_))
                ),
                "packet {id}"
            );
        }
    }
}
