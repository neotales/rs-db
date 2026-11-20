use std::future::Future;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{KlickhouseError, Result};

use crate::protocol::MAX_STRING_SIZE;

pub trait ClickhouseRead: AsyncRead + Unpin + Send + Sync {
    fn read_var_uint(&mut self) -> impl Future<Output = Result<u64>> + Send;

    fn read_string(&mut self) -> impl Future<Output = Result<Vec<u8>>> + Send;

    fn read_limited_string(&mut self, limit: usize)
    -> impl Future<Output = Result<Vec<u8>>> + Send;

    fn read_utf8_string(&mut self) -> impl Future<Output = Result<String>> + Send {
        async { Ok(String::from_utf8(self.read_string().await?)?) }
    }
}

impl<T: AsyncRead + Unpin + Send + Sync> ClickhouseRead for T {
    async fn read_var_uint(&mut self) -> Result<u64> {
        let mut out = 0u64;
        for i in 0..10u64 {
            let mut octet = [0u8];
            self.read_exact(&mut octet[..]).await?;
            if i == 9 && octet[0] > 1 {
                return Err(KlickhouseError::ProtocolError(
                    "varint overflow".to_string(),
                ));
            }
            out |= ((octet[0] & 0x7F) as u64) << (7 * i);
            if (octet[0] & 0x80) == 0 {
                if i > 0 && octet[0] == 0 {
                    return Err(KlickhouseError::ProtocolError(
                        "noncanonical varint".to_string(),
                    ));
                }
                return Ok(out);
            }
        }
        Err(KlickhouseError::ProtocolError(
            "unterminated varint".to_string(),
        ))
    }

    async fn read_string(&mut self) -> Result<Vec<u8>> {
        self.read_limited_string(MAX_STRING_SIZE).await
    }

    async fn read_limited_string(&mut self, limit: usize) -> Result<Vec<u8>> {
        let len = self.read_var_uint().await?;
        let cap = limit.min(MAX_STRING_SIZE);
        if len > cap as u64 {
            return Err(KlickhouseError::ProtocolError(format!(
                "string too large: {len} > {cap}"
            )));
        }
        let mut buf = vec![0; len as usize];
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

pub trait ClickhouseWrite: AsyncWrite + Unpin + Send + Sync + 'static {
    fn write_var_uint(&mut self, value: u64) -> impl Future<Output = Result<()>> + Send;

    fn write_string(
        &mut self,
        value: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl<T: AsyncWrite + Unpin + Send + Sync + 'static> ClickhouseWrite for T {
    async fn write_var_uint(&mut self, mut value: u64) -> Result<()> {
        for _ in 0..10u64 {
            let mut byte = value & 0x7F;
            if value > 0x7F {
                byte |= 0x80;
            }
            self.write_all(&[byte as u8]).await?;
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn write_string(&mut self, value: impl AsRef<[u8]> + Send) -> Result<()> {
        let value = value.as_ref();
        self.write_var_uint(value.len() as u64).await?;
        self.write_all(value).await?;
        Ok(())
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn malformed_varints_and_oversized_string_fail_before_body() {
        for bytes in [
            vec![0x80; 10],
            vec![0x80, 0x00],
            vec![0xff; 9].into_iter().chain([0x02]).collect(),
        ] {
            let (mut input, mut output) = tokio::io::duplex(128);
            output.write_all(&bytes).await.unwrap();
            drop(output);
            assert!(matches!(
                input.read_var_uint().await,
                Err(KlickhouseError::ProtocolError(_))
            ));
        }
        let (mut input, mut output) = tokio::io::duplex(128);
        output.write_var_uint(65537).await.unwrap();
        drop(output);
        assert!(matches!(
            input.read_string().await,
            Err(KlickhouseError::ProtocolError(_))
        ));
        let (mut input, mut output) = tokio::io::duplex(128);
        output.write_var_uint(u64::MAX).await.unwrap();
        drop(output);
        assert_eq!(input.read_var_uint().await.unwrap(), u64::MAX);
        let (mut input, mut output) = tokio::io::duplex(128);
        output.write_all(&[0x80]).await.unwrap();
        drop(output);
        assert!(matches!(
            input.read_var_uint().await,
            Err(KlickhouseError::Io(_))
        ));
    }
}
