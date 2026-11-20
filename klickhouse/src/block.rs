use std::{collections::VecDeque, str::FromStr};

use crate::Result;
use indexmap::IndexMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    KlickhouseError,
    io::{ClickhouseRead, ClickhouseWrite},
    types::{DeserializerState, SerializerState, Type},
    values::Value,
};

/// Metadata about a block
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub is_overflows: bool,
    pub bucket_num: i32,
}

impl Default for BlockInfo {
    fn default() -> Self {
        BlockInfo {
            is_overflows: false,
            bucket_num: -1,
        }
    }
}

impl BlockInfo {
    async fn read<R: ClickhouseRead>(reader: &mut R) -> Result<Self> {
        let mut new = Self::default();
        loop {
            let field_num = reader.read_var_uint().await?;
            match field_num {
                0 => break,
                1 => {
                    new.is_overflows = reader.read_u8().await? != 0;
                }
                2 => {
                    new.bucket_num = reader.read_i32_le().await?;
                }
                field_num => {
                    return Err(KlickhouseError::ProtocolError(format!(
                        "unknown block info field number: {field_num}"
                    )));
                }
            }
        }
        Ok(new)
    }

    async fn write<W: ClickhouseWrite>(&self, writer: &mut W) -> Result<()> {
        writer.write_var_uint(1).await?;
        writer
            .write_u8(if self.is_overflows { 1 } else { 2 })
            .await?;
        writer.write_var_uint(2).await?;
        writer.write_i32_le(self.bucket_num).await?;
        writer.write_var_uint(0).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// A chunk of data in columnar form.
pub struct Block {
    /// Metadata about the block
    pub info: BlockInfo,
    /// The number of rows contained in the block
    pub rows: u64,
    /// The type of each column by name, in order.
    pub column_types: IndexMap<String, Type>,
    /// The data of each column by name, in order. All `Value` should correspond to the associated type in `column_types`.
    pub column_data: IndexMap<String, Vec<Value>>,
}

/// Iterator type for `iter_rows`
pub struct BlockRowIter<'a> {
    block: &'a Block,
    row: u64,
}

impl<'a> Iterator for BlockRowIter<'a> {
    type Item = Vec<(&'a str, &'a Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.block.rows {
            return None;
        }
        let mut out = Vec::new();
        for (name, value) in self.block.column_data.iter() {
            out.push((&**name, value.get(self.row as usize)?));
        }
        self.row += 1;
        Some(out)
    }
}

// Iterator type for `take_iter_rows`
pub struct BlockRowValueIter<'a> {
    column_data: Vec<(&'a str, &'a Type, std::vec::IntoIter<Value>)>,
}

impl<'a> Iterator for BlockRowValueIter<'a> {
    type Item = Vec<(&'a str, &'a Type, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.column_data.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        for (name, type_, pop) in self.column_data.iter_mut() {
            out.push((*name, *type_, pop.next()?));
        }
        Some(out)
    }
}

/// Iterator type for `into_iter_rows`
pub struct BlockRowIntoIter {
    column_data: IndexMap<String, VecDeque<(Type, Value)>>,
}

impl Iterator for BlockRowIntoIter {
    type Item = IndexMap<String, (Type, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out = IndexMap::new();
        if self.column_data.is_empty() {
            return None;
        }
        for (name, value) in self.column_data.iter_mut() {
            out.insert(name.clone(), value.pop_front()?);
        }
        Some(out)
    }
}

impl Block {
    /// Create a borrowing iterator for all rows
    pub fn iter_rows(&self) -> BlockRowIter<'_> {
        BlockRowIter {
            block: self,
            row: 0,
        }
    }

    /// Iterate over all rows with owned values.
    pub fn take_iter_rows(&mut self) -> BlockRowValueIter<'_> {
        let mut column_data = IndexMap::new();
        std::mem::swap(&mut self.column_data, &mut column_data);
        let mut out = Vec::with_capacity(self.rows as usize);
        for (name, values) in column_data.into_iter() {
            let (name, type_) = self.column_types.get_key_value(&name).unwrap();
            out.push((&**name, type_.strip_low_cardinality(), values.into_iter()));
        }
        BlockRowValueIter { column_data: out }
    }

    /// Iterate over all rows with owned value, types, and names.
    pub fn into_iter_rows(self) -> BlockRowIntoIter {
        let column_types = self.column_types;
        BlockRowIntoIter {
            column_data: self
                .column_data
                .into_iter()
                .map(|(name, values)| {
                    let type_ = column_types.get(&name).unwrap();
                    (
                        name,
                        values.into_iter().map(|x| (type_.clone(), x)).collect(),
                    )
                })
                .collect(),
        }
    }

    pub async fn read<R: ClickhouseRead>(reader: &mut R, revision: u64) -> Result<Self> {
        let info = if revision > 0 {
            BlockInfo::read(reader).await?
        } else {
            Default::default()
        };
        let columns = reader.read_var_uint().await?;
        let rows = reader.read_var_uint().await?;
        const MAX_COLUMNS: u64 = 64;
        const MAX_ROWS: u64 = 4096;
        const MAX_CELLS: u64 = 16_384;
        const BLOCK_BUDGET: usize = 2 * 1024 * 1024;
        if columns > MAX_COLUMNS
            || rows > MAX_ROWS
            || columns
                .checked_mul(rows)
                .is_none_or(|cells| cells > MAX_CELLS)
        {
            return Err(KlickhouseError::ProtocolError(
                "native block dimensions exceed budget".to_string(),
            ));
        }
        let cells = usize::try_from(columns * rows).map_err(|_| {
            KlickhouseError::ProtocolError("native cell count overflow".to_string())
        })?;
        let charged = cells
            .checked_mul(std::mem::size_of::<Value>())
            .and_then(|n| n.checked_add(columns as usize * 1024))
            .ok_or_else(|| {
                KlickhouseError::ProtocolError("native block budget overflow".to_string())
            })?;
        let mut remaining = BLOCK_BUDGET.checked_sub(charged).ok_or_else(|| {
            KlickhouseError::ProtocolError("native block budget exceeded".to_string())
        })?;
        let mut block = Block {
            info,
            rows,
            column_types: IndexMap::new(),
            column_data: IndexMap::new(),
        };
        for _ in 0..columns {
            let name = String::from_utf8(reader.read_limited_string(256).await?)?;
            let type_name = String::from_utf8(reader.read_limited_string(64).await?)?;
            if revision >= crate::protocol::DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION
                && reader.read_u8().await? != 0
            {
                return Err(KlickhouseError::ProtocolError(
                    "custom serialization is unsupported".to_string(),
                ));
            }
            if !matches!(
                type_name.as_str(),
                "String"
                    | "Bool"
                    | "Int8"
                    | "Int16"
                    | "Int32"
                    | "Int64"
                    | "UInt8"
                    | "UInt16"
                    | "UInt32"
                    | "UInt64"
                    | "Nullable(Int64)"
                    | "Nullable(UInt64)"
                    | "Nullable(String)"
                    | "Float32"
                    | "Float64"
                    | "DateTime"
                    | "Enum8('increment' = 1, 'gauge' = 2)"
            ) {
                return Err(KlickhouseError::ProtocolError(
                    "unsupported native column type".to_string(),
                ));
            }
            let type_ = Type::from_str(&type_name)?;
            if block.column_types.contains_key(&name) {
                return Err(KlickhouseError::ProtocolError(
                    "duplicate native column name".to_string(),
                ));
            }
            block.column_types.insert(name.clone(), type_.clone());
            let mut state = DeserializerState {};
            let row_data = if rows > 0
                && matches!(&type_, Type::Nullable(inner) if matches!(inner.as_ref(), Type::String | Type::Int64 | Type::UInt64))
            {
                remaining = remaining.checked_sub(rows as usize).ok_or_else(|| {
                    KlickhouseError::ProtocolError("nullable map exceeds block budget".to_string())
                })?;
                let mut mask = vec![0u8; rows as usize];
                reader.read_exact(&mut mask).await?;
                if mask.iter().any(|bit| *bit > 1) {
                    return Err(KlickhouseError::ProtocolError(
                        "invalid nullable map".to_string(),
                    ));
                }
                let mut values = Vec::with_capacity(rows as usize);
                let is_string = matches!(&type_, Type::Nullable(inner) if matches!(inner.as_ref(), Type::String));
                let is_unsigned = matches!(&type_, Type::Nullable(inner) if matches!(inner.as_ref(), Type::UInt64));
                for bit in mask {
                    // Nested data exists even for null entries and still consumes budget.
                    let value = if is_string {
                        let value = reader.read_limited_string(remaining).await?;
                        remaining -= value.len();
                        Value::String(value)
                    } else if is_unsigned {
                        Value::UInt64(reader.read_u64_le().await?)
                    } else {
                        Value::Int64(reader.read_i64_le().await?)
                    };
                    values.push(if bit == 1 { Value::Null } else { value });
                }
                values
            } else if rows > 0 && matches!(type_, Type::String) {
                let mut values = Vec::with_capacity(rows as usize);
                for _ in 0..rows {
                    let value = reader.read_limited_string(remaining).await?;
                    remaining -= value.len();
                    values.push(Value::String(value));
                }
                values
            } else if rows > 0 {
                type_.deserialize_prefix(reader, &mut state).await?;
                type_
                    .deserialize_column(reader, rows as usize, &mut state)
                    .await?
            } else {
                vec![]
            };
            block.column_data.insert(name, row_data);
        }

        Ok(block)
    }

    pub async fn write<W: ClickhouseWrite>(mut self, writer: &mut W, revision: u64) -> Result<()> {
        if revision > 0 {
            self.info.write(writer).await?;
        }
        let joined = self
            .column_types
            .into_iter()
            .flat_map(|(key, type_)| {
                let values = self.column_data.swap_remove(&key)?;
                Some((key, (type_, values)))
            })
            .collect::<Vec<_>>();
        writer.write_var_uint(joined.len() as u64).await?;
        writer.write_var_uint(self.rows).await?;
        for (name, (type_, data)) in joined {
            writer.write_string(&name).await?;
            writer.write_string(&type_.to_string()).await?;
            if revision >= crate::protocol::DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION {
                writer.write_u8(0).await?;
            }
            if data.len() != self.rows as usize {
                return Err(KlickhouseError::ProtocolError(format!(
                    "row and column length mismatch. {} != {}",
                    data.len(),
                    self.rows
                )));
            }
            if self.rows > 0 {
                let mut state = SerializerState {};
                type_.serialize_prefix(writer, &mut state).await?;
                type_.serialize_column(data, writer, &mut state).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod revision_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn nullable_null_map_and_nested_payload_are_bounded() {
        for (null_bit, length) in [(2u8, 0u64), (1, 65537)] {
            let (mut reader, mut writer) = tokio::io::duplex(256);
            writer.write_var_uint(0).await.unwrap();
            writer.write_var_uint(1).await.unwrap();
            writer.write_var_uint(1).await.unwrap();
            writer.write_string("x").await.unwrap();
            writer.write_string("Nullable(String)").await.unwrap();
            writer.write_u8(0).await.unwrap();
            writer.write_u8(null_bit).await.unwrap();
            writer.write_var_uint(length).await.unwrap();
            drop(writer);
            assert!(Block::read(&mut reader, 54459).await.is_err());
        }
    }

    async fn encoded_block(revision: u64) -> Vec<u8> {
        let (mut reader, mut writer) = tokio::io::duplex(256);
        let mut column_types = IndexMap::new();
        column_types.insert("x".to_string(), Type::UInt8);
        let mut column_data = IndexMap::new();
        column_data.insert("x".to_string(), vec![]);
        Block {
            info: BlockInfo::default(),
            rows: 0,
            column_types,
            column_data,
        }
        .write(&mut writer, revision)
        .await
        .unwrap();
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    #[tokio::test]
    async fn golden_custom_serialization_marker_boundary_and_rejection() {
        let old = encoded_block(54453).await;
        let mut new = old.clone();
        new.push(0);
        assert_eq!(encoded_block(54454).await, new);
        *new.last_mut().unwrap() = 1;
        let (mut reader, mut writer) = tokio::io::duplex(256);
        writer.write_all(&new).await.unwrap();
        drop(writer);
        assert!(matches!(
            Block::read(&mut reader, 54454).await,
            Err(KlickhouseError::ProtocolError(_))
        ));
    }

    #[tokio::test]
    async fn block_dimensions_and_nested_types_are_rejected() {
        let (mut reader, mut writer) = tokio::io::duplex(256);
        writer.write_var_uint(0).await.unwrap(); // BlockInfo end
        writer.write_var_uint(0).await.unwrap(); // columns
        writer.write_var_uint(4097).await.unwrap();
        drop(writer);
        assert!(matches!(
            Block::read(&mut reader, 54459).await,
            Err(KlickhouseError::ProtocolError(_))
        ));

        let (mut reader, mut writer) = tokio::io::duplex(256);
        writer.write_var_uint(0).await.unwrap();
        writer.write_var_uint(1).await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("x").await.unwrap();
        writer.write_string("Array(UInt8)").await.unwrap();
        writer.write_u8(0).await.unwrap();
        drop(writer);
        assert!(matches!(
            Block::read(&mut reader, 54459).await,
            Err(KlickhouseError::ProtocolError(_))
        ));
    }

    #[tokio::test]
    async fn string_payload_budget_and_truncation_are_rejected() {
        let (mut reader, mut writer) = tokio::io::duplex(65536);
        let send = tokio::spawn(async move {
            let result: Result<()> = async {
                writer.write_var_uint(0).await?;
                writer.write_var_uint(1).await?;
                writer.write_var_uint(33).await?;
                writer.write_string("x").await?;
                writer.write_string("String").await?;
                writer.write_u8(0).await?;
                let value = vec![b'x'; 65536];
                for _ in 0..33 {
                    writer.write_string(&value).await?;
                }
                Ok(())
            }
            .await;
            result
        });
        assert!(matches!(
            Block::read(&mut reader, 54459).await,
            Err(KlickhouseError::ProtocolError(_))
        ));
        drop(reader);
        let _ = send.await;

        let (mut reader, mut writer) = tokio::io::duplex(256);
        writer.write_var_uint(0).await.unwrap();
        writer.write_var_uint(1).await.unwrap();
        writer.write_var_uint(1).await.unwrap();
        writer.write_string("x").await.unwrap();
        writer.write_string("Int64").await.unwrap();
        writer.write_u8(0).await.unwrap();
        writer.write_all(&[1, 2, 3]).await.unwrap();
        drop(writer);
        assert!(matches!(
            Block::read(&mut reader, 54459).await,
            Err(KlickhouseError::Io(_))
        ));
    }

    #[tokio::test]
    async fn excessive_columns_and_cells_fail_before_column_data() {
        for (columns, rows) in [(65, 0), (64, 257)] {
            let (mut reader, mut writer) = tokio::io::duplex(128);
            writer.write_var_uint(0).await.unwrap();
            writer.write_var_uint(columns).await.unwrap();
            writer.write_var_uint(rows).await.unwrap();
            drop(writer);
            assert!(matches!(
                Block::read(&mut reader, 54459).await,
                Err(KlickhouseError::ProtocolError(_))
            ));
        }
    }
}
