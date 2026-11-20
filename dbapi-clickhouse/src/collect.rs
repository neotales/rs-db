//! Bounded collection of native ClickHouse result events.
//!
//! Native data blocks are provisional until the explicit `EndOfStream` packet. A local
//! overflow or decode failure is remembered while the stream is drained through that packet so
//! the connection is not poisoned for an otherwise complete query. Server or transport
//! failures win over a local failure because a fetch statement can have side effects.

use futures_util::{Stream, StreamExt};
use klickhouse::{QueryEvent, Type, Value as NativeValue};
use neotales_dbapi::{DbError, DbType, ErrorKind, Row, RowLimit, Value};

fn supported_type(ty: &Type) -> bool {
    match ty {
        Type::Int64 | Type::String | Type::UInt64 => true,
        Type::Nullable(inner) => {
            matches!(inner.as_ref(), Type::Int64 | Type::UInt64 | Type::String)
        }
        _ => false,
    }
}

/// The outcome of draining one native event stream.
#[derive(Debug)]
pub(crate) struct Collected {
    /// Rows, or the first local or server failure.
    pub(crate) rows: Result<Vec<Row>, DbError>,
    /// Whether the connection may be reused after this drain.
    ///
    /// This is true only for an explicit `EndOfStream` or a known pre-dispatch failure. A
    /// transport error, a missing terminal packet, or an abandoned stream leaves the
    /// connection poisoned.
    pub(crate) connection_clean: bool,
}

fn unknown() -> DbError {
    DbError::new(ErrorKind::WriteOutcomeUnknown)
}

/// Drains a native event stream to `EndOfStream`, enforcing a hard row and byte limit.
///
/// The stream is always drained after a local failure so the explicit terminal packet can still
/// be observed. No rows are returned when any failure occurred.
#[allow(clippy::too_many_lines)] // One ordered drain keeps provisional-data rules auditable.
pub(crate) async fn collect<S>(mut events: S, limit: RowLimit) -> Collected
where
    S: Stream<Item = klickhouse::Result<QueryEvent>> + Unpin,
{
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut failure: Option<ErrorKind> = None;
    let mut schema: Option<Vec<(String, Type)>> = None;
    loop {
        match events.next().await {
            Some(Ok(QueryEvent::Data(mut block))) => {
                if failure.is_some() {
                    continue;
                }
                let count = usize::try_from(block.rows).unwrap_or(usize::MAX);
                if count > limit.rows().saturating_sub(rows.len()) {
                    rows.clear();
                    failure = Some(ErrorKind::Overflow);
                    continue;
                }
                // ClickHouse can send a schema-less empty block as a keepalive. It does not
                // change the result schema and carries no rows.
                if block.column_types.is_empty() && block.column_data.is_empty() && count == 0 {
                    continue;
                }
                let current_schema = block
                    .column_types
                    .iter()
                    .map(|(name, ty)| (name.clone(), ty.clone()))
                    .collect::<Vec<_>>();
                if schema
                    .as_ref()
                    .is_some_and(|previous| *previous != current_schema)
                {
                    rows.clear();
                    failure = Some(ErrorKind::Decode);
                    continue;
                }
                if schema.is_none() {
                    schema = Some(current_schema);
                }
                if (count > 0 && block.column_types.is_empty())
                    || block.column_data.len() != block.column_types.len()
                    || !block.column_data.keys().eq(block.column_types.keys())
                    || block.column_types.values().any(|ty| !supported_type(ty))
                    || block.column_types.keys().any(|name| {
                        block
                            .column_data
                            .get(name)
                            .is_none_or(|values| values.len() != count)
                    })
                {
                    rows.clear();
                    failure = Some(ErrorKind::Decode);
                    continue;
                }
                for native in block.take_iter_rows() {
                    let mut columns = Vec::with_capacity(native.len());
                    for (name, ty, value) in native {
                        let converted = match (ty, value) {
                            (Type::Int64, NativeValue::Int64(value)) => Value::I64(value),
                            (Type::UInt64, NativeValue::UInt64(value)) => {
                                if let Ok(value) = i64::try_from(value) {
                                    Value::I64(value)
                                } else {
                                    failure = Some(ErrorKind::Overflow);
                                    break;
                                }
                            }
                            // ClickHouse String has no UTF-8 invariant. Never infer a different
                            // DBAPI type from the contents of a particular row.
                            (Type::String, NativeValue::String(value)) => Value::Bytes(value),
                            (Type::Nullable(inner), NativeValue::Null)
                                if matches!(inner.as_ref(), Type::Int64 | Type::UInt64) =>
                            {
                                Value::Null(DbType::I64)
                            }
                            (Type::Nullable(inner), NativeValue::Null)
                                if matches!(inner.as_ref(), Type::String) =>
                            {
                                Value::Null(DbType::Bytes)
                            }
                            (Type::Nullable(inner), NativeValue::Int64(value))
                                if matches!(inner.as_ref(), Type::Int64) =>
                            {
                                Value::I64(value)
                            }
                            (Type::Nullable(inner), NativeValue::UInt64(value))
                                if matches!(inner.as_ref(), Type::UInt64) =>
                            {
                                if let Ok(value) = i64::try_from(value) {
                                    Value::I64(value)
                                } else {
                                    failure = Some(ErrorKind::Overflow);
                                    break;
                                }
                            }
                            (Type::Nullable(inner), NativeValue::String(value))
                                if matches!(inner.as_ref(), Type::String) =>
                            {
                                Value::Bytes(value)
                            }
                            _ => {
                                failure = Some(ErrorKind::Decode);
                                break;
                            }
                        };
                        columns.push((name.to_owned(), converted));
                    }
                    if failure.is_some() {
                        break;
                    }
                    let Ok(row) = Row::new(columns) else {
                        failure = Some(ErrorKind::Decode);
                        break;
                    };
                    let Ok(row_bytes) = row.encoded_bytes() else {
                        failure = Some(ErrorKind::Overflow);
                        break;
                    };
                    let Some(next_bytes) = bytes.checked_add(row_bytes) else {
                        failure = Some(ErrorKind::Overflow);
                        break;
                    };
                    if next_bytes > limit.bytes() {
                        failure = Some(ErrorKind::Overflow);
                        break;
                    }
                    bytes = next_bytes;
                    rows.push(row);
                }
                if failure.is_some() {
                    rows.clear();
                }
            }
            Some(Ok(QueryEvent::EndOfStream)) => {
                return Collected {
                    rows: match failure {
                        Some(kind) => Err(DbError::new(kind)),
                        None => Ok(rows),
                    },
                    connection_clean: true,
                };
            }
            Some(Err(_)) | None => {
                return Collected {
                    rows: Err(unknown()),
                    connection_clean: false,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::collect;
    use futures_util::stream;
    use klickhouse::{
        IndexMap, QueryEvent, Type, Value as NativeValue, block::Block, block::BlockInfo,
    };
    use neotales_dbapi::{ErrorKind, RowLimit};

    #[tokio::test]
    async fn ledger_unsigned_results_are_exact_and_overflow_is_not_truncated() {
        fn unsigned(values: Vec<NativeValue>, ty: Type) -> Block {
            let mut types = IndexMap::new();
            types.insert("n".to_owned(), ty);
            let mut data = IndexMap::new();
            let rows = values.len() as u64;
            data.insert("n".to_owned(), values);
            Block {
                info: BlockInfo::default(),
                rows,
                column_types: types,
                column_data: data,
            }
        }
        for (ty, value, expected) in
            [(Type::UInt64, NativeValue::UInt64(i64::MAX as u64), i64::MAX)]
        {
            let result = collect(
                stream::iter(vec![
                    Ok(QueryEvent::Data(unsigned(vec![value], ty))),
                    Ok(QueryEvent::EndOfStream),
                ]),
                RowLimit::new(1, 256).unwrap(),
            )
            .await;
            assert!(result.connection_clean);
            assert_eq!(result.rows.unwrap()[0].get::<i64>("n").unwrap(), expected);
        }
        let result = collect(
            stream::iter(vec![
                Ok(QueryEvent::Data(unsigned(
                    vec![NativeValue::UInt64(i64::MAX as u64 + 1)],
                    Type::UInt64,
                ))),
                Ok(QueryEvent::EndOfStream),
            ]),
            RowLimit::new(1, 256).unwrap(),
        )
        .await;
        assert!(result.connection_clean);
        assert_eq!(result.rows.unwrap_err().kind(), ErrorKind::Overflow);
    }

    #[tokio::test]
    async fn nullable_uint64_preserves_nulls_and_rejects_overflow() {
        let mut types = IndexMap::new();
        types.insert("n".to_owned(), Type::Nullable(Box::new(Type::UInt64)));
        for (values, expected) in [
            (vec![NativeValue::Null], Ok(None)),
            (
                vec![NativeValue::UInt64(i64::MAX as u64)],
                Ok(Some(i64::MAX)),
            ),
            (
                vec![NativeValue::UInt64(i64::MAX as u64 + 1)],
                Err(ErrorKind::Overflow),
            ),
        ] {
            let mut data = IndexMap::new();
            data.insert("n".to_owned(), values);
            let block = Block {
                info: BlockInfo::default(),
                rows: 1,
                column_types: types.clone(),
                column_data: data,
            };
            let result = collect(
                stream::iter(vec![
                    Ok(QueryEvent::Data(block)),
                    Ok(QueryEvent::EndOfStream),
                ]),
                RowLimit::new(1, 256).unwrap(),
            )
            .await;
            assert!(result.connection_clean);
            assert_eq!(
                result
                    .rows
                    .map(|rows| rows[0].get::<Option<i64>>("n").unwrap())
                    .map_err(neotales_dbapi::DbError::kind),
                expected
            );
        }
    }

    fn block(values: &[i64]) -> Block {
        let mut types = IndexMap::new();
        types.insert("n".to_owned(), Type::Int64);
        let mut data = IndexMap::new();
        data.insert(
            "n".to_owned(),
            values.iter().copied().map(NativeValue::Int64).collect(),
        );
        Block {
            info: BlockInfo::default(),
            rows: values.len() as u64,
            column_types: types,
            column_data: data,
        }
    }

    #[tokio::test]
    async fn aggregates_and_requires_the_terminal_packet() {
        let limit = RowLimit::new(2, 18).unwrap();
        let events = stream::iter(vec![
            Ok(QueryEvent::Data(block(&[1]))),
            Ok(QueryEvent::Data(block(&[2]))),
            Ok(QueryEvent::EndOfStream),
        ]);
        let collected = collect(events, limit).await;
        assert!(collected.connection_clean);
        assert_eq!(collected.rows.unwrap().len(), 2);

        let events = stream::iter(vec![
            Ok(QueryEvent::Data(block(&[1]))),
            Ok(QueryEvent::Data(block(&[2]))),
            Ok(QueryEvent::EndOfStream),
        ]);
        let collected = collect(events, RowLimit::new(2, 17).unwrap()).await;
        assert!(collected.connection_clean);
        assert_eq!(collected.rows.unwrap_err().kind(), ErrorKind::Overflow);

        let events = stream::iter(vec![Ok(QueryEvent::Data(block(&[1, 2, 3])))]);
        let collected = collect(events, limit).await;
        assert!(!collected.connection_clean);
        assert_eq!(
            collected.rows.unwrap_err().kind(),
            ErrorKind::WriteOutcomeUnknown
        );

        let events = stream::iter(vec![Ok(QueryEvent::Data(block(&[1])))]);
        let collected = collect(events, RowLimit::new(10, 256).unwrap()).await;
        assert!(!collected.connection_clean);
        assert_eq!(
            collected.rows.unwrap_err().kind(),
            ErrorKind::WriteOutcomeUnknown
        );
    }

    #[tokio::test]
    async fn local_overflow_is_drained_to_eos() {
        let events = stream::iter(vec![
            Ok(QueryEvent::Data(block(&[1, 2, 3]))),
            Ok(QueryEvent::EndOfStream),
        ]);
        let collected = collect(events, RowLimit::new(2, 18).unwrap()).await;
        assert!(collected.connection_clean);
        assert_eq!(collected.rows.unwrap_err().kind(), ErrorKind::Overflow);
    }

    #[tokio::test]
    async fn server_failure_wins_over_local_failure() {
        let events = stream::iter(vec![
            Ok(QueryEvent::Data(block(&[1, 2, 3]))),
            Err(klickhouse::KlickhouseError::ProtocolError(
                "injected".to_owned(),
            )),
        ]);
        let collected = collect(events, RowLimit::new(2, 18).unwrap()).await;
        assert!(!collected.connection_clean);
        assert_eq!(
            collected.rows.unwrap_err().kind(),
            ErrorKind::WriteOutcomeUnknown
        );
    }

    #[tokio::test]
    async fn schema_drift_is_rejected_after_eos() {
        let first = block(&[1]);
        let mut changed = block(&[2]);
        let data = changed.column_data.swap_remove("n").unwrap();
        changed.column_types.clear();
        changed
            .column_types
            .insert("other".to_string(), Type::Int64);
        changed.column_data.insert("other".to_string(), data);
        let events = stream::iter(vec![
            Ok(QueryEvent::Data(first)),
            Ok(QueryEvent::Data(changed)),
            Ok(QueryEvent::EndOfStream),
        ]);
        let collected = collect(events, RowLimit::new(10, 256).unwrap()).await;
        assert!(collected.connection_clean);
        assert_eq!(collected.rows.unwrap_err().kind(), ErrorKind::Decode);
    }

    #[tokio::test]
    async fn unsupported_empty_schema_is_rejected() {
        let mut empty = block(&[]);
        empty.column_types.insert("n".to_string(), Type::UInt16);
        let events = stream::iter(vec![
            Ok(QueryEvent::Data(empty)),
            Ok(QueryEvent::EndOfStream),
        ]);
        let collected = collect(events, RowLimit::new(10, 256).unwrap()).await;
        assert!(collected.connection_clean);
        assert_eq!(collected.rows.unwrap_err().kind(), ErrorKind::Decode);
    }

    #[tokio::test]
    async fn empty_results_are_ok() {
        let events = stream::iter(vec![Ok(QueryEvent::EndOfStream)]);
        let collected = collect(events, RowLimit::new(10, 256).unwrap()).await;
        assert!(collected.connection_clean);
        assert!(collected.rows.unwrap().is_empty());
    }

    #[tokio::test]
    async fn string_columns_are_bytes_regardless_of_utf8() {
        let mut types = IndexMap::new();
        types.insert("s".to_owned(), Type::String);
        let mut data = IndexMap::new();
        data.insert(
            "s".to_owned(),
            vec![
                NativeValue::String(b"hello".to_vec()),
                NativeValue::String(vec![0xff]),
            ],
        );
        let events = stream::iter(vec![
            Ok(QueryEvent::Data(Block {
                info: BlockInfo::default(),
                rows: 2,
                column_types: types,
                column_data: data,
            })),
            Ok(QueryEvent::EndOfStream),
        ]);
        let collected = collect(events, RowLimit::new(2, 100).unwrap()).await;
        let rows = collected.rows.unwrap();
        assert_eq!(rows[0].get::<Vec<u8>>("s").unwrap(), b"hello");
        assert_eq!(rows[1].get::<Vec<u8>>("s").unwrap(), vec![0xff]);
    }
}
