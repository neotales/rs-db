use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::query::extract_row_affected;
use crate::{
    BatchCompletion, CommandTag, Error, SimpleQueryMessage, SimpleQueryRow, TransactionStatus,
};
use bytes::Bytes;
use fallible_iterator::FallibleIterator;
use futures_util::Stream;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// Information about a column of a single query row.
#[derive(Debug)]
pub struct SimpleColumn {
    name: String,
}

impl SimpleColumn {
    pub(crate) fn new(name: String) -> SimpleColumn {
        SimpleColumn { name }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }
}

pub async fn simple_query(client: &InnerClient, query: &str) -> Result<SimpleQueryStream, Error> {
    let buf = encode(client, query)?;
    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(SimpleQueryStream {
        responses,
        columns: None,
        command_tag: CommandTag::Other,
        transaction_control: false,
        transaction_status: None,
    })
}

pub async fn batch_execute(client: &InnerClient, query: &str) -> Result<(), Error> {
    let buf = encode(client, query)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    loop {
        match responses.next().await? {
            Message::ReadyForQuery(_) => return Ok(()),
            Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::RowDescription(_)
            | Message::DataRow(_) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

pub async fn batch_complete(client: &InnerClient, query: &str) -> Result<BatchCompletion, Error> {
    let buf = encode(client, query)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;
    let mut tag = CommandTag::Other;
    let mut transaction_control = false;
    let mut commands = 0u64;
    loop {
        match responses.next().await? {
            Message::ReadyForQuery(body) => {
                return Ok(BatchCompletion {
                    commands,
                    tag,
                    status: TransactionStatus::parse(body.status())?,
                    transaction_control,
                });
            }
            Message::CommandComplete(body) => {
                commands = commands
                    .checked_add(1)
                    .ok_or_else(Error::unexpected_message)?;
                tag = CommandTag::parse(body.tag().map_err(Error::parse)?);
                transaction_control |= tag.is_transaction_control();
            }
            Message::EmptyQueryResponse | Message::RowDescription(_) | Message::DataRow(_) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

fn encode(client: &InnerClient, query: &str) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        frontend::query(query, buf).map_err(Error::encode)?;
        Ok(buf.split().freeze())
    })
}

pin_project! {
    /// A stream of simple query results.
    #[project(!Unpin)]
    pub struct SimpleQueryStream {
        responses: Responses,
        columns: Option<Arc<[SimpleColumn]>>,
        command_tag: CommandTag,
        transaction_control: bool,
        transaction_status: Option<TransactionStatus>,
    }
}

impl Stream for SimpleQueryStream {
    type Item = Result<SimpleQueryMessage, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.responses.poll_next(cx)?) {
            Message::CommandComplete(body) => {
                let rows = extract_row_affected(&body)?;
                *this.command_tag = CommandTag::parse(body.tag().map_err(Error::parse)?);
                *this.transaction_control |= this.command_tag.is_transaction_control();
                Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(rows))))
            }
            Message::EmptyQueryResponse => {
                Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(0))))
            }
            Message::RowDescription(body) => {
                let columns: Arc<[SimpleColumn]> = body
                    .fields()
                    .map(|f| Ok(SimpleColumn::new(f.name().to_string())))
                    .collect::<Vec<_>>()
                    .map_err(Error::parse)?
                    .into();

                *this.columns = Some(columns.clone());
                Poll::Ready(Some(Ok(SimpleQueryMessage::RowDescription(columns))))
            }
            Message::DataRow(body) => {
                let row = match &this.columns {
                    Some(columns) => SimpleQueryRow::new(columns.clone(), body)?,
                    None => return Poll::Ready(Some(Err(Error::unexpected_message()))),
                };
                Poll::Ready(Some(Ok(SimpleQueryMessage::Row(row))))
            }
            Message::ReadyForQuery(body) => {
                *this.transaction_status = Some(TransactionStatus::parse(body.status())?);
                Poll::Ready(None)
            }
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}

impl SimpleQueryStream {
    /// Whether any observed command managed transactions. Remains available even
    /// if a later command in this script produces a server error.
    pub fn transaction_control(&self) -> bool {
        self.transaction_control
    }

    /// Last classified command tag; not itself proof of successful completion.
    pub fn command_tag(&self) -> CommandTag {
        self.command_tag
    }

    /// Final transaction status; None until ReadyForQuery is consumed.
    pub fn transaction_status(&self) -> Option<TransactionStatus> {
        self.transaction_status
    }
}
