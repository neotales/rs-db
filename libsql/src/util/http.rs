use super::box_clone_service::BoxCloneService;
use hyper::rt::{Read, Write};
use hyper_util::client::legacy::connect::{Connected, Connection};

pub trait Socket: Connection + Read + Write + Send + Unpin + 'static + Sync {}

impl<T> Socket for T where T: Connection + Read + Write + Send + Unpin + 'static + Sync {}

impl Connection for Box<dyn Socket> {
    fn connected(&self) -> Connected {
        self.as_ref().connected()
    }
}

pub type ConnectorService =
    BoxCloneService<http::Uri, Box<dyn Socket>, Box<dyn std::error::Error + Sync + Send + 'static>>;

#[cfg(feature = "replication")]
pub type HttpRequestCallback = std::sync::Arc<dyn Fn(&mut http::Request<()>) + Send + Sync>;
