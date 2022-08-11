use std::future::Future;

use tokio::runtime::Runtime;
use tonic::transport::{Channel, Endpoint};

use crate::api::{connection::ConnectionControl, driver::SQLError};

use super::simpledb::{
    connection_client::ConnectionClient, CloseConnectionRequest, CommitRequest,
    RollbackRequest,
};

pub struct NetworkConnection {
    channel: Channel,
    rt: Runtime,
    client: ConnectionClient<Channel>,
}

impl NetworkConnection {
    pub fn new(endpoint: Endpoint) -> Result<NetworkConnection, SQLError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let channel = rt.block_on(endpoint.connect())?;
        let client = ConnectionClient::new(channel.clone());
        Ok(NetworkConnection {
            channel,
            rt,
            client,
        })
    }

    pub(in crate::api::network) fn channel(&self) -> Channel {
        self.channel.clone()
    }

    pub(in crate::api::network) fn run<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.rt.block_on(future)
    }
}

impl ConnectionControl for NetworkConnection {
    fn close(&mut self) -> Result<(), SQLError> {
        let request = tonic::Request::new(CloseConnectionRequest {});
        self.rt.block_on(self.client.close(request))?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), SQLError> {
        let request = tonic::Request::new(CommitRequest {});
        self.rt.block_on(self.client.commit(request))?;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), SQLError> {
        let request = tonic::Request::new(RollbackRequest {});
        self.rt.block_on(self.client.rollback(request))?;
        Ok(())
    }
}
