use std::{
    future,
    sync::{Arc, Mutex},
    vec,
};

use tarpc::{context, serde_transport::tcp, server, tokio_serde::formats::Json};

#[derive(thiserror::Error, Debug)]
pub enum WorkerError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[tarpc::service]
pub trait Worker {
    async fn ping() -> ();
    async fn get() -> Vec<u8>;
    async fn load(data: Vec<u8>) -> ();
    async fn map() -> ();
}

#[derive(Clone)]
pub struct WorkerServer {
    data: Arc<Mutex<Vec<u8>>>,
}

impl WorkerServer {
    pub async fn listen(address: &str) -> Result<(), WorkerError> {
        use futures::StreamExt;
        use tarpc::server::incoming::Incoming;
        use tarpc::server::Channel;

        let mut listener = tcp::listen(address, Json::default).await?;

        listener.config_mut().max_frame_length(usize::MAX);

        let data = Arc::new(Mutex::new(vec![]));

        listener
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
            .map(|channel| {
                let server = WorkerServer {
                    data: Arc::clone(&data),
                };
                channel.execute(server.serve())
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;

        Ok(())
    }
}

impl Worker for WorkerServer {
    type PingFut = future::Ready<()>;
    fn ping(self, _: context::Context) -> Self::PingFut {
        future::ready(())
    }

    type GetFut = future::Ready<Vec<u8>>;
    fn get(self, _: context::Context) -> Self::GetFut {
        future::ready(self.data.lock().unwrap().clone())
    }

    type LoadFut = future::Ready<()>;
    fn load(self, _: context::Context, data: Vec<u8>) -> Self::LoadFut {
        *self.data.lock().unwrap() = data;

        future::ready(())
    }

    type MapFut = future::Ready<()>;
    fn map(self, _: context::Context) -> Self::MapFut {
        future::ready(())
    }
}
