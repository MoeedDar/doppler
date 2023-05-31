use std::pin::Pin;

use futures::future;
use tarpc::{
    client::{self, RpcError},
    context,
    serde_transport::tcp,
    tokio_serde::formats::Json,
};

use crate::worker::WorkerClient;

#[derive(thiserror::Error, Debug)]
pub enum DopplerError {
    #[error("shard at {0} not found")]
    ShardNotFound(String),

    #[error("task (0) failed at {:?}", ..,)]
    TaskFail(String, Vec<(String, DopplerError)>),

    #[error("invalid number of args, recieved: {0}, expected: {1}")]
    InvalidNumArgs(usize, usize),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    RpcError(#[from] tarpc::client::RpcError),
}

pub struct Doppler {
    shards: Vec<(String, WorkerClient)>,
}

impl Doppler {
    pub fn new() -> Doppler {
        Doppler { shards: Vec::new() }
    }

    pub async fn connect(&mut self, address: &str) -> Result<(), DopplerError> {
        let transport = tcp::connect(address, Json::default).await?;

        let client = WorkerClient::new(client::Config::default(), transport).spawn();

        self.shards.push((address.into(), client));

        Ok(())
    }

    pub async fn disconnect(&mut self, address: &str) -> Result<(), DopplerError> {
        self.shards
            .iter()
            .position(|shard| shard.0 == address)
            .map_or(Err(DopplerError::ShardNotFound(address.into())), |index| {
                self.shards.remove(index);
                Ok(())
            })
    }

    async fn call<F, A, R>(
        &mut self,
        kill: bool,
        task_name: &str,
        args: Vec<A>,
        f: F,
    ) -> Result<Vec<R>, DopplerError>
    where
        F: Fn(usize, A) -> Pin<Box<dyn future::Future<Output = Result<R, RpcError>>>>,
    {
        if args.len() != self.shards.len() {
            return Err(DopplerError::InvalidNumArgs(args.len(), self.shards.len()));
        }

        let calls = args
            .into_iter()
            .enumerate()
            .map(|(index, arg)| f(index, arg))
            .collect::<Vec<_>>();

        let results = future::join_all(calls).await;

        let (successes, failiures): (Vec<_>, Vec<_>) =
            results.into_iter().enumerate().partition(|r| r.1.is_ok());

        if !failiures.is_empty() {
            let (indices, results): (Vec<_>, Vec<_>) = failiures.into_iter().unzip();

            let errors = results
                .into_iter()
                .map(|result| result.err().unwrap().into())
                .collect::<Vec<DopplerError>>();

            let addresses = indices
                .iter()
                .map(|&i| self.shards[i].0.clone())
                .collect::<Vec<_>>();

            if kill {
                indices.into_iter().for_each(|index| {
                    self.shards.remove(index);
                });
            }

            let error_info = addresses.into_iter().zip(errors).collect::<Vec<_>>();

            return Err(DopplerError::TaskFail(task_name.to_string(), error_info));
        }

        Ok(successes
            .into_iter()
            .map(|result| result.1.unwrap())
            .collect())
    }

    async fn ping(&mut self) -> Result<(), DopplerError> {
        let shards = self.shards.clone();

        self.call(true, "ping", vec![(); shards.len()], |index, _| {
            let shard = shards[index].1.clone();

            Box::pin(async move {
                shard.ping(context::current()).await?;
                Ok(())
            })
        })
        .await
        .map(|_| ())
    }

    pub async fn get(&mut self) -> Result<Vec<u8>, DopplerError> {
        self.ping().await?;

        let shards = self.shards.clone();

        self.call(false, "get", vec![(); shards.len()], |index, _| {
            let shard = shards[index].1.clone();

            Box::pin(async move {
                let result = shard.get(context::current()).await?;
                Ok(result)
            })
        })
        .await
        .map(|results| results.into_iter().flatten().collect::<Vec<_>>())
    }

    pub async fn load(&mut self, data: Vec<u8>) -> Result<(), DopplerError> {
        self.ping().await?;

        let num_chunks = data.len() / self.shards.len();
        let chunks = data.chunks(num_chunks);

        let args = chunks.into_iter().collect();

        let shards = self.shards.clone();

        self.call(false, "get", args, |index, args| {
            let shard = shards[index].1.clone();

            let chunk = args.to_vec();

            if index > num_chunks {
                return Box::pin(async { Ok(()) });
            }

            Box::pin(async move {
                shard.load(context::current(), chunk).await?;
                Ok(())
            })
        })
        .await
        .map(|_| ())
    }

    pub async fn map(&mut self, func: &str) -> Result<(), DopplerError> {
        let shards = self.shards.clone();

        self.call(false, "map", vec![(); shards.len()], |index, _| {
            let shard = shards[index].1.clone();
            let func = func.to_string();

            Box::pin(async move {
                shard.map(context::current(), func).await?;
                Ok(())
            })
        })
        .await
        .map(|_| ())
    }
}
