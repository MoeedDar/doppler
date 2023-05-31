#[cfg(test)]
mod test {
    use crate::{doppler::Doppler, worker::WorkerServer};

    #[tokio::test]
    async fn test_load_doppler() {
        let mut doppler = Doppler::new();

        tokio::spawn(WorkerServer::listen("localhost:123"));
        tokio::spawn(WorkerServer::listen("localhost:124"));

        doppler.connect("localhost:123").await.unwrap();
        doppler.connect("localhost:124").await.unwrap();

        let payload = vec![1, 2, 3, 4, 5, 6, 7, 8];

        doppler.load(payload.clone()).await.unwrap();

        let result = doppler.get().await.unwrap();

        assert_eq!(result, payload);
    }
}
