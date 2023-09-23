// Third Party
use kube::Client;
use tokio::time::{sleep, Duration};

// Local
use kube_state_rs::{
    utils::{init_tracing, setup_exit_hooks},
    NodeLabelPersistenceService,
};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing();
    setup_exit_hooks();
    let client = Client::try_default().await.unwrap();
    let node_watcher = NodeLabelPersistenceService::new("default", &client)
        .await
        .unwrap();

    // Run the loop at most once every second
    let interval = Duration::from_secs(1);

    // We prefer to crash on error so that the k8s controller can restart the pod
    loop {
        let loop_start = tokio::time::Instant::now();
        node_watcher.handle_nodes().await.unwrap();
        let elapsed = loop_start.elapsed();

        if elapsed < interval {
            sleep(interval - elapsed).await;
        }
    }
}
