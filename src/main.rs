// Third Party
use kube::Client;
use tracing::error;

// Local
use kube_state_rs::{utils::init_tracing, NodeLabelPersistenceService};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing();
    // TODO: We may need to modify how the client is created to suit our production kubeconfig
    let client = Client::try_default().await.unwrap();
    let node_watcher = NodeLabelPersistenceService::new("default", &client)
        .await
        .unwrap();
    while let Err(e) = node_watcher.watch_nodes().await {
        error!("Error watching nodes: {}", e);
    }
    Ok(())
}
