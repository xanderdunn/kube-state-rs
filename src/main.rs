// Third Party
use futures::future::BoxFuture;
use kube::Client;
use tracing::error;

// Local
use kube_state_rs::{
    utils::{init_tracing, setup_exit_hooks},
    NodeLabelPersistenceService,
};

// If the watch service fails, this is a last ditch effort to restart it.
fn watch(
    node_watcher: NodeLabelPersistenceService,
) -> BoxFuture<'static, Result<(), anyhow::Error>> {
    Box::pin(async move {
        while let Err(e) = node_watcher.watch_nodes().await {
            error!("Error watching nodes: {}", e);
        }
        watch(node_watcher).await
    })
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing();
    setup_exit_hooks();
    // TODO: We may need to modify how the client is created to suit our production kubeconfig
    let client = Client::try_default().await.unwrap();
    let node_watcher = NodeLabelPersistenceService::new("default", &client)
        .await
        .unwrap();
    watch(node_watcher).await.unwrap();
    Ok(())
}
