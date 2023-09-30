// Third Party
use kube::Client;

// Local
use kube_state_rs::{
    utils::{create_namespace, init_tracing, setup_exit_hooks, TRANSACTION_NAMESPACE},
    watcher::Watcher,
};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing("kube_state_rs", tracing::Level::DEBUG);
    setup_exit_hooks();
    let client = Client::try_default().await.unwrap();
    create_namespace(&client, TRANSACTION_NAMESPACE).await?;
    let node_watcher = Watcher::new(&client);

    // If the watch stream ends, we panic and Kubernetes will restart the pod with backoff
    node_watcher.watch_nodes().await?;

    Ok(())
}
