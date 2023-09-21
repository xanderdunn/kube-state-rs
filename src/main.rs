// Third Party
use kube::Client;

// Local
use kube_state_rs::{
    utils::{init_tracing, setup_exit_hooks},
    NodeLabelPersistenceService,
};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing();
    setup_exit_hooks();
    // TODO: We may need to modify how the client is created to suit our production kubeconfig
    let client = Client::try_default().await.unwrap();
    let node_watcher = NodeLabelPersistenceService::new("default", &client)
        .await
        .unwrap();
    // We prefer to crash on error so that the k8s controller can restart the pod
    node_watcher.watch_nodes().await.unwrap();
    Ok(())
}
