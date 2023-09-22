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
    let client = Client::try_default().await.unwrap();
    let node_watcher = NodeLabelPersistenceService::new("default", &client)
        .await
        .unwrap();
    // We prefer to crash on error so that the k8s controller can restart the pod
    loop {
        node_watcher.handle_nodes().await.unwrap();
    }
}
