// Third Party
use kube::Client;

// Local
use kube_state_rs::{
    processor::TransactionProcessor,
    utils::{
        create_namespace, init_tracing, setup_exit_hooks, NODE_METADATA_NAMESPACE,
        TRANSACTION_NAMESPACE,
    },
};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing("kube_state_rs", tracing::Level::DEBUG);
    setup_exit_hooks();
    let client = Client::try_default().await.unwrap();
    create_namespace(&client, TRANSACTION_NAMESPACE).await?;
    create_namespace(&client, NODE_METADATA_NAMESPACE).await?;
    let transaction_processor = TransactionProcessor::new(&client);

    // TODO: Spawn one for each available core
    transaction_processor.process().await?;

    Ok(())
}
