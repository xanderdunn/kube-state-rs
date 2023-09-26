// Third Party
use k8s_openapi::api::apps::v1::StatefulSet;
use kube::api::ListParams;
use kube::{Api, Client};
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};

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
        let num_ready_replicas: usize = {
            let stateful_sets: Api<StatefulSet> = Api::namespaced(client.clone(), "default");
            let lp = ListParams::default().fields("metadata.name=kube-state-rs");
            let stateful_sets = stateful_sets.list(&lp).await?;
            assert_eq!(stateful_sets.items.len(), 1);
            let stateful_set = &stateful_sets.items[0];
            match stateful_set.status {
                Some(ref status) => status.ready_replicas.unwrap_or(0) as usize,
                None => 0,
            }
        };

        if num_ready_replicas == 0 {
            warn!("No ready replicas, sleeping for 1 second");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Crash if there is no hostname or the hostname is not in the expected format
        let my_hostname = std::env::var("HOSTNAME").unwrap();
        let my_replica_id: usize = my_hostname.split('-').last().unwrap().parse::<usize>()?;

        debug!(
            "There are {} ready replicas and my hostname is {} and my replica id is {}",
            num_ready_replicas, my_hostname, my_replica_id
        );

        let loop_start = tokio::time::Instant::now();
        node_watcher
            .handle_nodes(num_ready_replicas, my_replica_id)
            .await
            .unwrap();
        let elapsed = loop_start.elapsed();

        if elapsed < interval {
            sleep(interval - elapsed).await;
        }
    }
}
