// System
use std::collections::BTreeMap;

// Third Party
use futures::{pin_mut, TryStreamExt};
use k8s_openapi::{
    api::core::v1::{ConfigMap, Node},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, PostParams, WatchEvent, WatchParams},
    Client,
};
use rand::Rng;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, warn};

// Local
use crate::utils::{
    code_key_slashes, hash_node_name, LABEL_STORE_VERSION_KEY, TRANSACTION_NAMESPACE,
    TRANSACTION_TYPE_KEY,
};

const NUM_WATCH_RETRIES: u32 = 30;

/// A service that watches for node added and node deleted events. When it encounters one, it
/// creates a versioned transaction as a ConfigMap in the namespace `TRANSACTION_NAMESPACE`.
///
pub struct Watcher {
    client: Client,
    config_map_api: Api<ConfigMap>,
}

impl Watcher {
    pub fn new(client: &Client) -> Self {
        let config_map_api: Api<ConfigMap> = Api::namespaced(client.clone(), TRANSACTION_NAMESPACE);
        Self {
            client: client.clone(),
            config_map_api,
        }
    }

    /// Creates a ConfigMap transaction with the name `<NODE_NAME>.<NODE_RESOURCE_VERSION>.added`, in the namespace `TRANSACTION_NAMESPACE`.
    async fn node_added(node: &Node, config_map_api: &Api<ConfigMap>) -> Result<(), anyhow::Error> {
        let node_name_hash = hash_node_name(&node.metadata.name.clone().unwrap());
        let transaction_name = format!(
            "{}.{}",
            node_name_hash,
            node.metadata.resource_version.clone().unwrap()
        );
        debug!(
            "Node added: {:?}, creating transaction {}...",
            node.metadata.name.clone().unwrap(),
            transaction_name
        );
        let mut data: BTreeMap<String, String> = BTreeMap::new();
        data.insert(TRANSACTION_TYPE_KEY.to_string(), "added".to_string());
        data.insert("node_name".to_string(), node.metadata.name.clone().unwrap());
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some(transaction_name.clone()),
                namespace: Some(TRANSACTION_NAMESPACE.to_string()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        };

        if let Err(error) = config_map_api
            .create(&PostParams::default(), &config_map)
            .await
        {
            match error {
                // 409 Conflict
                kube::Error::Api(kube::error::ErrorResponse { code, .. }) if code == 409 => {
                    debug!("ConfigMap already exists: {}", transaction_name);
                }
                _ => return Err(anyhow::Error::new(error)),
            }
        } else {
            debug!("Created transaction ConfigMap: {}", transaction_name);
        }
        Ok(())
    }

    /// Store all labels in a ConfigMap transaction with name `<NODE_NAME>.<NODE_RESOURCE_VERSION>.deleted`, in the namespace `TRANSACTION_NAMESPACE`.
    async fn node_deleted(
        node: &Node,
        config_map_api: &Api<ConfigMap>,
    ) -> Result<(), anyhow::Error> {
        let node_name_hash = hash_node_name(&node.metadata.name.clone().unwrap());
        let transaction_name = format!(
            "{}.{}",
            node_name_hash,
            node.metadata.resource_version.clone().unwrap()
        );
        debug!(
            "Node deleted: {:?}, creating transaction {}...",
            node.metadata.name.clone().unwrap(),
            transaction_name
        );
        let mut labels = node.metadata.labels.clone().unwrap_or(BTreeMap::new());
        labels.insert(
            LABEL_STORE_VERSION_KEY.to_string(),
            node.metadata.resource_version.clone().unwrap(),
        );
        labels.insert(TRANSACTION_TYPE_KEY.to_string(), "deleted".to_string());
        labels.insert("node_name".to_string(), node.metadata.name.clone().unwrap());
        code_key_slashes(&mut labels, true);
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some(transaction_name.clone()),
                namespace: Some(TRANSACTION_NAMESPACE.to_string()),
                ..Default::default()
            },
            data: Some(labels),
            ..Default::default()
        };

        if let Err(error) = config_map_api
            .create(&PostParams::default(), &config_map)
            .await
        {
            match error {
                kube::Error::Api(kube::error::ErrorResponse { code, .. }) if code == 409 => {
                    debug!("ConfigMap already exists: {}", transaction_name);
                }
                _ => return Err(anyhow::Error::new(error)),
            }
        } else {
            debug!("Created transaction ConfigMap: {}", transaction_name);
        }
        Ok(())
    }

    /// Start watching all node events and save and restore labels as needed.
    /// This will save all metadata labels for a node when it is deleted.
    pub async fn watch_nodes(&self) -> Result<(), anyhow::Error> {
        let mut delay_duration = Duration::from_secs(1);
        let max_delay = Duration::from_secs(30);
        let mut rng = rand::thread_rng();
        let mut num_retries = 0;

        loop {
            let nodes: Api<Node> = Api::all(self.client.clone());
            let nodes_resource_version = {
                let nodes_list = nodes.list(&Default::default()).await?;
                nodes_list.metadata.resource_version.unwrap()
            };
            info!("Starting node label watcher...");
            let watch_result = nodes
                .watch(&WatchParams::default(), &nodes_resource_version)
                .await;

            match watch_result {
                Ok(stream) => {
                    pin_mut!(stream);
                    // Successfully connected, so reset duration and retry limit
                    delay_duration = Duration::from_secs(1);
                    num_retries = 0;
                    while let Some(status) = stream.try_next().await? {
                        let config_map_api = self.config_map_api.clone();
                        match status {
                            WatchEvent::Added(node) => {
                                tokio::spawn(async move {
                                    if let Err(e) = Self::node_added(&node, &config_map_api).await {
                                        error!("Error in node_added: {}", e);
                                    }
                                });
                            }
                            WatchEvent::Deleted(node) => {
                                tokio::spawn(async move {
                                    if let Err(e) = Self::node_deleted(&node, &config_map_api).await
                                    {
                                        error!("Error in node_deleted: {}", e);
                                    }
                                });
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    error!("Watch connection broken: {}", e);
                }
            }

            if num_retries < NUM_WATCH_RETRIES {
                num_retries += 1;
                // Implement exponential backoff with jitter
                let jitter = rng.gen_range(0..500);
                warn!(
                    "Retrying watch in {} seconds...",
                    (delay_duration + Duration::from_millis(jitter)).as_secs()
                );
                time::sleep(delay_duration + Duration::from_millis(jitter)).await;
                // Increase the delay for next retry
                delay_duration = Duration::from_secs(std::cmp::min(
                    max_delay.as_secs(),
                    delay_duration.as_secs() * 2,
                ));
            } else {
                let message = format!("Max retries {} exceeded", NUM_WATCH_RETRIES);
                return Err(anyhow::Error::msg(message));
            }
        }
    }
}
