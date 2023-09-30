// System
use std::collections::BTreeMap;

// Third Party
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::{
    api::core::v1::{ConfigMap, Node},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, PostParams, WatchEvent, WatchParams},
    Client,
};
use tracing::{debug, error, info};

// Local
use crate::utils::{code_key_slashes, LABEL_STORE_VERSION_KEY, TRANSACTION_NAMESPACE};

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
        debug!("Node added: {:?}", node.metadata.name.clone().unwrap());
        let name = format!(
            "{}.{}.added",
            node.metadata.name.clone().unwrap(),
            node.metadata.resource_version.clone().unwrap()
        );
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some(TRANSACTION_NAMESPACE.to_string()),
                ..Default::default()
            },
            data: None, // We don't store any data, this is just a transaction marker
            ..Default::default()
        };

        if let Err(error) = config_map_api
            .create(&PostParams::default(), &config_map)
            .await
        {
            match error {
                // 409 Conflict
                kube::Error::Api(kube::error::ErrorResponse { code, .. }) if code == 409 => {
                    debug!("ConfigMap already exists: {}", name);
                }
                _ => return Err(anyhow::Error::new(error)),
            }
        } else {
            debug!("Created transaction ConfigMap: {}", name);
        }
        Ok(())
    }

    /// Store all labels in a ConfigMap transaction with name `<NODE_NAME>.<NODE_RESOURCE_VERSION>.deleted`, in the namespace `TRANSACTION_NAMESPACE`.
    async fn node_deleted(
        node: &Node,
        config_map_api: &Api<ConfigMap>,
    ) -> Result<(), anyhow::Error> {
        debug!("Node deleted: {:?}", node.metadata.name.clone().unwrap());
        let name = format!(
            "{}.{}.deleted",
            node.metadata.name.clone().unwrap(),
            node.metadata.resource_version.clone().unwrap()
        );
        let mut labels = node.metadata.labels.clone().unwrap_or(BTreeMap::new());
        labels.insert(
            LABEL_STORE_VERSION_KEY.to_string(),
            node.metadata.resource_version.clone().unwrap(),
        );
        code_key_slashes(&mut labels, true);
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.clone()),
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
                    debug!("ConfigMap already exists: {}", name);
                }
                _ => return Err(anyhow::Error::new(error)),
            }
        } else {
            debug!("Created transaction ConfigMap: {}", name);
        }
        Ok(())
    }

    /// Start watching all node events and save and restore labels as needed.
    /// This will save all metadata labels for a node when it is deleted.
    pub async fn watch_nodes(&self) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let nodes_list = nodes.list(&Default::default()).await?;
        let nodes_resource_version = nodes_list.metadata.resource_version.unwrap();

        info!("Starting node label watcher...");
        // It's expected that this stream will periodically break and will have to be re-issued.
        let mut stream = nodes
            .watch(&WatchParams::default(), &nodes_resource_version)
            .await?
            .boxed();
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
                        if let Err(e) = Self::node_deleted(&node, &config_map_api).await {
                            error!("Error in node_deleted: {}", e);
                        }
                    });
                }
                _ => {}
            }
        }

        Ok(())
    }
}
