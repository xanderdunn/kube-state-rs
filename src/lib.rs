// System
use std::collections::BTreeMap;

// Third Party
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Node};
use kube::{
    api::{Api, ListParams, ObjectMeta, PartialObjectMetaExt, Patch, PatchParams},
    error::ErrorResponse,
    runtime::{watcher, watcher::Event},
    Client,
};
use serde_json::json;

pub struct NodeStateRestorer {
    client: Client,
    namespace: String,
}

impl NodeStateRestorer {
    pub async fn new(namespace: &str) -> Result<Self, kube::Error> {
        let client = Client::try_default().await?;
        Ok(NodeStateRestorer {
            client,
            namespace: namespace.to_string(),
        })
    }

    /// A convenience debug class method that prints all config map data to console.
    pub async fn print_all_config_map_data(
        client: Client,
        namespace: &str,
    ) -> Result<(), kube::Error> {
        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);

        for cm in config_maps.list(&ListParams::default()).await? {
            println!("ConfigMap: {}", cm.metadata.name.unwrap());
            if let Some(data) = cm.data {
                for (key, value) in data {
                    println!("{}: {}", key, value);
                }
            }
        }
        Ok(())
    }

    /// A convenience class method that returns all labels stored in the ConfigMap for a given node
    /// name.
    pub async fn get_config_map_labels_for_node_name(
        node_name: &str,
        client: Client,
        namespace: &str,
    ) -> Result<Option<BTreeMap<String, String>>, kube::Error> {
        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);

        for cm in config_maps.list(&ListParams::default()).await? {
            if let Some(name) = cm.metadata.name {
                if name == node_name {
                    return Ok(cm.data);
                }
            }
        }

        Ok(None)
    }

    /// A convience class method to set all labels on a node. This will overwrite any existing
    /// labels.
    pub async fn set_node_labels(
        node_name: &str,
        client: Client,
        labels: BTreeMap<String, String>,
    ) -> Result<(), kube::Error> {
        let nodes = Api::<Node>::all(client);

        let patch_params = PatchParams::apply("node-state-restorer").force();

        let metadata = ObjectMeta {
            labels: Some(labels),
            ..Default::default()
        }
        .into_request_partial::<Node>();

        nodes
            .patch_metadata(node_name, &patch_params, &Patch::Apply(metadata))
            .await?;
        Ok(())
    }

    pub async fn get_node_labels(
        node_name: &str,
        client: Client,
    ) -> Result<Option<BTreeMap<String, String>>, kube::Error> {
        let nodes: Api<Node> = Api::all(client);

        for node in nodes.list(&ListParams::default()).await? {
            if let Some(name) = node.metadata.name {
                if name == node_name {
                    return Ok(node.metadata.labels);
                }
            }
        }

        Ok(None)
    }

    /// Start watching all node events and save and restore labels as needed.
    /// This will save all metadata labels for a node when it is deleted and increment the
    /// label_version.
    /// If the label_version of the node's metadata is greater than or equal to what's stored, nothing will be
    /// restored. This is to prevent restoring labels that are intentionally deleted on a running
    /// node.
    // TODO: Simplify this method
    pub async fn watch_nodes(&self) -> Result<(), watcher::Error> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let watcher = watcher(nodes, watcher::Config::default());

        watcher
            .try_for_each(|event| async move {
                match event {
                    Event::Applied(node) => {
                        // This event is triggered when a node is either added or modified.
                        println!(
                            "Node Added/Modified: {:?}, resource_version: {:?}, labels: {:?}",
                            node.metadata.name,
                            node.metadata.resource_version,
                            node.metadata.labels
                        );
                        // Get labels we have stored for this node if present
                        let node_name = node.metadata.name.clone().unwrap_or_default();
                        match Self::get_config_map_labels_for_node_name(
                            &node_name,
                            self.client.clone(),
                            &self.namespace,
                        )
                        .await
                        {
                            Ok(Some(stored_labels)) => {
                                // Proceed only if the node's label_version is lower than the
                                // label_version in stored_labels
                                if let Some(node_label_version) = node
                                    .metadata
                                    .labels
                                    .clone()
                                    .unwrap_or_default()
                                    .get("label_version")
                                {
                                    if let Some(stored_label_version) =
                                        stored_labels.get("label_version")
                                    {
                                        if stored_label_version.parse::<u64>().unwrap_or(0)
                                            <= node_label_version.parse::<u64>().unwrap_or(0)
                                        {
                                            return Ok(());
                                        }
                                    }
                                }
                                // If the node is missing one of these labels, set it on the node
                                let node_labels = node.metadata.labels.clone().unwrap_or_default();
                                let mut new_labels = node_labels.clone();

                                for (key, value) in stored_labels {
                                    if !node_labels.contains_key(&key) {
                                        new_labels.insert(key, value);
                                    }
                                }
                                if new_labels.len() > node_labels.len() {
                                    let patch = json!({ "metadata": { "labels": new_labels }});
                                    let nodes: Api<Node> = Api::all(self.client.clone());
                                    nodes
                                        .patch(
                                            &node_name,
                                            &PatchParams::default(),
                                            &Patch::Merge(&patch),
                                        )
                                        .await
                                        .map_err(|e| {
                                            watcher::Error::WatchError(ErrorResponse {
                                                status: e.to_string(),
                                                message: format!(
                                                    "Failed to patch node {}: {}",
                                                    node_name, e
                                                ),
                                                reason: "Failed to patch node".to_string(),
                                                code: 500,
                                            })
                                        })?;
                                }
                            }
                            Ok(None) => {
                                println!("No stored labels found for node: {}", node_name);
                            }
                            Err(e) => {
                                return Err(watcher::Error::WatchError(ErrorResponse {
                                    status: e.to_string(),
                                    message: format!(
                                        "Failed to get stored labels for node {}: {}",
                                        node_name, e
                                    ),
                                    reason: "Failed to get stored labels".to_string(),
                                    code: 500,
                                }));
                            }
                        }
                    }
                    Event::Deleted(node) => {
                        println!(
                            "Node Deleted: {:?}, resource_version: {:?}, labels: {:?}",
                            node.metadata.name,
                            node.metadata.resource_version,
                            node.metadata.labels
                        );
                        let config_maps: Api<ConfigMap> =
                            Api::namespaced(self.client.clone(), &self.namespace);
                        if let Some(node_labels) = &node.metadata.labels {
                            // TODO: No node name should be an error
                            let node_name = node.metadata.name.unwrap_or_default();

                            match config_maps.get(&node_name).await {
                                Ok(_) => {
                                    // Update the stored labels if they exist for this node
                                    let mut node_labels = node_labels.clone();
                                    // Get the exisitng string and increment it by 1
                                    let label_version = node_labels
                                        .get("label_version")
                                        .unwrap_or(&"0".to_string())
                                        .parse::<u32>()
                                        .unwrap_or(0)
                                        + 1;
                                    node_labels.insert(
                                        "label_version".to_string(),
                                        label_version.to_string(),
                                    );
                                    config_maps
                                        .patch(
                                            &node_name,
                                            &PatchParams::default(),
                                            &Patch::Merge(&json!({ "data": node_labels })),
                                        )
                                        .await
                                        .map_err(|e| {
                                            let error_response = ErrorResponse {
                                                status: e.to_string(),
                                                message: format!(
                                                    "Failed to update config map for node {}: {}",
                                                    node_name, e
                                                ),
                                                reason: "Failed to update config map".to_string(),
                                                code: 500,
                                            };
                                            watcher::Error::WatchError(error_response)
                                        })?;
                                }
                                Err(_) => {
                                    // Create the stored labels if they don't exist for this node
                                    let mut node_labels = node_labels.clone();
                                    node_labels
                                        .insert("label_version".to_string(), "1".to_string());
                                    let data = ConfigMap {
                                        data: Some(node_labels),
                                        metadata: ObjectMeta {
                                            name: Some(node_name.clone()),
                                            ..Default::default()
                                        },
                                        ..Default::default()
                                    };
                                    config_maps
                                        .create(&Default::default(), &data)
                                        .await
                                        .map_err(|e| {
                                            // propagate the error
                                            let error_response = ErrorResponse {
                                                status: e.to_string(),
                                                message: format!(
                                                    "Failed to create config map for node {}: {}",
                                                    node_name, e
                                                ),
                                                reason: "Failed to create config map".to_string(),
                                                code: 500,
                                            };
                                            watcher::Error::WatchError(error_response)
                                        })?;
                                }
                            }
                        }
                    }
                    _ => {}
                }
                Ok(())
            })
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // System
    use std::collections::BTreeMap;

    // Third Party
    use k8s_openapi::api::core::v1::Node;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::PostParams;
    use kube::{api::Api, Client};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    // Local
    use super::NodeStateRestorer;

    async fn assert_stored_label_has_value(
        node_name: &str,
        key: &str,
        value: &str,
        client: Client,
    ) {
        let stored_labels = NodeStateRestorer::get_config_map_labels_for_node_name(
            node_name,
            client.clone(),
            "default",
        )
        .await
        .unwrap();
        assert_eq!(stored_labels.unwrap()[key], value);
    }

    async fn assert_node_label_has_value(node_name: &str, key: &str, value: &str, client: Client) {
        let nodes: Api<Node> = Api::all(client.clone());
        let node = nodes.get(node_name).await.unwrap();
        assert_eq!(node.metadata.labels.unwrap()[key], value.to_string());
    }

    #[tokio::test]
    // TODO: How do I remove the usage of sleeps?
    async fn test_add_and_remove_nodes() {
        let client = Client::try_default().await.unwrap();
        let test_node_name = "node4";
        println!(
            "{}: {:?}",
            test_node_name,
            NodeStateRestorer::get_config_map_labels_for_node_name(
                test_node_name,
                client.clone(),
                "default"
            )
            .await
            .unwrap()
        );
        let nodes: Api<Node> = Api::all(client.clone());

        let node_watcher = NodeStateRestorer::new("default").await.unwrap();
        tokio::spawn(async move {
            node_watcher.watch_nodes().await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // TODO: Do this for multiple nodes
        let node_label_key = "label_to_persist";
        let node_label_value: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        println!("The node label should be set to: {}", node_label_value);

        // Add nodes
        let node = Node {
            metadata: ObjectMeta {
                name: Some(test_node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let node = nodes.create(&PostParams::default(), &node).await.unwrap();
        assert_eq!(node.metadata.labels, None);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // Check if nodes were added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));

        let mut new_labels = BTreeMap::new();
        new_labels.insert(node_label_key.to_string(), node_label_value.clone());
        NodeStateRestorer::set_node_labels(test_node_name, client.clone(), new_labels)
            .await
            .unwrap();
        assert_node_label_has_value(
            test_node_name,
            node_label_key,
            &node_label_value,
            client.clone(),
        )
        .await;

        // Remove node
        nodes
            .delete(test_node_name, &Default::default())
            .await
            .unwrap();
        assert!(!nodes
            .list(&Default::default())
            .await
            .unwrap()
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));

        // Add back nodes and check that the labels are restored
        let node = Node {
            metadata: ObjectMeta {
                name: Some(test_node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let node = nodes.create(&PostParams::default(), &node).await.unwrap();
        assert_eq!(node.metadata.labels, None);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert_stored_label_has_value(
            test_node_name,
            node_label_key,
            &node_label_value,
            client.clone(),
        )
        .await;
        assert_node_label_has_value(
            test_node_name,
            node_label_key,
            &node_label_value,
            client.clone(),
        )
        .await;

        // Cleanup
        nodes
            .delete(test_node_name, &Default::default())
            .await
            .unwrap();
        assert!(!nodes
            .list(&Default::default())
            .await
            .unwrap()
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));
    }
}
