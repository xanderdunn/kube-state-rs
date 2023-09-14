// System
use std::collections::BTreeMap;

// Third Party
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Node};
use kube::{
    api::{Api, ListParams, ObjectMeta, Patch, PatchParams},
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

    pub async fn get_labels_for_node_name(
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

    pub async fn watch_nodes(&self) -> Result<(), watcher::Error> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let watcher = watcher(nodes, watcher::Config::default());

        watcher
            .try_for_each(|event| async move {
                match event {
                    Event::Applied(node) => {
                        // This event is triggered when a node is either
                        // added or modified.
                        // TODO: How do I tell the difference between an added node and a modified node?
                        println!(
                            "Node Added/Modified: {:?}, resource_version: {:?}, labels: {:?}",
                            node.metadata.name,
                            node.metadata.resource_version,
                            node.metadata.labels
                        );
                        let config_maps: Api<ConfigMap> =
                            Api::namespaced(self.client.clone(), &self.namespace);
                        // TODO: Get labels we have stored for this node if present
                        // TODO: If the node is missing one of these labels, apply it
                    }
                    Event::Deleted(node) => {
                        println!("Node Deleted: {:?}", node.metadata.name);
                        let config_maps: Api<ConfigMap> =
                            Api::namespaced(self.client.clone(), &self.namespace);
                        if let Some(labels) = &node.metadata.labels {
                            // TODO: No node name should be an error
                            let node_name = node.metadata.name.unwrap_or_default();
                            let mut config_map_data: BTreeMap<String, String> =
                                labels.clone().into_iter().collect();
                            config_map_data.insert(
                                "resource_version".to_string(),
                                node.metadata.resource_version.clone().unwrap_or_default(),
                            );

                            match config_maps.get(&node_name).await {
                                Ok(_) => {
                                    // Update the stored labels if they exist for this node
                                    config_maps
                                        .patch(
                                            &node_name,
                                            &PatchParams::default(),
                                            &Patch::Merge(&json!({ "data": config_map_data })),
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
                                    let data = ConfigMap {
                                        data: Some(config_map_data),
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
    // Third Party
    use k8s_openapi::api::core::v1::Node;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::PostParams;
    use kube::{api::Api, Client};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    // Local
    use super::NodeStateRestorer;

    #[tokio::test]
    async fn test_add_and_remove_nodes() {
        let client = Client::try_default().await.unwrap();
        let test_node_name = "node1";
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
        let node_label: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        println!("node_label: {}", node_label);

        // Add nodes
        let node = Node {
            metadata: ObjectMeta {
                name: Some(test_node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let node = nodes.create(&PostParams::default(), &node).await.unwrap();

        // Check if nodes were added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));

        // TODO: Add some labels to the node

        // Remove nodes
        nodes
            .delete(test_node_name, &Default::default())
            .await
            .unwrap();

        // Check if nodes were removed
        let node_list_after_deletion = nodes.list(&Default::default()).await.unwrap();
        assert!(!node_list_after_deletion
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));

        // TODO: Modify labels and nodes, then remove them, then restore them, and check that the
        // labels are restored properly

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
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

        // TODO: Add the node back
        // TODO: Restore labels
        // TODO: Check if the label is still there
    }
}
