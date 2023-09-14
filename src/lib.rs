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
}

impl NodeStateRestorer {
    pub async fn new() -> Result<Self, kube::Error> {
        let client = Client::try_default().await?;
        Ok(NodeStateRestorer { client })
    }

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

    pub async fn watch_nodes(&self) -> Result<(), watcher::Error> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let watcher = watcher(nodes, watcher::Config::default());
        let config_map_name_space = "default";

        watcher
            .try_for_each(|event| async move {
                match event {
                    Event::Applied(node) => {
                        // TODO: How do I tell the difference between an added node and a modified node?
                        println!("Node Added/Modified: {:?}", node.metadata.name);
                        println!("here1");
                        let config_maps: Api<ConfigMap> =
                            Api::namespaced(self.client.clone(), config_map_name_space);
                        println!("here2");
                        // TODO: Restore the labels to the node
                    }
                    Event::Deleted(node) => {
                        println!("Node Deleted: {:?}", node.metadata.name);
                        let config_maps: Api<ConfigMap> =
                            Api::namespaced(self.client.clone(), config_map_name_space.clone());
                        if let Some(labels) = &node.metadata.labels {
                            println!("here3");
                            // TODO: No node name should be an error
                            let node_name = node.metadata.name.unwrap_or_default();
                            let config_map_data: BTreeMap<String, String> =
                                labels.clone().into_iter().collect();
                            println!("here4");

                            match config_maps.get(&node_name).await {
                                Ok(_) => {
                                    // TODO
                                    // Update the stored labels if they exist for this node
                                    //println!("here5");
                                    //let patch = Patch::Apply(json!({ "value": config_map_data }));
                                    //config_maps
                                    //.patch(&node_name, &PatchParams::default(), &patch)
                                    //.await
                                    //.map_err(|e| {
                                    //// propagate the error
                                    //let error_response = ErrorResponse {
                                    //status: e.to_string(),
                                    //message: format!(
                                    //"Failed to patch config map for node {}: {}",
                                    //node_name, e
                                    //),
                                    //reason: "Failed to patch config map".to_string(),
                                    //code: 500,
                                    //};
                                    //watcher::Error::WatchError(error_response)
                                    //})?;
                                }
                                Err(_) => {
                                    // Create the stored labels if they don't exist for this node
                                    println!("here6");
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
        NodeStateRestorer::print_all_config_map_data(client.clone(), "default")
            .await
            .unwrap();
        let nodes: Api<Node> = Api::all(client.clone());

        let node_watcher = NodeStateRestorer::new().await.unwrap();
        tokio::spawn(async move {
            node_watcher.watch_nodes().await.unwrap();
        });

        // TODO: Do this for multiple nodes
        let node_to_add = "node1";
        let node_label: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        println!("node_label: {}", node_label);

        // Add nodes
        let node = Node {
            metadata: ObjectMeta {
                name: Some(node_to_add.to_string()),
                labels: Some({
                    let mut labels = std::collections::BTreeMap::new();
                    labels.insert("label_to_persist".to_string(), node_label);
                    labels
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        nodes.create(&PostParams::default(), &node).await.unwrap();
        // TODO: Store node labels

        // Check if nodes were added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_to_add.to_string())));

        // Remove nodes
        nodes
            .delete(node_to_add, &Default::default())
            .await
            .unwrap();

        // Check if nodes were removed
        let node_list_after_deletion = nodes.list(&Default::default()).await.unwrap();
        assert!(!node_list_after_deletion
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_to_add.to_string())));

        // TODO: Modify labels and nodes, then remove them, then restore them, and check that the
        // labels are restored properly

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        NodeStateRestorer::print_all_config_map_data(client.clone(), "default")
            .await
            .unwrap();

        // TODO: Add the node back
        // TODO: Restore labels
        // TODO: Check if the label is still there
    }
}
