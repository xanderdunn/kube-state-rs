// System
use std::collections::BTreeMap;

// Third Party
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Node};
use kube::{
    api::{
        Api, DeleteParams, ListParams, ObjectMeta, PartialObjectMetaExt, Patch, PatchParams,
        PostParams,
    },
    runtime::{watcher, watcher::Event},
    Client,
};
use serde_json::json;
use tracing::debug;

// Local
mod utils;
use utils::generate_error_response;

/// This service listens to all Kubernetes node events and will:
/// - Save all node metadata labels when a node is deleted.
/// - Restore all node metadata labels when a node is added back to the cluster with the same name.
/// This service uniquely identifies nodes by name.
pub struct NodeLabelPersistenceService {
    client: Client,
    namespace: String,
}

impl NodeLabelPersistenceService {
    pub async fn new(namespace: &str, client: Client) -> Result<Self, kube::Error> {
        Ok(NodeLabelPersistenceService {
            client,
            namespace: namespace.to_string(),
        })
    }

    /// A convenience class method that returns all labels stored in the ConfigMap for a given node
    /// name.
    pub async fn get_config_map_labels(
        client: Client,
        node_name: &str,
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

    /// A convenience class method to get all of the labels on a node.
    pub async fn get_node_labels(
        client: Client,
        node_name: &str,
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

    /// A convience class method to set label values on a node.
    pub async fn set_node_labels(
        client: Client,
        node_name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), kube::Error> {
        let nodes = Api::<Node>::all(client);

        let patch_params = PatchParams::apply("node-state-restorer");

        let metadata = ObjectMeta {
            labels: Some(labels),
            ..Default::default()
        }
        .into_request_partial::<Node>();

        nodes
            .patch_metadata(node_name, &patch_params, &Patch::Merge(metadata))
            .await?;
        Ok(())
    }

    /// A convenience class method to remove the label on a node.
    pub async fn remove_node_label(
        client: Client,
        node_name: &str,
        label_key: String,
    ) -> Result<(), kube::Error> {
        let nodes = Api::<Node>::all(client);

        let patch_params = PatchParams::apply("node-state-restorer");

        let patch = json!({
            "metadata": {
                "labels": {
                    label_key: serde_json::Value::Null
                }
            }
        });

        nodes
            .patch(node_name, &patch_params, &Patch::Merge(patch))
            .await?;
        Ok(())
    }

    /// Given a set of node labels and stored labels, restore the stored labels on the node.
    pub async fn restore_node_labels(
        client: Client,
        node_name: &str,
        node_labels: BTreeMap<String, String>,
        stored_labels: BTreeMap<String, String>,
    ) -> Result<(), watcher::Error> {
        let mut new_labels = node_labels.clone();

        for (key, value) in stored_labels {
            if !node_labels.contains_key(&key) {
                new_labels.insert(key, value);
            }
        }
        if new_labels.len() > node_labels.len() {
            let patch = json!({ "metadata": { "labels": new_labels }});
            let nodes: Api<Node> = Api::all(client.clone());
            nodes
                .patch(node_name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .map_err(|e| generate_error_response(Some(e), node_name, "Failed to patch node"))?;
        }
        Ok(())
    }

    /// Given a set of node labels, store them in the ConfigMap for the first time.
    async fn create_stored_labels(
        client: Client,
        node_name: &str,
        node_labels: BTreeMap<String, String>,
        namespace: &str,
    ) -> Result<(), watcher::Error> {
        let mut node_labels = node_labels.clone();
        node_labels.insert("label_version".to_string(), "1".to_string());
        let data = ConfigMap {
            data: Some(node_labels),
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);
        config_maps
            .create(&Default::default(), &data)
            .await
            .map_err(|e| {
                generate_error_response(Some(e), node_name, "Failed to create config map")
            })?;
        Ok(())
    }

    /// Given a set of node labels, update the stored labels in the `ConfigMap`.
    /// If the node_labels is non-empty, it will replace all labels in the `ConfigMap`
    /// If the node_labels is empty, it will delete the `ConfigMap` for this node.
    async fn update_stored_labels(
        client: Client,
        node_name: &str,
        node_labels: BTreeMap<String, String>,
        namespace: &str,
    ) -> Result<(), watcher::Error> {
        let mut node_labels = node_labels;
        let label_version = node_labels
            .get("label_version")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0)
            + 1;

        node_labels.insert("label_version".to_string(), label_version.to_string());

        let data = ConfigMap {
            data: Some(node_labels.clone()),
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);

        if node_labels.len() == 1 {
            // Only the label_version key is present
            config_maps
                .delete(node_name, &DeleteParams::default())
                .await
                .map_err(|e| {
                    generate_error_response(Some(e), node_name, "Failed to update config map")
                })?;
        } else {
            config_maps
                .replace(node_name, &PostParams::default(), &data)
                .await
                .map_err(|e| {
                    generate_error_response(Some(e), node_name, "Failed to update config map")
                })?;
        }

        Ok(())
    }

    /// Class method to handle a node being added or modified.
    /// When a node is modified, do nothing.
    /// When a node is added, restore any missing labels.
    pub async fn handle_node_applied(
        client: Client,
        node: Node,
        namespace: &str,
    ) -> Result<(), watcher::Error> {
        // This event is triggered when a node is either added or modified.
        debug!(
            "Node Added/Modified: {:?}, resource_version: {:?}, labels: {:?}",
            node.metadata.name, node.metadata.resource_version, node.metadata.labels
        );
        // Get labels we have stored for this node if present
        let node_name = {
            if let Some(node_name) = node.metadata.name.clone() {
                Ok(node_name)
            } else {
                Err(generate_error_response(None, "", "No node name found"))
            }
        }?;
        match Self::get_config_map_labels(client.clone(), &node_name, namespace).await {
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
                    if let Some(stored_label_version) = stored_labels.get("label_version") {
                        if stored_label_version.parse::<u64>().unwrap_or(0)
                            <= node_label_version.parse::<u64>().unwrap_or(0)
                        {
                            return Ok(());
                        }
                    }
                }

                // If the node is missing one of these labels, set it on the node
                let node_labels = node.metadata.labels.clone().unwrap_or_default();
                Self::restore_node_labels(client.clone(), &node_name, node_labels, stored_labels)
                    .await?;
            }
            Ok(None) => {
                debug!("No stored labels found for node: {}", node_name);
            }
            Err(e) => {
                return Err(generate_error_response(
                    Some(e),
                    &node_name,
                    "Failed to get sotred labels for node",
                ));
            }
        }
        Ok(())
    }

    /// Class method to handle a node being deleted.
    /// When a node is deleted, update the stored labels to reflect the current labels on the node.
    pub async fn handle_node_deleted(
        client: Client,
        node: Node,
        namespace: &str,
    ) -> Result<(), watcher::Error> {
        debug!(
            "Node Deleted: {:?}, resource_version: {:?}, labels: {:?}",
            node.metadata.name, node.metadata.resource_version, node.metadata.labels
        );
        if let Some(node_labels) = &node.metadata.labels {
            if let Some(node_name) = node.metadata.name {
                let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
                match config_maps.get(&node_name).await {
                    Ok(_) => {
                        // Update the stored labels if they exist for this node
                        Self::update_stored_labels(
                            client.clone(),
                            &node_name,
                            node_labels.clone(),
                            namespace,
                        )
                        .await?;
                    }
                    Err(_) => {
                        // Create the stored labels if they don't exist for this node
                        Self::create_stored_labels(
                            client.clone(),
                            &node_name,
                            node_labels.clone(),
                            namespace,
                        )
                        .await?;
                    }
                }
            } else {
                return Err(generate_error_response(None, "", "No node name found"));
            }
        }
        Ok(())
    }

    /// Start watching all node events and save and restore labels as needed.
    /// This will save all metadata labels for a node when it is deleted and increment the
    /// label_version.
    /// If the label_version of the node's metadata is greater than or equal to what's stored, nothing will be
    /// restored. This is to prevent restoring labels that are intentionally deleted on a running
    /// node.
    pub async fn watch_nodes(&self) -> Result<(), watcher::Error> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let watcher = watcher(nodes, watcher::Config::default());

        watcher
            .try_for_each(|event| async move {
                match event {
                    Event::Applied(node) => {
                        Self::handle_node_applied(self.client.clone(), node, &self.namespace)
                            .await?;
                    }
                    Event::Deleted(node) => {
                        Self::handle_node_deleted(self.client.clone(), node, &self.namespace)
                            .await?;
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
    use std::sync::Once;

    // Third Party
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::PostParams;
    use kube::{
        api::{Api, ListParams},
        Client,
    };
    use rand::{distributions::Alphanumeric, seq::IteratorRandom, thread_rng, Rng, SeedableRng};
    use serial_test::serial;
    use tracing::debug;
    use tracing_subscriber::prelude::*;

    // Local
    use super::NodeLabelPersistenceService;

    /// A convenience function for asserting that the stored value is correct.
    async fn assert_stored_label_has_value(
        client: Client,
        node_name: &str,
        key: &str,
        value: Option<&String>,
    ) {
        let stored_labels = NodeLabelPersistenceService::get_config_map_labels(
            client.clone(),
            node_name,
            "default",
        )
        .await
        .unwrap();
        assert_eq!(stored_labels.unwrap().get(key), value);
    }

    /// A convenience function for asserting that the node's label value is correct.
    async fn assert_node_label_has_value(
        client: Client,
        node_name: &str,
        key: &str,
        value: Option<&String>,
    ) {
        let nodes: Api<Node> = Api::all(client.clone());
        let node = nodes.get(node_name).await.unwrap();
        assert_eq!(node.metadata.labels.unwrap().get(key), value);
    }

    /// A convenience function to create a node by name.
    async fn add_node(client: Client, node_name: &str) -> Result<(), kube::Error> {
        let nodes: Api<Node> = Api::all(client.clone());

        let node = Node {
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let node = nodes.create(&PostParams::default(), &node).await?;
        assert_eq!(node.metadata.labels, None);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // Check if the node was added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_name.to_string())));
        Ok(())
    }

    /// A convenience function to delete a node by name.
    async fn delete_node(client: Client, node_name: &str) -> Result<(), kube::Error> {
        let nodes: Api<Node> = Api::all(client.clone());
        nodes.delete(node_name, &Default::default()).await?;
        assert!(!nodes
            .list(&Default::default())
            .await
            .unwrap()
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_name.to_string())));
        Ok(())
    }

    /// Set the value of the label on the node to a random string.
    async fn set_random_label(
        client: Client,
        node_name: &str,
        key: &str,
    ) -> Result<String, kube::Error> {
        let value: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let mut new_labels = BTreeMap::new();
        new_labels.insert(key.to_string(), value.clone());
        NodeLabelPersistenceService::set_node_labels(client.clone(), node_name, new_labels)
            .await
            .unwrap();
        assert_node_label_has_value(client.clone(), node_name, key, Some(&value)).await;
        Ok(value)
    }

    static INIT: Once = Once::new();

    /// Create the global tracing subscriber for tests only once so that multiple tests do not
    /// conflict.
    fn init_tracing() {
        INIT.call_once(|| {
            let filter = tracing_subscriber::filter::Targets::new()
                .with_target("kube_state_rs", tracing::Level::DEBUG);
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .with(filter)
                .init();
        });
    }

    #[tokio::test]
    #[serial]
    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Delete the node and assert that the label is stored
    /// 4. Add the node back to the cluster and assert that the label is restored
    async fn test_add_and_remove_nodes() {
        init_tracing();

        // Start our service
        let client = Client::try_default().await.unwrap();
        let node_watcher = NodeLabelPersistenceService::new("default", client.clone())
            .await
            .unwrap();
        tokio::spawn(async move {
            node_watcher.watch_nodes().await.unwrap();
        });
        // Make sure the Service is watching before proceeding
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        add_node(client.clone(), test_node_name).await.unwrap();

        //
        // 2. Add a label to the node
        //
        let node_label_key = "label_to_persist";
        let node_label_value = set_random_label(client.clone(), test_node_name, node_label_key)
            .await
            .unwrap();

        //
        // 3. Delete the node and assert that the label is stored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;

        //
        // 4. Add the node back to the cluster and assert that the label is restored
        //
        add_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
        assert_node_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;

        //
        // Cleanup
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    /// 1. Create a node
    /// 2. Add a label to the node.
    /// 3. Delete the node so that the state is stored
    /// 4. Add the node back to the cluster
    /// 5. Delete the label on the node
    /// 6. Delete the node so that the labels are stored and assert that deleted label is not in
    ///    the store.
    async fn test_deleting_labels() {
        init_tracing();

        // Start from a clean slate
        let namespace = "default";
        let client = Client::try_default().await.unwrap();
        delete_kube_state(client.clone(), namespace).await.unwrap();

        // Start our service
        let node_watcher = NodeLabelPersistenceService::new(namespace, client.clone())
            .await
            .unwrap();
        tokio::spawn(async move {
            node_watcher.watch_nodes().await.unwrap();
        });
        // Make sure the Service is watching before proceeding
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        add_node(client.clone(), test_node_name).await.unwrap();

        //
        // 2. Add a label to the node
        //
        let node_label_key = "label_to_persist";
        let node_label_value = set_random_label(client.clone(), test_node_name, node_label_key)
            .await
            .unwrap();

        //
        // 3. Delete the node and assert that the label is stored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;

        //
        // 4. Add the node back to the cluster
        //
        add_node(client.clone(), test_node_name).await.unwrap();

        //
        // 5. Delete the label on the node
        //
        NodeLabelPersistenceService::remove_node_label(
            client.clone(),
            test_node_name,
            node_label_key.to_string(),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_node_label_has_value(client.clone(), test_node_name, node_label_key, None).await;

        //
        // 6. Delete the node so that the labels are stored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        //assert_stored_label_has_value(test_node_name, node_label_key, None, client.clone()).await;

        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);
        let config_maps_list = config_maps.list(&ListParams::default()).await.unwrap();
        for config_map in config_maps_list.items.clone().into_iter() {
            if let Some(name) = config_map.metadata.name {
                assert_ne!(name, test_node_name);
            }
        }
        assert_eq!(config_maps_list.items.len(), 1); // There is always a root certificate ConfigMap
    }

    /// Delete all nodes in the cluster.
    async fn delete_all_nodes(client: Client) -> Result<(), kube::Error> {
        let nodes: Api<Node> = Api::all(client.clone());
        let nodes_list = nodes.list(&ListParams::default()).await?;
        for node in nodes_list {
            let node_name = node.metadata.name.unwrap();
            delete_node(client.clone(), &node_name).await?;
        }
        Ok(())
    }

    /// Delete all nodes and delete all ConfMap data.
    /// Run this between unit tests to start from a clean state.
    async fn delete_kube_state(client: Client, namespace: &str) -> Result<(), kube::Error> {
        delete_all_nodes(client.clone()).await?;

        let config_maps: Api<k8s_openapi::api::core::v1::ConfigMap> =
            Api::namespaced(client.clone(), namespace);
        let config_map_list = config_maps.list(&Default::default()).await.unwrap();
        for config_map in config_map_list.items {
            config_maps
                .delete(&config_map.metadata.name.unwrap(), &Default::default())
                .await?;
        }
        Ok(())
    }

    #[derive(Debug)]
    enum NodeAction {
        Create,
        AddLabelTo,
        DeleteLabelFrom,
        ChangeValueOfLabelOn,
        Delete,
    }

    #[tokio::test]
    #[serial]
    /// This test takes random actions and asserts that the state is as expected.
    /// This serves as differential fuzzing: the book-keeping has been re-implemented and we check
    /// this implementation against the NodeLabelPersistenceService implementation.
    async fn fuzz_test() {
        init_tracing();

        // Start from a clean slate
        let namespace = "default";
        let client = Client::try_default().await.unwrap();
        delete_kube_state(client.clone(), namespace).await.unwrap(); // start with a clean slate
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Start our service
        let node_watcher = NodeLabelPersistenceService::new(namespace, client.clone())
            .await
            .unwrap();
        tokio::spawn(async move {
            node_watcher.watch_nodes().await.unwrap();
        });
        // Make sure the Service is watching before proceeding
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let num_steps = 50;
        // node_name -> (in_cluster, label_key -> label_value)
        // A node is `in_cluster` if it is not deleted
        let mut truth_node_labels: BTreeMap<String, (bool, BTreeMap<String, String>)> =
            BTreeMap::new();
        let mut rng = rand::thread_rng();
        let seed = rng.gen::<u64>();
        debug!("The seed is {}", seed);
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        for _ in 0..num_steps {
            // Take a random action: Create node, add label to node, delete label from node, change
            // value of label on node, delete node.
            let choice = rng.gen_range(0..10);
            // Skewed choice, preferring to add rather than delete
            let action = match choice {
                0 => NodeAction::Create,
                1 => NodeAction::Create,
                2 => NodeAction::Create,
                3 => NodeAction::AddLabelTo,
                4 => NodeAction::AddLabelTo,
                5 => NodeAction::AddLabelTo,
                6 => NodeAction::DeleteLabelFrom,
                7 => NodeAction::ChangeValueOfLabelOn,
                8 => NodeAction::ChangeValueOfLabelOn,
                9 => NodeAction::Delete,
                _ => panic!("Invalid choice"),
            };

            match action {
                NodeAction::Create => {
                    let node_name: String = rng
                        .clone()
                        .sample_iter(&Alphanumeric)
                        .take(10)
                        .map(char::from)
                        .collect();
                    let node_name = node_name.to_lowercase();
                    truth_node_labels.insert(node_name.clone(), (true, BTreeMap::new()));
                    add_node(client.clone(), &node_name).await.unwrap();
                    debug!("Action Completed: added node {}", node_name);
                }
                NodeAction::AddLabelTo => {
                    // Randomly choose a key from truth_node_labels
                    let node_name = truth_node_labels.keys().choose(&mut rng).cloned();
                    if let Some(node_name) = node_name {
                        let label_key = rng
                            .clone()
                            .sample_iter(&Alphanumeric)
                            .take(10)
                            .map(char::from)
                            .collect::<String>();
                        let label_value = rng
                            .clone()
                            .sample_iter(&Alphanumeric)
                            .take(10)
                            .map(char::from)
                            .collect::<String>();
                        let (in_cluster, mut labels) = truth_node_labels[&node_name].clone();
                        if in_cluster {
                            // can't add label to a deleted node
                            labels.insert(label_key, label_value);
                            truth_node_labels
                                .insert(node_name.clone(), (in_cluster, labels.clone()));
                            debug!("Action Completed: added label on node {}", node_name);
                            NodeLabelPersistenceService::set_node_labels(
                                client.clone(),
                                &node_name,
                                labels,
                            )
                            .await
                            .unwrap();
                        }
                    }
                }
                NodeAction::DeleteLabelFrom => {
                    // Randomly choose a key from truth_node_labels
                    let node_name = truth_node_labels.keys().choose(&mut rng).cloned();
                    if let Some(node_name) = node_name {
                        // Randomly choose a label from the node
                        let label_key = truth_node_labels
                            .get(&node_name)
                            .unwrap()
                            .1
                            .keys()
                            .choose(&mut rng)
                            .cloned();
                        if let Some(label_key) = label_key {
                            let (in_cluster, mut labels) = truth_node_labels[&node_name].clone();
                            if in_cluster {
                                // can't delete label from a deleted node
                                labels.remove(&label_key);
                                truth_node_labels
                                    .insert(node_name.clone(), (in_cluster, labels.clone()));
                                NodeLabelPersistenceService::remove_node_label(
                                    client.clone(),
                                    &node_name,
                                    label_key.to_string(),
                                )
                                .await
                                .unwrap();
                                debug!(
                                    "Action Completed: deleted label {} on node {}",
                                    label_key, node_name
                                );
                            }
                        }
                    }
                }
                NodeAction::ChangeValueOfLabelOn => {
                    // Randomly choose a key from truth_node_labels
                    let node_name = truth_node_labels.keys().choose(&mut rng).cloned();
                    if let Some(node_name) = node_name {
                        // Randomly choose a label from the node
                        let label_key = truth_node_labels
                            .get(&node_name)
                            .unwrap()
                            .1
                            .keys()
                            .choose(&mut rng)
                            .cloned();
                        if let Some(label_key) = label_key {
                            let label_value = rng
                                .clone()
                                .sample_iter(&Alphanumeric)
                                .take(10)
                                .map(char::from)
                                .collect::<String>();
                            let (in_cluster, mut labels) = truth_node_labels[&node_name].clone();
                            if in_cluster {
                                // can't change label on a deleted node
                                labels.insert(label_key, label_value);
                                truth_node_labels
                                    .insert(node_name.clone(), (in_cluster, labels.clone()));
                                debug!("Action Completed: changed label on node {}", node_name);
                                NodeLabelPersistenceService::set_node_labels(
                                    client.clone(),
                                    &node_name,
                                    labels,
                                )
                                .await
                                .unwrap();
                            }
                        }
                    }
                }
                NodeAction::Delete => {
                    let node_name = truth_node_labels.keys().choose(&mut rng).cloned();
                    if let Some(node_name) = node_name {
                        let (in_cluster, labels) = truth_node_labels[&node_name].clone();
                        if in_cluster {
                            // can't delete a deleted node
                            truth_node_labels.insert(node_name.clone(), (false, labels.clone()));
                            delete_node(client.clone(), &node_name).await.unwrap();
                            debug!("Action Completed: deleted node {}", node_name);
                        }
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let nodes: Api<Node> = Api::all(client.clone());
        let nodes_list = nodes.list(&ListParams::default()).await.unwrap();
        let num_nodes = nodes_list.items.len();
        let num_truth_nodes_in_cluster = truth_node_labels
            .iter()
            .filter(|(_, (in_cluster, _))| *in_cluster)
            .count();
        debug!("Finished fuzz test with {} nodes", num_nodes);
        debug!("Truth labels: {:?}", truth_node_labels);
        for node in nodes_list.items.clone().into_iter() {
            debug!("Node in cluster: {:?}", node.metadata.name.unwrap());
        }
        assert_eq!(num_nodes, num_truth_nodes_in_cluster);
        // Iterate through the nodes and assert that their labels are correct
        for node in nodes_list {
            let node_name = node.metadata.name.unwrap();
            let truth_labels = truth_node_labels.get(&node_name).unwrap();
            if let Some(node_labels) = node.metadata.labels {
                assert_eq!(node_labels, truth_labels.1);
            } else {
                assert_eq!(
                    truth_node_labels.get(&node_name).unwrap().1.len(),
                    0,
                    "Node {} has no labels but truth is expecting {:?}",
                    node_name,
                    truth_labels
                );
            }
        }

        // All labels should be stored when the nodes are deleted:
        delete_all_nodes(client.clone()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Iterate through the configmaps and assert that the values are correct
        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);
        let config_maps_list = config_maps.list(&ListParams::default()).await.unwrap();
        let num_config_maps = config_maps_list.items.len();
        let num_truth_nodes_with_labels = truth_node_labels
            .iter()
            .filter(|(_, (_, labels))| !labels.is_empty())
            .count();
        debug!(
            "Finished fuzz test with {} config maps, {} truth_nodes",
            num_config_maps, num_truth_nodes_with_labels
        );
        for config_map in config_maps_list.items.clone().into_iter() {
            if let Some(config_map_labels) = config_map.data {
                debug!(
                    "ConfigMap {} has labels {:?}",
                    config_map.metadata.name.unwrap(),
                    config_map_labels
                );
            }
        }
        debug!("Truth labels: {:?}", truth_node_labels);
        assert_eq!(num_config_maps, num_truth_nodes_with_labels + 1);
        for config_map in config_maps_list {
            let config_map_name = config_map.metadata.name.unwrap();
            if config_map_name != "kube-root-ca.crt" {
                let truth_labels = truth_node_labels.get(&config_map_name).unwrap();
                if let Some(config_map_labels) = config_map.data {
                    let mut config_map_labels = config_map_labels.clone();
                    config_map_labels.remove("label_version");
                    assert_eq!(
                        config_map_labels, truth_labels.1,
                        "ConfigMap {} has labels {:?} but truth is expecting {:?}",
                        config_map_name, config_map_labels, truth_labels
                    );
                } else {
                    assert_eq!(
                        truth_node_labels.get(&config_map_name).unwrap().1.len(),
                        0,
                        "ConfigMap {} has no labels but truth is expecting {:?}",
                        config_map_name,
                        truth_labels
                    );
                }
            }
        }
    }
}
