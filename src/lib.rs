// System
use std::collections::BTreeMap;

// Third Party
use anyhow::anyhow;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::{ConfigMap, Node};
use kube::{
    api::{
        Api, DeleteParams, ObjectMeta, PartialObjectMetaExt, Patch, PatchParams, PostParams,
        WatchEvent, WatchParams,
    },
    Client,
};
use serde_json::json;
use tracing::{debug, error, info};

// Local
pub mod utils;

/// The special token reserved to encode `/` in label keys so that they can be stored as ConfigMap
/// keys.
const SLASH_TOKEN: &str = "-SLASH-";

/// This service listens to all Kubernetes node events and will:
/// - Save all node metadata labels when a node is deleted.
/// - Restore all node metadata labels when a node is added back to the cluster with the same name.
/// This service uniquely identifies nodes by name.
pub struct NodeLabelPersistenceService {
    client: Client,
    namespace: String,
}

impl NodeLabelPersistenceService {
    pub async fn new(namespace: &str, client: &Client) -> Result<Self, anyhow::Error> {
        Ok(NodeLabelPersistenceService {
            client: client.clone(),
            namespace: namespace.to_string(),
        })
    }

    /// As a workaround for Kubernetes ConfigMap key restrictions, we replace all forward slashes in
    /// keys with SLASH_TOKEN.
    /// This function either encodes or decodes all keys in a given map.
    fn code_key_slashes(node_labels: &mut BTreeMap<String, String>, encode: bool) {
        let (from, to) = if encode {
            ("/", SLASH_TOKEN)
        } else {
            (SLASH_TOKEN, "/")
        };
        node_labels
            .keys()
            .cloned()
            .collect::<Vec<String>>()
            .into_iter()
            .for_each(|key| {
                if let Some(value) = node_labels.remove(&key) {
                    node_labels.insert(key.replace(from, to), value);
                }
            });
    }

    /// A convenience class method that returns all labels stored in the ConfigMap for a given node
    /// name.
    pub async fn get_config_map_labels(
        client: &Client,
        node_name: &str,
        namespace: &str,
    ) -> Result<Option<BTreeMap<String, String>>, anyhow::Error> {
        let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

        match config_maps.get(node_name).await {
            Ok(config_map) => match config_map.data {
                Some(config_map_data) => {
                    let mut config_map_data = config_map_data.clone();
                    Self::code_key_slashes(&mut config_map_data, false);
                    Ok(Some(config_map_data))
                }
                None => Ok(None),
            },
            Err(_) => Ok(None),
        }
    }

    /// A convenience class method to get all of the labels on a node.
    pub async fn get_node_labels(
        client: &Client,
        node_name: &str,
    ) -> Result<Option<BTreeMap<String, String>>, anyhow::Error> {
        let nodes: Api<Node> = Api::all(client.clone());

        match nodes.get(node_name).await {
            Ok(node) => Ok(node.metadata.labels),
            Err(_) => Ok(None),
        }
    }

    /// A convenience class method to set label values on a node.
    pub async fn set_node_labels(
        client: &Client,
        node_name: &str,
        labels: &BTreeMap<String, String>,
        service_name: &str,
    ) -> Result<(), anyhow::Error> {
        let nodes = Api::<Node>::all(client.clone());

        let patch_params = PatchParams::apply(service_name);

        let metadata = ObjectMeta {
            labels: Some(labels.clone()),
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
        client: &Client,
        node_name: &str,
        label_key: &str,
        service_name: &str,
    ) -> Result<(), anyhow::Error> {
        let nodes = Api::<Node>::all(client.clone());

        let patch_params = PatchParams::apply(service_name);

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
    async fn restore_node_labels(
        client: &Client,
        node_name: &str,
        node_labels: &BTreeMap<String, String>,
        stored_labels: &BTreeMap<String, String>,
    ) -> Result<(), anyhow::Error> {
        let mut new_labels = node_labels.clone();

        // Add missing keys
        for (key, value) in stored_labels {
            new_labels
                .entry(key.clone().replace(SLASH_TOKEN, "/")) // Decode keys with slashes
                .or_insert(value.clone());
        }

        if new_labels.len() > node_labels.len() {
            let patch = json!({ "metadata": { "labels": new_labels }});
            let nodes: Api<Node> = Api::all(client.clone());
            nodes
                .patch(node_name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .map_err(|e| anyhow!(e).context("Failed to patch node"))?;
        }
        Ok(())
    }

    /// Given a set of node labels, store them in the ConfigMap for the first time.
    async fn create_stored_labels(
        client: &Client,
        node_name: &str,
        node_labels: &BTreeMap<String, String>,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        let mut node_labels = node_labels.clone();
        Self::code_key_slashes(&mut node_labels, true);
        let data = ConfigMap {
            data: Some(node_labels),
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        config_maps
            .create(&Default::default(), &data)
            .await
            .map_err(|e| anyhow!(e).context("Failed to create config map"))?;
        Ok(())
    }

    /// Given a set of node labels, update the stored labels in the `ConfigMap`.
    /// It will replace all labels in the `ConfigMap`
    /// `node_labels` must be non-empty.
    async fn update_stored_labels(
        client: &Client,
        node_name: &str,
        node_labels: &BTreeMap<String, String>,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        assert!(!node_labels.is_empty());
        let mut node_labels = node_labels.clone();

        let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        Self::code_key_slashes(&mut node_labels, true);
        let data = ConfigMap {
            data: Some(node_labels.clone()),
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        config_maps
            .replace(node_name, &PostParams::default(), &data)
            .await
            .map_err(|e| anyhow!(e).context("Failed to update config map"))?;

        Ok(())
    }

    /// Class method to handle a node being added or modified.
    /// When a node is modified, do nothing.
    /// When a node is added, restore any missing labels.
    async fn handle_node_applied(
        client: &Client,
        node: &Node,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        // This event is triggered when a node is either added or modified.
        debug!(
            "Node Added: {:?}, resource_version: {:?}, labels: {:?}",
            node.metadata.name, node.metadata.resource_version, node.metadata.labels
        );
        // Get labels we have stored for this node if present
        let node_name = {
            if let Some(node_name) = node.metadata.name.clone() {
                Ok(node_name)
            } else {
                Err(anyhow!("No node name found"))
            }
        }?;
        match Self::get_config_map_labels(client, &node_name, namespace).await {
            Ok(Some(stored_labels)) => {
                // If the node is missing one of these labels, set it on the node
                let node_labels = node.metadata.labels.clone().unwrap_or_default();
                Self::restore_node_labels(client, &node_name, &node_labels, &stored_labels).await?;
            }
            Ok(None) => {
                debug!("No stored labels found for node: {}", node_name);
            }
            Err(e) => {
                return Err(anyhow!(e).context("Failed to get stored labels"));
            }
        }
        Ok(())
    }

    /// Class method to handle a node being deleted.
    /// When a node is deleted, update the stored labels to reflect the current labels on the node.
    async fn handle_node_deleted(
        client: &Client,
        node: &Node,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        debug!(
            "Node Deleted: {:?}, resource_version: {:?}, labels: {:?}",
            node.metadata.name, node.metadata.resource_version, node.metadata.labels
        );
        let node_name: String = {
            if let Some(node_name) = node.metadata.name.clone() {
                node_name
            } else {
                return Err(anyhow!("No node name found"));
            }
        };
        let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        if let Some(node_labels) = &node.metadata.labels {
            match config_maps.get(&node_name).await {
                Ok(_) => {
                    // Update the stored labels if they exist for this node
                    Self::update_stored_labels(client, &node_name, node_labels, namespace).await?;
                }
                Err(_) => {
                    // Create the stored labels if they don't exist for this node
                    Self::create_stored_labels(client, &node_name, node_labels, namespace).await?;
                }
            }
        } else {
            // There are no labels left, so delete the corresponding ConfigMap if it exists
            if (config_maps.get(&node_name).await).is_ok() {
                config_maps
                    .delete(&node_name, &DeleteParams::default())
                    .await
                    .map_err(|e| anyhow!(e).context("Failed to update config map"))?;
            }
        }
        Ok(())
    }

    /// Start watching all node events and save and restore labels as needed.
    /// This will save all metadata labels for a node when it is deleted.
    pub async fn watch_nodes(&self) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(self.client.clone());

        info!("Starting node label watcher...");
        let mut stream = nodes.watch(&WatchParams::default(), "0").await?.boxed();
        while let Some(status) = stream.try_next().await? {
            match status {
                WatchEvent::Added(node) => {
                    Self::handle_node_applied(&self.client, &node, &self.namespace)
                        .await
                        .map_err(|e| error!("handle_node_applied failed: {:?}", e))
                        .ok();
                }
                WatchEvent::Deleted(node) => {
                    Self::handle_node_deleted(&self.client, &node, &self.namespace)
                        .await
                        .map_err(|e| error!("handle_node_deleted failed: {:?}", e))
                        .ok();
                }
                _ => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // System
    use std::collections::BTreeMap;

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

    // Local
    use super::{utils::init_tracing, NodeLabelPersistenceService};

    /// A convenience function for asserting that the stored value is correct.
    async fn assert_stored_label_has_value(
        client: Client,
        node_name: &str,
        key: &str,
        value: Option<&String>,
    ) {
        let stored_labels =
            NodeLabelPersistenceService::get_config_map_labels(&client, node_name, "default")
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
    async fn add_node(client: Client, node_name: &str) -> Result<(), anyhow::Error> {
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
    async fn delete_node(client: Client, node_name: &str) -> Result<(), anyhow::Error> {
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
        service_name: &str,
    ) -> Result<String, anyhow::Error> {
        let value: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let mut new_labels = BTreeMap::new();
        new_labels.insert(key.to_string(), value.clone());
        NodeLabelPersistenceService::set_node_labels(&client, node_name, &new_labels, service_name)
            .await
            .unwrap();
        assert_node_label_has_value(client.clone(), node_name, key, Some(&value)).await;
        Ok(value)
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
        let namespace = "default";
        delete_kube_state(client.clone(), namespace).await.unwrap();
        println!("here1");

        //let node_watcher = NodeLabelPersistenceService::new("default", &client)
        //.await
        //.unwrap();
        //tokio::spawn(async move {
        //node_watcher.watch_nodes().await.unwrap();
        //});
        // Make sure the Service is watching before proceeding
        //tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        add_node(client.clone(), test_node_name).await.unwrap();
        println!("here2");

        //
        // 2. Add a label to the node
        //
        let node_label_key = "label.to.persist.com/test_slash";
        let node_label_value = set_random_label(
            client.clone(),
            test_node_name,
            node_label_key,
            "node-label-service",
        )
        .await
        .unwrap();
        println!("here3");

        //
        // 3. Delete the node and assert that the label is stored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        println!("here4");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
        println!("here5");

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
    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Delete the node and assert that the label is stored
    /// 4. Add the node back to the cluster with a different label already set. Assert that the new
    ///    label is not overwritten.
    /// 5. Delete the node and see that the new label is stored.
    /// This test is to make sure that we do not overwrite newer labels on nodes even when they are
    /// added back.
    async fn test_not_overwriting_labels() {
        init_tracing();

        let namespace = "default";
        let client = Client::try_default().await.unwrap();
        delete_kube_state(client.clone(), namespace).await.unwrap();

        // Start our service
        //let node_watcher = NodeLabelPersistenceService::new(namespace, &client)
        //.await
        //.unwrap();
        //tokio::spawn(async move {
        //node_watcher.watch_nodes().await.unwrap();
        //});
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
        let node_label_value = set_random_label(
            client.clone(),
            test_node_name,
            node_label_key,
            "node-label-service",
        )
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
        // 4. Add the node back to the cluster with a different label already set. Assert that the new
        //    label is not overwritten.
        //
        let new_label_value = "test_label_value";
        let nodes: Api<Node> = Api::all(client.clone());
        let node = Node {
            metadata: ObjectMeta {
                name: Some(test_node_name.to_string()),
                labels: Some(
                    vec![(node_label_key.to_string(), new_label_value.to_string())]
                        .into_iter()
                        .collect(),
                ),
                ..Default::default()
            },
            ..Default::default()
        };
        nodes.create(&PostParams::default(), &node).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // Check if the node was added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));

        // Now the stored label value and the node label value are not the same.
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
        // Assert that the node has the new label
        assert_node_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&new_label_value.to_string()),
        )
        .await;

        //
        // 5. Delete the node and see that the new label is stored.
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&new_label_value.to_string()),
        )
        .await;
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
        let service_name = "node-label-service";
        let client = Client::try_default().await.unwrap();
        delete_kube_state(client.clone(), namespace).await.unwrap();

        // Start our service
        //let node_watcher = NodeLabelPersistenceService::new(namespace, &client)
        //.await
        //.unwrap();
        //tokio::spawn(async move {
        //node_watcher.watch_nodes().await.unwrap();
        //});
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
        let node_label_value =
            set_random_label(client.clone(), test_node_name, node_label_key, service_name)
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
            &client,
            test_node_name,
            node_label_key,
            service_name,
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // The node should now have no labels
        let nodes: Api<Node> = Api::all(client.clone());
        assert_eq!(
            nodes.get(test_node_name).await.unwrap().metadata.labels,
            None
        );

        //
        // 6. Delete the node so that the labels are stored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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
    async fn delete_all_nodes(client: Client) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(client.clone());
        let nodes_list = nodes.list(&ListParams::default()).await?;
        for node in nodes_list {
            let node_name = node.metadata.name.unwrap();
            if node_name != "minikube" {
                delete_node(client.clone(), &node_name).await?;
            }
        }
        Ok(())
    }

    /// Delete all nodes and delete all ConfMap data.
    /// Run this between unit tests to start from a clean state.
    async fn delete_kube_state(client: Client, namespace: &str) -> Result<(), anyhow::Error> {
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
        let service_name = "node-label-service";
        let client = Client::try_default().await.unwrap();
        delete_kube_state(client.clone(), namespace).await.unwrap(); // start with a clean slate
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Start our service
        //let node_watcher = NodeLabelPersistenceService::new(namespace, &client)
        //.await
        //.unwrap();
        //tokio::spawn(async move {
        //node_watcher.watch_nodes().await.unwrap();
        //});
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
                                &client,
                                &node_name,
                                &labels,
                                service_name,
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
                                    &client,
                                    &node_name,
                                    &label_key,
                                    service_name,
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
                                    &client,
                                    &node_name,
                                    &labels,
                                    service_name,
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
        // Subtract 1 because we expect 1 minikube node in the test cluster
        assert_eq!(num_nodes - 1, num_truth_nodes_in_cluster);
        // Iterate through the nodes and assert that their labels are correct
        for node in nodes_list
            .into_iter()
            .filter(|n| n.metadata.name != Some("minikube".to_string()))
        {
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

        // Iterate through the ConfigMaps and assert that the values are correct
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
