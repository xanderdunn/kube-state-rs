// Local
pub mod processor;
pub mod utils;
pub mod watcher;

#[cfg(test)]
mod tests {
    // System
    use std::collections::BTreeMap;

    // Third Party
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::{PartialObjectMetaExt, Patch, PatchParams, PostParams};
    use kube::{
        api::{Api, ListParams},
        Client,
    };
    use rand::{distributions::Alphanumeric, seq::IteratorRandom, thread_rng, Rng, SeedableRng};
    use serde_json::json;
    use serial_test::serial;
    use tracing::debug;

    // Local
    use crate::utils::{init_tracing, LABEL_STORE_VERSION_KEY};

    /// Set label values on a node.
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

    /// Remove the label on a node.
    pub async fn delete_node_label(
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

    /// Add a label or update its value if it already exists.
    pub async fn add_or_update_node_label(
        client: &Client,
        node_name: &str,
        label_key: &str,
        label_value: &str,
        service_name: &str,
    ) -> Result<(), anyhow::Error> {
        let nodes = Api::<Node>::all(client.clone());

        let patch_params = PatchParams::apply(service_name);

        let patch = json!({
            "metadata": {
                "labels": {
                    label_key: label_value
                }
            }
        });

        nodes
            .patch(node_name, &patch_params, &Patch::Merge(patch))
            .await?;
        Ok(())
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
        assert_eq!(
            node.metadata.labels.as_ref().unwrap().get(key),
            value,
            "Node has {} value {:?} but expected {:?}",
            key,
            node.metadata.labels.as_ref().unwrap().get(key),
            value
        );
    }

    /// A convenience function to create a node by name.
    /// This does nothing if the node already exists.
    async fn create_node(client: Client, node_name: &str) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(client.clone());

        let node = Node {
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        // Check if it already exists
        let node_list = nodes.list(&Default::default()).await.unwrap();
        if node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(node_name.to_string()))
        {
            return Ok(());
        }
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
        let random_value: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let value: String = format!("value_{}", random_value);
        let mut new_labels = BTreeMap::new();
        new_labels.insert(key.to_string(), value.clone());
        set_node_labels(&client, node_name, &new_labels, service_name)
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
    /// 3. Delete the node, add the node back to the cluster and assert that the label is restored
    async fn test_add_and_remove_node() {
        init_tracing("kube_state_rs", tracing::Level::DEBUG);

        // Start our service
        let client = Client::try_default().await.unwrap();

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        create_node(client.clone(), test_node_name).await.unwrap();

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

        //
        // 3. Delete the node, add the node back to the cluster, and assert that the label is restored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        create_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
        assert_node_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Delete the node and assert that the label is stored
    /// 4. Add the node back to the cluster with a different label already set. Assert that the new
    ///    label is overwritten.
    /// This is testing the edge case where labels are set on a node that is brought back to the
    /// cluster before the processor has time to restore labels.
    async fn test_overwriting_labels() {
        init_tracing("kube_state_rs", tracing::Level::DEBUG);

        //let namespace = "default";
        let client = Client::try_default().await.unwrap();

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        create_node(client.clone(), test_node_name).await.unwrap();

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
        println!("here1");
        assert_node_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value.to_string()),
        )
        .await;

        //
        // 3. Delete the node so that the label is stored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        //
        // 4. Add the node back to the cluster with a different label already set.
        // Assert that the new label is overwritten.
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
        // Check if the node was added
        let node_list = nodes.list(&Default::default()).await.unwrap();
        assert!(node_list
            .items
            .iter()
            .any(|n| n.metadata.name == Some(test_node_name.to_string())));
        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;

        // Assert that the node has the stored label value
        println!("here2");
        assert_node_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value.to_string()),
        )
        .await;
        println!("here3");
    }

    #[tokio::test]
    #[serial]
    /// 1. Create a node
    /// 2. Add a label to the node.
    /// 3. Delete the node so that the label is stored
    /// 4. Add the node back to the cluster and assert that the label is restored
    /// 5. Delete the label on the node
    /// 6. Cycle the node again and see that the label is not restored
    async fn test_deleting_labels() {
        init_tracing("kube_state_rs", tracing::Level::DEBUG);

        // Start from a clean slate
        let service_name = "node-label-service";
        let client = Client::try_default().await.unwrap();

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        create_node(client.clone(), test_node_name).await.unwrap();

        //
        // 2. Add a label to the node
        //
        let node_label_key = "label_to_persist";
        let node_label_value =
            set_random_label(client.clone(), test_node_name, node_label_key, service_name)
                .await
                .unwrap();
        add_or_update_node_label(
            &client,
            test_node_name,
            node_label_key,
            &node_label_value,
            service_name,
        )
        .await
        .unwrap();

        //
        // 3. Delete the node so that labels are stored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();

        //
        // 4. Add the node back to the cluster and assert that the label is restored
        //
        create_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        debug!("assertion 1");
        assert_node_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;

        //
        // 5. Delete the label on the node
        //
        delete_node_label(&client, test_node_name, node_label_key, service_name)
            .await
            .unwrap();
        // The node should now have no labels
        let nodes: Api<Node> = Api::all(client.clone());
        // The node should not have the key that was deleted
        let node_label_keys = nodes
            .get(test_node_name)
            .await
            .unwrap()
            .metadata
            .labels
            .unwrap()
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        debug!("assertion 2");
        assert!(!node_label_keys.contains(&node_label_key.to_string()));

        //
        // 6. Cycle the node again and see that the label is not restored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        create_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        // The node should not have the key that was deleted
        let node_label_keys = nodes
            .get(test_node_name)
            .await
            .unwrap()
            .metadata
            .labels
            .unwrap()
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        debug!("assertion 3");
        assert!(!node_label_keys.contains(&node_label_key.to_string()));
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
    async fn test_differential_fuzzing() {
        init_tracing("kube_state_rs", tracing::Level::DEBUG);

        // Start from a clean slate
        let namespace = "default";
        let service_name = "node-label-service";
        let client = Client::try_default().await.unwrap();

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
                    let node_name_length = rng.gen_range(1..254);
                    let node_name: String = rng
                        .clone()
                        .sample_iter(&Alphanumeric)
                        .take(node_name_length)
                        .map(char::from)
                        .collect();
                    let node_name = node_name.to_lowercase();
                    truth_node_labels.insert(node_name.clone(), (true, BTreeMap::new()));
                    create_node(client.clone(), &node_name).await.unwrap();
                    debug!("Action Completed: added node {}", node_name);
                }
                NodeAction::AddLabelTo => {
                    // Randomly choose a key from truth_node_labels
                    let node_name = truth_node_labels.keys().choose(&mut rng).cloned();
                    if let Some(node_name) = node_name {
                        let random_values = rng
                            .clone()
                            .sample_iter(&Alphanumeric)
                            .take(20)
                            .map(char::from)
                            .collect::<String>();
                        let label_key =
                            format!("key_{}", random_values.chars().take(10).collect::<String>());
                        let label_value = format!(
                            "value_{}",
                            random_values.chars().skip(10).collect::<String>()
                        );
                        let (in_cluster, mut labels) = truth_node_labels[&node_name].clone();
                        if in_cluster {
                            // can't add label to a deleted node
                            labels.insert(label_key, label_value);
                            truth_node_labels
                                .insert(node_name.clone(), (in_cluster, labels.clone()));
                            debug!("Action Completed: added label on node {}", node_name);
                            set_node_labels(&client, &node_name, &labels, service_name)
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
                                delete_node_label(&client, &node_name, &label_key, service_name)
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
                            let label_value = format!(
                                "value_{}",
                                rng.clone()
                                    .sample_iter(&Alphanumeric)
                                    .take(10)
                                    .map(char::from)
                                    .collect::<String>()
                            );
                            let (in_cluster, mut labels) = truth_node_labels[&node_name].clone();
                            if in_cluster {
                                // can't change label on a deleted node
                                labels.insert(label_key, label_value.clone());
                                truth_node_labels
                                    .insert(node_name.clone(), (in_cluster, labels.clone()));
                                debug!(
                                    "Action Completed: changed label on node {} to {}",
                                    node_name, label_value
                                );
                                set_node_labels(&client, &node_name, &labels, service_name)
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
        debug!("Finished fuzz test with {} nodes", num_nodes);
        debug!("Truth labels: {:?}", truth_node_labels);
        for node in nodes_list.items.clone().into_iter() {
            debug!("Node in cluster: {:?}", node.metadata.name.unwrap());
        }
        // Iterate through the nodes and assert that their labels are correct
        for node in nodes_list
            .into_iter()
            .filter(|n| n.metadata.name != Some("minikube".to_string()))
        {
            let node_name = node.metadata.name.unwrap();
            let node_labels = match node.metadata.labels.clone() {
                Some(mut node_labels) => {
                    node_labels.remove(LABEL_STORE_VERSION_KEY);
                    Some(node_labels)
                }
                None => None,
            };
            if let Some(truth_labels) = truth_node_labels.get(&node_name) {
                if let Some(node_labels) = node_labels {
                    assert_eq!(node_labels, truth_labels.1);
                } else {
                    assert_eq!(
                        truth_labels.1.len(),
                        0,
                        "Node {} has no labels but truth is expecting {:?}",
                        node_name,
                        truth_labels
                    );
                }
            }
        }

        // All labels should be stored when the nodes are deleted:
        //delete_all_nodes(client.clone()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

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
        for config_map in config_maps_list {
            let config_map_name = config_map.metadata.name.unwrap();
            if config_map_name != "kube-root-ca.crt" && config_map_name != "minikube" {
                debug!("ConfigMap name: {}", config_map_name);
                let truth_labels = truth_node_labels.get(&config_map_name).unwrap();
                if let Some(mut config_map_labels) = config_map.data.clone() {
                    config_map_labels.remove(LABEL_STORE_VERSION_KEY);
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
