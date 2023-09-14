#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::Node;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::PostParams;
    use kube::{api::Api, Client};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    #[tokio::test]
    async fn test_add_and_remove_nodes() {
        let client = Client::try_default().await.unwrap();
        let nodes: Api<Node> = Api::all(client);

        // TODO: Do this for multiple nodes
        let node_to_add = "node1";
        let node_label: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

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

        // TODO: Add the node back
        // TODO: Restore labels
        // TODO: Check if the label is still there
    }
}
