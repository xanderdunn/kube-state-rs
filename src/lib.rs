// System
use std::collections::BTreeMap;

// Third Party
use anyhow::anyhow;
use k8s_openapi::api::core::v1::{ConfigMap, Node};
use kube::{
    api::{
        Api, DeleteParams, ListParams, ObjectMeta, PartialObjectMetaExt, Patch, PatchParams,
        PostParams,
    },
    Client,
};
use serde_json::json;
use tokio::task;
use tracing::{debug, info, warn};

// Local
pub mod utils;

/// The monotonically increasing revision count of the labels on the node. +1 every time the labels
/// stored in the ConfigMap change for a node.
/// This is updated on the ConfigMap every time labels are stored.
const LABEL_VERSION: &str = "label_version";
/// The ResourceVersion of the node as last seen by the ConfigMap.
/// This is updated on the ConfigMap every time labels are stored or restored.
const RESOURCE_VERSION: &str = "resource_version";

/// The special token reserved to encode `/` in label keys so that they can be stored as ConfigMap
/// keys.
const SLASH_TOKEN: &str = "-SLASH-";

enum ConfigMapState {
    None,  // A ConfigMap does not exist for this node
    Empty, // Exists but no labels on it (still has a LabelVersion)
    NonEmpty,
}

enum NodeState {
    NoLabels,
    LabelsNoVersion,
    LabelsAndVersion,
}

enum LabelVersionComparison {
    Equal,      // The node and ConfigMap have the same label_version
    NodeHigher, // The node has a higher label_version than the ConfigMap
    ConfigMapHigher,
}

enum ResourceVersionComparison {
    Equal,
    NodeHigher,
    ConfigMapHigher,
}

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
        info!("Creating NodeLabelPersistenceService");
        Ok(NodeLabelPersistenceService {
            client: client.clone(),
            namespace: namespace.to_string(),
        })
    }

    /// As a workaround for Kubernetes ConfigMap key restrictions, we replace all forward slashes in
    /// keys with SLASH_TOKEN.
    /// This function either encodes or decodes all keys in a given map.
    /// encode: true is to replace `/` with `SLASH_TOKEN`
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

    /// Given a set of node labels and stored labels, add the stored_labels to the node.
    /// This does not overwrite.
    async fn restore_node_labels(
        client: &Client,
        node_name: &str,
        node_labels: Option<&BTreeMap<String, String>>,
        stored_labels: Option<&BTreeMap<String, String>>,
    ) -> Result<(), anyhow::Error> {
        let node_labels: BTreeMap<String, String> = match node_labels {
            Some(node_labels) => {
                let mut node_labels = node_labels.clone();
                node_labels.remove(RESOURCE_VERSION);
                node_labels
            }
            None => BTreeMap::<String, String>::new(),
        };
        let mut new_labels = node_labels.clone();

        let stored_labels = match stored_labels {
            Some(stored_labels) => stored_labels.clone(),
            None => BTreeMap::<String, String>::new(),
        };

        // Add missing keys
        for (key, value) in stored_labels {
            if key != RESOURCE_VERSION {
                new_labels
                    .entry(key.clone().replace(SLASH_TOKEN, "/")) // Decode keys with slashes
                    .or_insert(value.clone());
            }
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
    /// This sets the LABEL_VERSION to 1 on both the node and the ConfigMap.
    /// This sets the RESOURCE_VERSION on the ConfigMap.
    async fn create_stored_labels(
        client: &Client,
        node_name: &str,
        node_labels: &BTreeMap<String, String>,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        let mut node_labels = node_labels.clone();
        // Add label_version to ConfigMap
        node_labels.insert(LABEL_VERSION.to_string(), "1".to_string());
        // Add resource_version to ConfigMap
        node_labels.insert(
            RESOURCE_VERSION.to_string(),
            node_labels
                .get(RESOURCE_VERSION)
                .unwrap_or(&"0".to_string())
                .to_string(),
        );
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

        // Add label_version to node. We should not crash if this fails because we have already
        // safely stored the labels in the ConfigMap.
        // TODO: Maybe I should surface all errors and print them as warnings inside handle_nodes
        // rather than handling this one here?
        let _ = Self::add_or_update_node_label(client, node_name, LABEL_VERSION, "1", "default")
            .await
            .map_err(|e| {
                warn!(
                    "create_stored_label failed to update LABEL_VERSION on node {}: {:?}",
                    node_name, e
                );
            });
        Ok(())
    }

    /// Given a set of node labels, update the stored labels in the `ConfigMap`.
    /// It will replace all labels in the `ConfigMap`
    /// `node_labels` must be non-empty.
    /// This overwrites.
    /// This increments the LABEL_VERSION +1 on the ConfigMap and sets it on the node.
    /// This updates the RESOURCE_VERSION on the ConfigMap.
    async fn update_stored_labels(
        client: &Client,
        node_name: &str,
        node_resource_version: &str,
        node_labels: &BTreeMap<String, String>,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        assert!(!node_labels.is_empty());
        let mut node_labels = node_labels.clone();

        let label_version = node_labels
            .get(LABEL_VERSION)
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0)
            + 1;

        // Increment the label_version on the ConfigMap
        node_labels.insert(LABEL_VERSION.to_string(), label_version.to_string());
        // Update the resource_version on the ConfigMap
        node_labels.insert(
            RESOURCE_VERSION.to_string(),
            node_resource_version.to_string(),
        );

        let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

        if node_labels.len() == 2 {
            // Only the LABEL_VERSION and RESOURCE_VERSION are set on the node
            Self::delete_config_map(client, node_name, namespace).await?;
            Self::delete_node_label(client, node_name, LABEL_VERSION, "kube-state-rs").await?;
        } else {
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

            // Update the label_version on the Node
            // TODO: Maybe I should surface all errors and print them as warnings inside handle_nodes
            // rather than handling this one here?
            let _ = Self::add_or_update_node_label(
                client,
                node_name,
                LABEL_VERSION,
                &label_version.to_string(),
                "default",
            )
            .await
            .map_err(|e| {
                warn!(
                    "create_stored_label failed to update LABEL_VERSION on node {}: {:?}",
                    node_name, e
                );
            });
        }

        Ok(())
    }

    /// This updates the RESOURCE_VERSION we store to record the most recent version of the node
    /// that's been seen by the ConfigMap.
    async fn update_config_map_resource_version(
        client: &Client,
        config_map: &ConfigMap,
        new_resource_version: &str,
    ) -> Result<(), anyhow::Error> {
        let config_maps: Api<ConfigMap> = Api::namespaced(
            client.clone(),
            config_map
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("default"),
        );
        let patch = Patch::Apply(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "data": {
                RESOURCE_VERSION: new_resource_version
            }
        }));
        // TODO: Prefer to specify the version rather than use .force() here
        let params = PatchParams::apply(config_map.metadata.name.as_deref().unwrap()).force();
        config_maps
            .patch(
                config_map.metadata.name.as_deref().unwrap(),
                &params,
                &patch,
            )
            .await?;
        Ok(())
    }

    /// It's important that the config map we want to delete here actually exists or this will throw
    /// an error
    async fn delete_config_map(
        client: &Client,
        node_name: &str,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        config_maps
            .delete(node_name, &DeleteParams::default())
            .await
            .map_err(|e| anyhow!(e).context("Failed to update config map"))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_node(
        client: &Client,
        namespace: &str,
        node: &Node,
        config_map: Result<ConfigMap, kube::Error>,
        config_map_state: ConfigMapState,
        node_state: NodeState,
        label_version_cmp: LabelVersionComparison,
        resource_version_cmp: ResourceVersionComparison,
    ) -> Result<(), anyhow::Error> {
        match (config_map_state, node_state) {
            (ConfigMapState::None, NodeState::NoLabels) => {
                debug!("1: {} Do nothing", node.metadata.name.as_ref().unwrap());
                Ok(())
            }
            (ConfigMapState::None, NodeState::LabelsNoVersion) => {
                debug!(
                    "2: {} Create ConfigMap + Store all labels from Node + set LabelVersion to 1",
                    node.metadata.name.as_ref().unwrap()
                );
                Self::create_stored_labels(
                    client,
                    node.metadata.name.as_ref().unwrap(),
                    node.metadata.labels.as_ref().unwrap(),
                    namespace,
                )
                .await?;
                Ok(())
            }
            (ConfigMapState::None, NodeState::LabelsAndVersion) => {
                debug!(
                    "3: {} Create ConfigMap + Store all labels from Node + set LabelVersion to 1",
                    node.metadata.name.as_ref().unwrap()
                );
                warn!("Encountered node {:?} with a label version, but there was no ConfigMap associated with that node. This is not expected during normal operation.", node.metadata.name);
                Self::create_stored_labels(
                    client,
                    node.metadata.name.as_ref().unwrap(),
                    node.metadata.labels.as_ref().unwrap(),
                    namespace,
                )
                .await?;
                Ok(())
            }
            (ConfigMapState::Empty, NodeState::NoLabels) => {
                debug!(
                    "4: {} Delete the ConfigMap",
                    node.metadata.name.as_ref().unwrap()
                );
                Self::delete_config_map(client, node.metadata.name.as_ref().unwrap(), namespace)
                    .await?;
                Ok(())
            }
            (ConfigMapState::Empty, NodeState::LabelsNoVersion) => {
                debug!(
                    "5: {} Store all labels from Node + increment LabelVersion",
                    node.metadata.name.as_ref().unwrap()
                );
                Self::update_stored_labels(
                    client,
                    node.metadata.name.as_ref().unwrap(),
                    node.metadata.resource_version.as_ref().unwrap(),
                    node.metadata.labels.as_ref().unwrap(),
                    namespace,
                )
                .await?;
                Ok(())
            }
            (ConfigMapState::Empty, NodeState::LabelsAndVersion) => {
                debug!(
                    "6: {} <This should not happen> Store all labels from the node into the ConfigMap"
                , node.metadata.name.as_ref().unwrap());
                Self::update_stored_labels(
                    client,
                    node.metadata.name.as_ref().unwrap(),
                    node.metadata.resource_version.as_ref().unwrap(),
                    node.metadata.labels.as_ref().unwrap(),
                    namespace,
                )
                .await?;
                Ok(())
            }
            (ConfigMapState::NonEmpty, NodeState::NoLabels) => {
                debug!(
                    "7: {} Restore all labels to Node + Update ResourceVersion",
                    node.metadata.name.as_ref().unwrap()
                );
                Self::restore_node_labels(
                    client,
                    node.metadata.name.as_ref().unwrap(),
                    node.metadata.labels.as_ref(),
                    config_map.as_ref().unwrap().data.as_ref(),
                )
                .await?;
                Self::update_config_map_resource_version(
                    client,
                    &config_map.unwrap(),
                    node.metadata.resource_version.as_ref().unwrap(),
                )
                .await?;
                Ok(())
            }
            (ConfigMapState::NonEmpty, NodeState::LabelsNoVersion) => {
                debug!("8: {} Store any labels on node not in ConfigMap, Restore any labels in the ConfigMap not on the Node", node.metadata.name.as_ref().unwrap());
                Self::restore_node_labels(
                    client,
                    node.metadata.name.as_ref().unwrap(),
                    node.metadata.labels.as_ref(),
                    config_map.unwrap().data.as_ref(),
                )
                .await?;
                Self::update_stored_labels(
                    client,
                    node.metadata.name.as_ref().unwrap(),
                    node.metadata.resource_version.as_ref().unwrap(),
                    node.metadata.labels.as_ref().unwrap(),
                    namespace,
                )
                .await?;
                Ok(())
            }
            (ConfigMapState::NonEmpty, NodeState::LabelsAndVersion) => {
                match (label_version_cmp, resource_version_cmp) {
                    (LabelVersionComparison::Equal, ResourceVersionComparison::Equal) => {
                        debug!("9: {} Do nothing", node.metadata.name.as_ref().unwrap());
                        Ok(())
                    }
                    (LabelVersionComparison::Equal, ResourceVersionComparison::NodeHigher) => {
                        // Someone could've modified the labels without incrementing the LabelVersion
                        debug!(
                            "10: {} Update ConfigMap with node labels if there are any changes + Store updated ResourceVersion",
                            node.metadata.name.as_ref().unwrap()
                        );
                        // It's important I update the stored labels here only if the node's labels
                        // are different from the stored labels. Otherwise it will increment the
                        // node's ResourceVersion and cause an infinite loop of updates.
                        let node_labels: Option<BTreeMap<String, String>> = {
                            match node.metadata.labels.clone() {
                                Some(mut labels) => {
                                    labels.remove(RESOURCE_VERSION);
                                    Some(labels)
                                }
                                None => None,
                            }
                        };
                        let config_map_labels_decoded: Option<BTreeMap<String, String>> =
                            match config_map.as_ref().unwrap().data.clone() {
                                Some(data) => {
                                    let mut data = data.clone();
                                    Self::code_key_slashes(&mut data, false);
                                    data.remove(RESOURCE_VERSION);
                                    Some(data)
                                }
                                None => None,
                            };
                        debug!("node labels: {:?}", node_labels);
                        debug!("config map labels: {:?}", config_map_labels_decoded);
                        if node_labels != config_map_labels_decoded {
                            Self::update_stored_labels(
                                client,
                                node.metadata.name.as_ref().unwrap(),
                                node.metadata.resource_version.as_ref().unwrap(),
                                node.metadata.labels.as_ref().unwrap(),
                                namespace,
                            )
                            .await?;
                        } else {
                            Self::update_config_map_resource_version(
                                client,
                                &config_map.unwrap(),
                                node.metadata.resource_version.as_ref().unwrap(),
                            )
                            .await?;
                        }
                        Ok(())
                    }
                    (LabelVersionComparison::Equal, ResourceVersionComparison::ConfigMapHigher) => {
                        // This means that the node was deleted and labels were restored without updating the ResourceVersion in the ConfigMap
                        debug!("11: <This should not happen!> Store any labels on node not in ConfigMap, Restore any labels in the ConfigMap not on the Node");
                        warn!("Both the node and the ConfigMap have labels and equal label version, but the ConfigMap has a higher ResourceVersion. This means an incorrect ResourceVersion was stored in the COnfig, or the node was removed from the cluster and then re-added, but the ConfigMap's ResourceVersion was not updated. This is not expected to occur during normal operation.");
                        Self::restore_node_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.labels.as_ref(),
                            config_map.unwrap().data.as_ref(),
                        )
                        .await?;
                        Self::update_stored_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.resource_version.as_ref().unwrap(),
                            node.metadata.labels.as_ref().unwrap(),
                            namespace,
                        )
                        .await?;
                        Ok(())
                    }
                    (LabelVersionComparison::NodeHigher, ResourceVersionComparison::Equal) => {
                        debug!(
                            "12: Store: Overwrite ConfigMap labels + Store updated LabelVersion"
                        );
                        Self::update_stored_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.resource_version.as_ref().unwrap(),
                            node.metadata.labels.as_ref().unwrap(),
                            namespace,
                        )
                        .await?;
                        Ok(())
                    }
                    (LabelVersionComparison::NodeHigher, ResourceVersionComparison::NodeHigher) => {
                        // Someone incremented the label version
                        debug!("13: Store: Overwrite ConfigMap labels + Store ResourceVersion in ConfigMap");
                        Self::update_stored_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.resource_version.as_ref().unwrap(),
                            node.metadata.labels.as_ref().unwrap(),
                            namespace,
                        )
                        .await?;
                        Ok(())
                    }
                    (
                        LabelVersionComparison::NodeHigher,
                        ResourceVersionComparison::ConfigMapHigher,
                    ) => {
                        // The node has been removed and added back to the cluster and also someone else incremented the label version
                        debug!("14: Store: Overwrite ConfigMap labels + Store ResourceVersion in ConfigMap");
                        Self::update_stored_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.resource_version.as_ref().unwrap(),
                            node.metadata.labels.as_ref().unwrap(),
                            namespace,
                        )
                        .await?;
                        Ok(())
                    }
                    (LabelVersionComparison::ConfigMapHigher, ResourceVersionComparison::Equal) => {
                        debug!("15: Restore: Overwrite node labels");
                        // TODO: This does not overwrite but it should.
                        Self::restore_node_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.labels.as_ref(),
                            config_map.unwrap().data.as_ref(),
                        )
                        .await?;
                        Ok(())
                    }
                    (
                        LabelVersionComparison::ConfigMapHigher,
                        ResourceVersionComparison::NodeHigher,
                    ) => {
                        debug!(
                            "16: Restore: Overwrite node labels + Store ResourceVersion in ConfigMap"
                        );
                        // TODO: This does not overwrite but it should.
                        Self::restore_node_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.labels.as_ref(),
                            config_map.as_ref().unwrap().data.as_ref(),
                        )
                        .await?;
                        Self::update_config_map_resource_version(
                            client,
                            &config_map.unwrap(),
                            node.metadata.resource_version.as_ref().unwrap(),
                        )
                        .await?;
                        Ok(())
                    }
                    (
                        LabelVersionComparison::ConfigMapHigher,
                        ResourceVersionComparison::ConfigMapHigher,
                    ) => {
                        // The node has been removed from the cluster and added back
                        debug!(
                            "17: Restore: Overwrite node labels + Store ResourceVersion in ConfigMap"
                        );
                        // TODO: This does not overwrite but it should.
                        Self::restore_node_labels(
                            client,
                            node.metadata.name.as_ref().unwrap(),
                            node.metadata.labels.as_ref(),
                            config_map.as_ref().unwrap().data.as_ref(),
                        )
                        .await?;
                        Self::update_config_map_resource_version(
                            client,
                            &config_map.unwrap(),
                            node.metadata.resource_version.as_ref().unwrap(),
                        )
                        .await?;
                        Ok(())
                    }
                }
            }
        }
    }

    async fn get_config_map_state(
        config_map: &Result<ConfigMap, kube::Error>,
    ) -> Result<ConfigMapState, anyhow::Error> {
        match config_map {
            Ok(config_map) => match config_map.data.clone() {
                Some(data) => {
                    if data.is_empty() {
                        Ok(ConfigMapState::Empty)
                    } else {
                        Ok(ConfigMapState::NonEmpty)
                    }
                }
                None => Ok(ConfigMapState::Empty),
            },
            Err(_) => Ok(ConfigMapState::None),
        }
    }

    async fn get_node_state(node: &Node) -> Result<NodeState, anyhow::Error> {
        let labels = &node.metadata.labels;
        match labels {
            Some(labels) => {
                if labels.contains_key(LABEL_VERSION) {
                    Ok(NodeState::LabelsAndVersion)
                } else {
                    Ok(NodeState::LabelsNoVersion)
                }
            }
            None => Ok(NodeState::NoLabels),
        }
    }

    async fn compare_label_version(
        node: &Node,
        config_map: &Result<ConfigMap, kube::Error>,
    ) -> Result<LabelVersionComparison, anyhow::Error> {
        let node_version: Option<&String> = node
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(LABEL_VERSION));

        let config_map_version = {
            match config_map {
                Ok(config_map) => config_map
                    .data
                    .as_ref()
                    .and_then(|data| data.get(LABEL_VERSION)),
                Err(_) => None,
            }
        };

        match (node_version, config_map_version) {
            (Some(node_version), Some(config_map_version)) => {
                Ok(match node_version.cmp(config_map_version) {
                    std::cmp::Ordering::Equal => LabelVersionComparison::Equal,
                    std::cmp::Ordering::Greater => LabelVersionComparison::NodeHigher,
                    std::cmp::Ordering::Less => LabelVersionComparison::ConfigMapHigher,
                })
            }
            (Some(_), None) => Ok(LabelVersionComparison::NodeHigher),
            (None, Some(_)) => Ok(LabelVersionComparison::ConfigMapHigher),
            _ => Ok(LabelVersionComparison::Equal),
        }
    }

    async fn compare_resource_version(
        node: &Node,
        config_map: &Result<ConfigMap, kube::Error>,
    ) -> Result<ResourceVersionComparison, anyhow::Error> {
        let node_resource_version: Option<u64> = node
            .metadata
            .resource_version
            .as_ref()
            .and_then(|resource_version| resource_version.parse().ok());

        let config_map_resource_version: Option<u64> = {
            match config_map {
                Ok(config_map) => match config_map.data.clone() {
                    Some(data) => data
                        .get(RESOURCE_VERSION)
                        .and_then(|resource_version| resource_version.clone().parse().ok()),
                    None => None,
                },
                Err(_) => None,
            }
        };

        match (node_resource_version, config_map_resource_version) {
            (Some(node_resource_version), Some(config_map_resource_version)) => Ok(
                match node_resource_version.cmp(&config_map_resource_version) {
                    std::cmp::Ordering::Equal => ResourceVersionComparison::Equal,
                    std::cmp::Ordering::Greater => ResourceVersionComparison::NodeHigher,
                    std::cmp::Ordering::Less => ResourceVersionComparison::ConfigMapHigher,
                },
            ),
            (Some(_), None) => Ok(ResourceVersionComparison::NodeHigher),
            (None, Some(_)) => Ok(ResourceVersionComparison::ConfigMapHigher),
            (None, None) => Ok(ResourceVersionComparison::Equal),
        }
    }

    /// Iterate over the nodes in the cluster looking for changed metadata to either store or
    /// update on the nodes.
    pub async fn handle_nodes(
        &self,
        num_ready_replicas: usize,
        my_replica_id: usize,
    ) -> Result<(), anyhow::Error> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let nodes_list = nodes.list(&ListParams::default()).await?;
        let config_maps: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);

        let mut handles = Vec::new();
        let my_nodes = nodes_list
            .items
            .chunks(num_ready_replicas)
            .nth(my_replica_id)
            .unwrap_or(&[]);
        debug!(
            "replica_id {} of {}: I am responsible for {} of {} nodes",
            my_replica_id,
            num_ready_replicas - 1,
            my_nodes.len(),
            nodes_list.items.len(),
        );
        for node in my_nodes {
            let node = node.clone();
            let config_maps = config_maps.clone();
            let client = self.client.clone();
            let namespace = self.namespace.clone();
            let handle = task::spawn(async move {
                let node_name = node.metadata.name.as_ref().unwrap();
                let config_map = config_maps.get(node_name).await;
                let config_map_state = Self::get_config_map_state(&config_map).await?;
                let node_state = Self::get_node_state(&node).await?;
                let label_version_cmp = Self::compare_label_version(&node, &config_map).await?;
                let resource_version_cmp =
                    Self::compare_resource_version(&node, &config_map).await?;
                Self::handle_node(
                    &client,
                    &namespace,
                    &node,
                    config_map,
                    config_map_state,
                    node_state,
                    label_version_cmp,
                    resource_version_cmp,
                )
                .await?;
                Result::<(), anyhow::Error>::Ok(())
            });
            handles.push(handle);
        }

        let results: Result<Vec<_>, _> = futures::future::join_all(handles)
            .await
            .into_iter()
            .collect();
        match results {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
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
    use super::{
        utils::init_tracing, NodeLabelPersistenceService, LABEL_VERSION, RESOURCE_VERSION,
    };

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
        let random_value: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let value: String = format!("value_{}", random_value);
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
    /// 3. Assert that the label is stored
    /// 4. Add the node back to the cluster and assert that the label is restored
    async fn test_add_and_remove_nodes() {
        init_tracing();

        // Start our service
        let client = Client::try_default().await.unwrap();
        let namespace = "default";
        delete_kube_state(client.clone(), namespace).await.unwrap();

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        add_node(client.clone(), test_node_name).await.unwrap();

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
        // 3. Assert that the label is stored
        //
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;

        //
        // 4. Delete the node, add the node back to the cluster, and assert that the label is restored
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        add_node(client.clone(), test_node_name).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
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
    }

    // TODO: Fix this test. Currently we're overwriting.
    #[ignore]
    #[tokio::test]
    #[serial]
    /// Test the following scenario:
    /// 1. Create a node
    /// 2. Add a label to the node
    /// 3. Assert that the label is stored
    /// 4. Add the node back to the cluster with a different label already set. Assert that the new
    ///    label is not overwritten.
    /// 5. Assert that the new label is stored.
    /// This test is to make sure that we do not overwrite newer labels on nodes even when they are
    /// added back.
    async fn test_not_overwriting_labels() {
        init_tracing();

        let namespace = "default";
        let client = Client::try_default().await.unwrap();
        delete_kube_state(client.clone(), namespace).await.unwrap();

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
        // 3. Assert that the label is stored
        //
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;

        //
        // 4. Delete the node and add the node back to the cluster with a different label already set.
        // Assert that the new label is not overwritten.
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
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
        println!("here1");
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
        println!("here2");
        // Assert that the node has the new label
        assert_node_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&new_label_value.to_string()),
        )
        .await;
        println!("here3");

        //
        // 5. Assert that the new label is stored.
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
        println!("here4");
    }

    #[tokio::test]
    #[serial]
    /// 1. Create a node
    /// 2. Add a label to the node.
    /// 3. Add the node back to the cluster
    /// 4. Delete the label on the node
    /// 5. Assert that deleted label is not in the store.
    async fn test_deleting_labels() {
        init_tracing();

        // Start from a clean slate
        let namespace = "default";
        let service_name = "node-label-service";
        let client = Client::try_default().await.unwrap();
        delete_kube_state(client.clone(), namespace).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

        //
        // 1. Create a node.
        //
        let test_node_name = "node1";
        add_node(client.clone(), test_node_name).await.unwrap();
        println!("here1");

        //
        // 2. Add a label to the node
        //
        let node_label_key = "label_to_persist";
        let node_label_value =
            set_random_label(client.clone(), test_node_name, node_label_key, service_name)
                .await
                .unwrap();
        println!("here2");

        //
        // 3. Assert that the label is stored
        //
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        assert_stored_label_has_value(
            client.clone(),
            test_node_name,
            node_label_key,
            Some(&node_label_value),
        )
        .await;
        println!("here3");

        //
        // 4. Remove and add the node back to the cluster
        //
        delete_node(client.clone(), test_node_name).await.unwrap();
        add_node(client.clone(), test_node_name).await.unwrap();
        println!("here4");

        //
        // 5. Delete the label on the node
        //
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        NodeLabelPersistenceService::delete_node_label(
            &client,
            test_node_name,
            node_label_key,
            service_name,
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        // The node should now have no labels
        let nodes: Api<Node> = Api::all(client.clone());
        assert_eq!(
            nodes.get(test_node_name).await.unwrap().metadata.labels,
            None
        );
        println!("here5");

        let config_maps: Api<ConfigMap> = Api::namespaced(client, namespace);
        let config_maps_list = config_maps.list(&ListParams::default()).await.unwrap();
        for config_map in config_maps_list.items.clone().into_iter() {
            if let Some(name) = config_map.metadata.name {
                assert_ne!(name, test_node_name);
            }
        }
        println!("here6");
        // minikube and a root certificate
        assert_eq!(config_maps_list.items.len(), 2);
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

    // FIXME: There are scenarios where this test fails
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
                                NodeLabelPersistenceService::delete_node_label(
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
                    // If a change is made and then the node is deleted before the storage service
                    // sees it, the change will be lost, so sleep.
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
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
            let node_labels = match node.metadata.labels.clone() {
                Some(mut node_labels) => {
                    node_labels.remove(LABEL_VERSION);
                    Some(node_labels)
                }
                None => None,
            };
            if let Some(node_labels) = node_labels {
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
        // +2 because of the minikube and root certificate config maps
        assert_eq!(num_config_maps, num_truth_nodes_with_labels + 2);
        for config_map in config_maps_list {
            let config_map_name = config_map.metadata.name.unwrap();
            if config_map_name != "kube-root-ca.crt" && config_map_name != "minikube" {
                println!("ConfigMap name: {}", config_map_name);
                let truth_labels = truth_node_labels.get(&config_map_name).unwrap();
                if let Some(mut config_map_labels) = config_map.data.clone() {
                    config_map_labels.remove(RESOURCE_VERSION);
                    config_map_labels.remove(LABEL_VERSION);
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
