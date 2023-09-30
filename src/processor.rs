// Third Party
use k8s_openapi::api::core::v1::{ConfigMap, Node};
use kube::{
    api::{Api, ObjectMeta},
    Client,
};
use kube_leader_election::{LeaseLock, LeaseLockParams, LeaseLockResult};
use rand::seq::SliceRandom;
use tokio::time::Duration;
use tracing::{debug, warn};

// Local
use crate::utils::{
    code_key_slashes, replace_config_map_data, replace_node_labels, LABEL_STORE_VERSION_KEY,
    NODE_METADATA_NAMESPACE, TRANSACTION_NAMESPACE,
};

enum TransactionProcessorState {
    FetchTransactions,
    SelectTransaction(Vec<ConfigMap>),
    LeaderElection(ConfigMap),
    Process(ConfigMap, LeaseLock, LeaseLockResult),
    Cleanup(ConfigMap),
}

pub struct TransactionProcessor {
    transactions: Api<ConfigMap>,
    node_metadata: Api<ConfigMap>,
    all_nodes: Api<Node>,
}

impl TransactionProcessor {
    pub fn new(client: &Client) -> Self {
        let transactions: Api<ConfigMap> = Api::namespaced(client.clone(), TRANSACTION_NAMESPACE);
        let node_metadata: Api<ConfigMap> =
            Api::namespaced(client.clone(), NODE_METADATA_NAMESPACE);
        Self {
            transactions,
            node_metadata,
            all_nodes: Api::all(client.clone()),
        }
    }

    /// Get all ConfigMaps in the `TRANSACTION_NAMESPACE`
    async fn fetch_transactions(&self) -> Result<Option<Vec<ConfigMap>>, anyhow::Error> {
        let transaction_list = self.transactions.list(&Default::default()).await?;
        // Exclude special system ConfigMap
        let filtered_list: Vec<ConfigMap> = transaction_list
            .items
            .into_iter()
            .filter(|cm| cm.metadata.name.as_deref() != Some("kube-root-ca.crt"))
            .collect();

        if filtered_list.is_empty() {
            Ok(None)
        } else {
            Ok(Some(filtered_list))
        }
    }

    /// Choose a random transaction to process from the list of transactions.
    async fn select_transaction(
        &self,
        transactions: &[ConfigMap],
    ) -> Result<Option<ConfigMap>, anyhow::Error> {
        // Every ConfigMap in `transactions` has a name in the format `<NODE_NAME>.<NODE_RESOURCE_VERSION>.*`
        // Get all unique `<NODE_NAME>`s from the list of transactions
        // Extract unique node names
        let mut unique_node_names: Vec<String> = transactions
            .iter()
            .filter_map(|config_map| {
                config_map
                    .metadata
                    .name
                    .as_ref()
                    .and_then(|name| name.split('.').next())
                    .map(|s| s.to_string())
            })
            .collect();
        unique_node_names.sort();
        unique_node_names.dedup();

        // Randomly select a node name
        if let Some(selected_node_name) = unique_node_names.choose(&mut rand::thread_rng()) {
            // Find all ConfigMaps that start with the chosen node name
            let node_config_maps: Vec<_> = transactions
                .iter()
                .filter(|config_map| {
                    config_map
                        .metadata
                        .name
                        .as_ref()
                        .map(|name| name.starts_with(selected_node_name))
                        .unwrap_or(false)
                })
                .collect();

            // Sort by resource version and choose the smallest one
            if let Some(selected_config_map) = node_config_maps
                .iter()
                .min_by_key(|config_map| config_map.metadata.resource_version.clone())
            {
                return Ok(Some((*selected_config_map).clone()));
            }
        }
        Ok(None)
    }

    /// Perform leader election by attempting to acquire a lease lock to see if we can modify the
    /// data for this particular node.
    /// While we have an acquired lock, no other replica of this service should be able to modify
    /// that particular node.
    async fn leader_election(
        &self,
        transaction: &ConfigMap,
    ) -> Result<Option<(LeaseLock, LeaseLockResult)>, anyhow::Error> {
        let hostname = std::env::var("HOSTNAME").unwrap();
        let transaction_name = transaction.metadata.name.clone().unwrap();
        let node_name = transaction_name.split('.').next().unwrap();
        // One should try to renew/acquire the lease before `lease_ttl` runs out.
        // E.g. if `lease_ttl` is set to 15 seconds, one should renew it every 5 seconds.
        let leadership = LeaseLock::new(
            kube::Client::try_default().await?,
            "default",
            LeaseLockParams {
                holder_id: hostname,
                // Both node names and lease names can be up to 253 characters long
                lease_name: node_name.to_string(),
                lease_ttl: Duration::from_secs(5),
            },
        );

        // Run this in a background task and share the result with the rest of your application
        let lease = leadership.try_acquire_or_renew().await?;
        // `lease.acquired_lease` can be used to determine if we're leading or not
        if lease.acquired_lease {
            Ok(Some((leadership, lease)))
        } else {
            Ok(None)
        }
    }

    async fn still_leader(lease: &LeaseLock) -> Result<Option<LeaseLockResult>, anyhow::Error> {
        let lease_result = lease.try_acquire_or_renew().await?;
        if lease_result.acquired_lease {
            Ok(Some(lease_result))
        } else {
            Ok(None)
        }
    }

    /// Attempt to restore saved labels to a node.
    /// Returns Ok(Some(transaction)) so that the transaction can be cleaned up if any one of these
    /// is true:
    /// - The transaction is successfully processed.
    /// - The node no longer exists.
    /// - There is no metadata to restore for this node.
    async fn process_transaction_added(
        &self,
        transaction: &ConfigMap,
        lease: &LeaseLock,
    ) -> Result<Option<ConfigMap>, anyhow::Error> {
        let transaction_name = transaction.metadata.name.clone().unwrap();
        let node_name = transaction_name.split('.').next().unwrap();
        match self.node_metadata.get(node_name).await {
            Ok(node_config_map) => {
                match self.all_nodes.get(node_name).await {
                    Ok(node) => {
                        // Read all labels from the ConfigMap named `<NODE_NAME>` in the `NODE_METADATA_NAMESPACE`. Replace all labels on the node.
                        let mut stored_labels = node_config_map.data.unwrap();
                        code_key_slashes(&mut stored_labels, false);
                        if let Some(_lease_result) = Self::still_leader(lease).await? {
                        } else {
                            // I am no longer the leader, do nothing.
                            return Ok(None);
                        }
                        replace_node_labels(&self.all_nodes, &node, &stored_labels).await?;
                        Ok(Some(transaction.clone()))
                    }
                    Err(error) => {
                        match error {
                            // 404 Not found
                            kube::Error::Api(kube::error::ErrorResponse { code, .. })
                                if code == 404 =>
                            {
                                // If the node no longer exists, simply delete the transaction
                                debug!(
                                    "Node {} no longer exists, won't restore anything to it",
                                    node_name
                                );
                                Ok(Some(transaction.clone()))
                            }
                            _ => Err(anyhow::Error::new(error)),
                        }
                    }
                }
            }
            Err(error) => {
                match error {
                    // 404 Not found
                    kube::Error::Api(kube::error::ErrorResponse { code, .. }) if code == 404 => {
                        // If there is no ConfigMap for this node, simply delete the transaction.
                        debug!(
                            "Node Metadata Storage ConfigMap for node {} does not exist",
                            node_name
                        );
                        Ok(Some(transaction.clone()))
                    }
                    _ => Err(anyhow::Error::new(error)),
                }
            }
        }
    }

    /// Handle the case where we want to store node labels and the ConfigMap already exists, so it
    /// needs to be replaced.
    async fn handle_node_label_update(
        &self,
        transaction: &ConfigMap,
        stored_metadata: &ConfigMap,
        lease: &LeaseLock,
    ) -> Result<Option<ConfigMap>, anyhow::Error> {
        let transaction_name = transaction.metadata.name.clone().unwrap();
        let node_name = transaction_name.split('.').next().unwrap();
        if let Some(node_labels) = transaction.data.clone() {
            // TODO: This resource version check is not sufficient. It will always be there because
            // it's inserted by the watcher. We need to do a label_version check.
            if node_labels.get(LABEL_STORE_VERSION_KEY).cloned().is_some() {
                let mut updated_labels = node_labels.clone();
                code_key_slashes(&mut updated_labels, true);
                if let Some(_lease_result) = Self::still_leader(lease).await? {
                } else {
                    // I am no longer the leader, do nothing.
                    return Ok(None);
                }
                debug!("Updating metadata stored for node {}...", node_name);
                replace_config_map_data(&self.node_metadata, stored_metadata, &updated_labels)
                    .await?;
                debug!(
                    "Successfully updated metadata stored for node {}",
                    node_name
                );
                Ok(Some(transaction.clone()))
            } else {
                debug!("Node {} does not have the `{}` label, so ignoring and deleting the transaction", LABEL_STORE_VERSION_KEY, node_name);
                Ok(Some(transaction.clone()))
            }
        } else {
            debug!(
                "There are no labels on the node {}, so ignoring and deleting the transaction",
                node_name
            );
            Ok(Some(transaction.clone()))
        }
    }

    /// Handle the case where we want to store node labels but the ConfigMap needs to be created.
    async fn handle_node_label_creation(
        &self,
        transaction: &ConfigMap,
        lease: &LeaseLock,
    ) -> Result<Option<ConfigMap>, anyhow::Error> {
        let transaction_name = transaction.metadata.name.clone().unwrap();
        let node_name = transaction_name.split('.').next().unwrap();
        let labels = transaction.data.clone().unwrap();
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                namespace: Some(NODE_METADATA_NAMESPACE.to_string()),
                ..Default::default()
            },
            data: Some(labels),
            ..Default::default()
        };
        if let Some(_lease_result) = Self::still_leader(lease).await? {
        } else {
            // I am no longer the leader, do nothing.
            return Ok(None);
        }
        debug!("Creating metadata stored for node {}...", node_name);
        self.node_metadata
            .create(&Default::default(), &config_map)
            .await?;
        debug!(
            "Successfully created metadata stored for node {}",
            node_name
        );
        Ok(Some(transaction.clone()))
    }

    /// This returns the transaction to be deleted if any one of these is true:
    /// - The ConfigMap is successfully created.
    /// - The ConfigMap is successfully updated with all labels.
    /// - The Node does not exist anymore.
    async fn process_transaction_deleted(
        &self,
        transaction: &ConfigMap,
        lease: &LeaseLock,
    ) -> Result<Option<ConfigMap>, anyhow::Error> {
        let transaction_name = transaction.metadata.name.clone().unwrap();
        let node_name = transaction_name.split('.').next().unwrap();
        match self.node_metadata.get(node_name).await {
            Ok(stored_metadata) => {
                // If there already exists a ConfigMap named `<NODE_NAME>` in the `NODE_METADATA_NAMESPACE`, replace it if `LABEL_STORE_VERSION_KEY` is present in the node's labels. If it's not present, do nothing. This is to prevent erasing our stored labels if the node was rapidly added and then deleted before we could restore the saved labels.
                self.handle_node_label_update(transaction, &stored_metadata, lease)
                    .await
            }
            Err(error) => {
                match error {
                    // If there does not already exist a ConfigMap named `<NODE_NAME>` in the `NODE_METADATA_NAMESPACE`, create one with all labels from the node, add the label `LABEL_STORE_VERSION_KEY: <RESOURCE_VERSION>`.
                    kube::Error::Api(kube::error::ErrorResponse { code, .. }) if code == 404 => {
                        // If there is no ConfigMap for this node, simply delete the transaction.
                        debug!(
                            "Node Metadata Storage ConfigMap for node {} does not exist",
                            node_name
                        );
                        // Create a ConfigMap named node_name and put all of the labels from the node into it.
                        self.handle_node_label_creation(transaction, lease).await
                    }
                    _ => Err(anyhow::Error::new(error)),
                }
            }
        }
    }

    async fn process_transaction(
        &self,
        transaction: &ConfigMap,
        lease: &LeaseLock,
    ) -> Result<Option<ConfigMap>, anyhow::Error> {
        let transaction_name = transaction.metadata.name.clone().unwrap();
        if transaction_name.ends_with("added") {
            self.process_transaction_added(transaction, lease).await
        } else if transaction_name.ends_with("deleted") {
            self.process_transaction_deleted(transaction, lease).await
        } else {
            warn!(
                "Got unexpected transaction name that ends with neither `deleted` nor `added`: {}",
                transaction_name
            );
            // We can do nothing and delete the transaction
            Ok(Some(transaction.clone()))
        }
    }

    /// Delete a transaction that has been successfully processed.
    async fn cleanup(&self, transaction: &ConfigMap) -> Result<(), anyhow::Error> {
        let transaction_name = transaction.metadata.name.clone().unwrap();
        debug!("Deleting transaction {}...", transaction_name);
        match self
            .transactions
            .delete(&transaction_name, &Default::default())
            .await
        {
            Ok(_) => {
                debug!("Successfully deleted transaction {}", transaction_name);
                Ok(())
            }
            Err(kube::Error::Api(kube::error::ErrorResponse { code, .. })) if code == 404 => {
                debug!("Transaction {} already deleted", transaction_name);
                Ok(())
            }
            Err(error) => Err(anyhow::Error::new(error)),
        }
    }

    /// State machine loop to continuously process transactions.
    pub async fn process(&self) -> Result<(), anyhow::Error> {
        let interval = Duration::from_millis(500);
        let mut state = TransactionProcessorState::FetchTransactions;
        loop {
            let start_time = std::time::Instant::now();
            match state {
                TransactionProcessorState::FetchTransactions => {
                    // Start the state machine at most once per second
                    let elapsed_time = start_time.elapsed();
                    if elapsed_time < interval {
                        tokio::time::sleep(interval - elapsed_time).await;
                    }
                    debug!("State: FetchTransactions");
                    if let Some(config_maps) = self.fetch_transactions().await? {
                        debug!("There are {} transactions to process...", config_maps.len());
                        debug!("Transactions to process: {:?}", config_maps);
                        state = TransactionProcessorState::SelectTransaction(config_maps);
                    } else {
                        debug!("There are no transactions to process...");
                        state = TransactionProcessorState::FetchTransactions;
                    }
                }
                TransactionProcessorState::SelectTransaction(transactions) => {
                    debug!("State: SelectTransaction");
                    if let Some(transaction) = self.select_transaction(&transactions).await? {
                        state = TransactionProcessorState::LeaderElection(transaction);
                    } else {
                        state = TransactionProcessorState::FetchTransactions;
                    }
                }
                TransactionProcessorState::LeaderElection(transaction) => {
                    debug!("State: LeaderElection");
                    if let Some((lease, lease_result)) = self.leader_election(&transaction).await? {
                        state =
                            TransactionProcessorState::Process(transaction, lease, lease_result);
                    } else {
                        state = TransactionProcessorState::FetchTransactions;
                    }
                }
                TransactionProcessorState::Process(transaction, lease, _lease_lock_result) => {
                    // Keep the lease_lock alive until we're done processing the transaction
                    debug!("State: Process");
                    if let Some(transaction_to_delete) =
                        self.process_transaction(&transaction, &lease).await?
                    {
                        state = TransactionProcessorState::Cleanup(transaction_to_delete);
                    } else {
                        state = TransactionProcessorState::FetchTransactions;
                    }
                }
                TransactionProcessorState::Cleanup(transaction) => {
                    debug!("State: Cleanup");
                    self.cleanup(&transaction).await?;
                    state = TransactionProcessorState::FetchTransactions;
                }
            }
        }
    }
}
