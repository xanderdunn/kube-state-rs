## Problem
Within a Kubernetes cluster, nodes are often added/deleted as they undergo maintenance with cloud providers. When this happens, metadata stored in the Kubernetes "Node" object is lost. This can be undesirable when using dedicated capacity, as you would like some data such as any Node labels to be kept across the node leaving/entering the cluster.

Write a service that will preserve Nodes’ labels if they are deleted from the cluster and re-apply them if they enter back into the cluster. This service itself should be stateless, but can use Kubernetes for any state storage.

# High Level Design
We have two services. One stores versioned transactions on node addition or deletion. The other service processes these transactions in a FIFO manner. We achieve: Arbitrarily horizontally scalable, highly available, arbitrarily replicable, idempotent versioned transactions, eventual consistency, per-node parallelism (per-node leader election) across a FIFO queue of transactions, stateless services. Relies on the strong consistency of Kubernetes ConfigMaps, which use RAFT-based etcd.

### Service: Watcher
- Node Added Event
    - Create a ConfigMap transaction with the name `<NODE_NAME_HASH>.<NODE_RESOURCE_VERSION>`, with `data: { "type": "added" }` in the `TRANSACTION_NAMESPACE`.
- Node Deleted Event
    - Store all labels in a ConfigMap transaction with name `<NODE_NAME_HASH>.<NODE_RESOURCE_VERSION>` with `data: {"type": "deleted"}`, in the `TRANSACTION_NAMESPACE`.

Replicas of the Watcher service provide redundant work to ensure high availability.
Node names are SHA256 hashed to a fixed 64 hexadecimal character length to prevent exceeding the 253 character limit of a ConfigMap name.

### Service: Transaction Processor
- Loop
    - 1: Get all ConfigMaps in the `TRANSACTION_NAMESPACE`
    - 2: Choose a random node to process from the list of transactions.
    - Leader election using a lease lock on that node’s name
        - Lease successful
            - Sort the transactions for this node by ResourceVersion, process the one with the smallest ResourceVersion.
                - If it's a transaction.deleted
                   - If there does not already exist a ConfigMap named `<NODE_NAME>` in the `NODE_METADATA_NAMESPACE`, create one with all labels from the node, add the label `labels_restored: <RESOURCE_VERSION>`.
                   - If there already exists a ConfigMap named `<NODE_NAME>` in the `NODE_METADATA_NAMESPACE`, replace it if `labels_restored` is present in the node's labels. If it's not present, do nothing. This is to prevent erasing our stored labels if the node was rapidly added and then deleted before we could restore the saved labels.
                - If it's a transaction.added, read all labels from the ConfigMap named `<NODE_NAME>` in the `NODE_METADATA_NAMESPACE`. Replace all labels on the node. If the node no longer exists, simply delete the transaction. If there is no ConfigMap for this node, simply delete the transaction.
            - If successfully processed, delete this particular transaction ConfigMap. Go to 1.
        - Lease failed. Go to 1.

Replicating the Transaction Processor service provides both horizontal scaling and high availability. Each replica does non-overlapping work to process the transactions recorded by the Watcher.

### Potential Issues:
- There is a time period after adding a node where it is not safe to make edits to the labels on that node because they could be overwritten by the Transaction Processor until it has processed the `transaction.added` for that node, restoring all stored labels.
- We are potentially abusing the intended usage of ConfigMap and Lease here. We could have 5,000 different ConfigMaps and Leases in worst case scenario. This is within the limits imposed by Kubernetes, but may produce a lot of network traffic for the Kubernetes API. See [#3](https://github.com/xanderdunn/kube-state-rs/issues/3) for moving to a proper database such as Redis.
- Limitation: If the ResourceVersion of a particular node ever became larger than `10**188 - 1`, then the transaction ConfigMap naming scheme `<NODE_NAME_HASH>.<NODE_RESOURCE_VERSION>` would no longer work. This is 253 characters maximum for a ConfigMap name - 64 characters for a SHA256 hash - 1 character = 188 characters to encode the `NODE_RESOURCE_VERSION`.

### Edge Cases:
- We have a node that exists long enough to accumulate labels. We rapidly delete it, add it back, and then delete it again, all before the labels could be restored. So we will have a non-empty transaction.deleted, then a transaction.added, and then an empty transaction.deleted. The first transaction will successfully store all labels. The second transaction will fail to restore labels because by the time it's processed the node has been deleted again. The third transaction will be thrown out because it has a `label_version` of 0, whereas the stored has a higher label version, so we do nothing, thus preventing us from losing all of our stored labels.
- A label is changed immediately before the node is deleted. This will be successfully preserved in the `transaction.deleted` because the `WatchEvent::Deleted` will contain the node's state at the time of deletion.
- A label is changed immediately after a node is created, before the label restoration occurs. Or, a node is created with a non-empty label set. Any label changes prior to the label restoration will be overwritten when the Transaction Processor processes the `transaction.added` for that node.

## Setup
- Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- [Install minikube](https://minikube.sigs.k8s.io/docs/start/)

## Build
- `cargo build`

## Lint
- `./lint.sh`

## Test
- `cargo test`

## Assumptions
- It's important when we deploy this that we make multiple nodes across different regions available to the service, and that region is set in the key `kubernetes.io/hostname`.
- Node labels can be modified by anyone from anywhere at anytime. If we were to instead apply all label changes through a particular pod service with an exposed API, this problem would be much easier to solve. That service could store the metadata, and a separate service could trigger applying the metadata to the node by reading from storage. See [#6](https://github.com/xanderdunn/kube-state-rs/issues/6) for details.
- Every node has a `metadata.name`, and it assumes that the name will be a unique identifier. Suppose unique node names `node-1...node-5000`. I assume that when `node-n` leaves the cluster, we can expect `node-n` to return to the cluster, rather than always returning to the cluster with a new identifier, such as `node-5001`. If that's not the case, then we need a zookeeper strategy to apply labels to at most one node, regardless of node identifier.
- The frequency of node metadata changes is approximately the same across all nodes.
- One ConfigMap per node is used for storage. Any particular node will have a relatively small number of labels (not hundreds or thousands of labels per node).
- A node label key might contain characters `[-._a-zA-Z0-9\/]+`, which is a superset of the characters allowed in a `ConfigMap` key: `[-._a-zA-Z0-9]+`. For example, the label key `beta.kubernetes.io/os` is not compatible with storage as a key in a `ConfigMap`. As a workaround, we encode all slashes as the reserved token `---SLASH---` when stored as keys in the `ConfigMap` and then decode those tokens into `/` when restoring labels.
- Up to 5,000 nodes in a cluster. More nodes will be managed with cluster federation.
- We want 99.9% availability. Losing a label change is to be avoided with high certainty.
- `ResourceVersion` is monotonically increasing.
- Restoring labels to Nodes only succeeds if the ResourceVersion we expect is the actual ResourceVersion at the time our change is applied by etcd. If a Node is being rapidly modified by other services after it has been added to the cluster, it will take time for our Transaction Processor to restore labels to that Node.
- Strong consistency from the Kubernetes API, ConfigMaps, and etcd. When we read a ConfigMap after writing to it, we can expect to get the value that was written. We expect Kubernetes `POST`, `PUT`, and `DELETE` are atomic. The etcd datastore ensures that these operations either fully succeed or fully fail, maintaining consistency.
- Idempotency
    - Every time a label is set on a node, all labels are set, replacing all labels on the node. This includes a `labels_restored` label, which holds the ResourceVersion of the node when its labels were stored.
    - Suppose a node is brought back into a cluster with a non-empty label set. These labels will be replaced with what is stored.

## Fault Tolerance
We want to achieve availability and eventual consistency in the face of:
- Crash: The service crashes. We have many replicas, so when one crashes another will take over. In addition, we have automatic restart. A transaction is removed from the queue only if it has fully succeeded. If a transaction is executed more than once, the result will be the same.
- Transient: Suppose a replica of our Transaction Processor is trying to restore labels to a node and it stalls, takes much longer than usual for some reason, but eventually succeeds. During that stall, some other replica may have already applied that change, as well as several more recent changes in the queue. When applying changes to Node metadata or ConfigMaps, the change succeeds only if the object's ResourceVersion is what we expect. In this scenario the replica with the transient fault will have an outdated ResourceVersion so no change will occur, the latest change will remain.
- Permanent: In this case some other replica will eventually be chosen. A liveness and readiness check will fail and Kubernetes will replace this node.
- Hardware: A node's hardware dies. Kubernetes will remove this replica and replace it. Kubernetes node metadata updates are atomic, so any operation should completely succeed or completely fail.
- Network: This could be considered a crash or transient failure. Label updates both on Nodes and on ConfigMaps are atomic and idempotent.
- Response: Stale information should not occur because etcd has a strong consistency guarantee.
