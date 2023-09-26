### Problem
Within a Kubernetes cluster, nodes are often added/deleted as they undergo maintenance with cloud providers. When this happens, metadata stored in the Kubernetes "Node" object is lost. This can be undesirable when using dedicated capacity, as you would like some data such as any Node labels to be kept across the node leaving/entering the cluster.

Write a service that will preserve Nodes’ labels if they are deleted from the cluster and re-apply them if they enter back into the cluster. This service itself should be stateless, but can use Kubernetes for any state storage.

### Architecture Overview
We have a service that runs as a replicable, horizontally scalable Kubernetes StatefulSet. The service iterates through nodes in the cluster both looking for changes in node metadata to store, as well as missing metadata on nodes to restore. The service iterates over the nodes in the cluster in a loop, looking for changes to either store or set on nodes. Liveness is achieved by running as a Kubernetes pod with `N` replicas. Each replica is responsible for `1/N` of the nodes in the cluster. If one of the replicas goes down, the remaining replicas will adjust their `1/N` partitions to cover all nodes on their next iteration through the nodes. Consistency is eventual, we can expect a short delay between a node entering a cluster and the metadata being set on the node. Similarly, we can expect a short delay between someone modifying the metadata on a node and our service storing the updated metadata. The delay is dependent on the number of replicas `N` and the number of nodes in the cluster, but is generally expected to be sub-minute. Note that we use a StatefulSet so that our replicas have an index that's available, but the service itself is stateless and the replicas are interchangeable.

### Setup
- Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- [Install minikube](https://minikube.sigs.k8s.io/docs/start/)

### Build
- `cargo build`

### Lint
- `./lint.sh`

### Test
- `cargo test`

### Example Local Dev Usage
Here is some example usage of the binary:
- Run the binary: `cargo run`
- Create a node:
```
echo '{
  "apiVersion": "v1",
  "kind": "Node",
  "metadata": {
    "name": "my-new-node"
  },
  "spec": {
    "taints": [
      {
        "effect": "NoSchedule",
        "key": "node.kubernetes.io/unschedulable",
        "value": "true"
      }
    ]
  }
}' | kubectl create -f -
```
- You'll see the service print debug messages that a node has been added
- Add a label to the node: `kubectl label nodes my-new-node my_key=my_test_value`
- Delete the node: `kubectl delete node my-new-node`
- Print the `ConfigMap` to see that the label was stored: `kubectl get configmap my-new-node -o yaml`
- Add the node back to the cluster with the same command as above.
- See that the labels have been restored on the node: `kubectl get nodes my-new-node --show-labels`

### Assumptions
- For this implementation I assume that node labels can be modified by anyone from anywhere at anytime. If we were to instead apply all label changes through a particular pod service, this problem would be much easier to solve. That service could store the metadata, and a separate service could trigger applying the metadata to the node by reading from storage.
- This repo uses a local minikube to run integration tests with `cargo test` both locally for dev and in GitHub Action CI.
- This service assumes that every node has a `metadata.name`, and it assumes that the name will be a unique identifier. Suppose unique node names `node-1...node-10000`. I assume that when `node-n` leaves the cluster, we can expect `node-n` to return to the cluster, rather than always returning to the cluster with a new identifier, such as `node-10001`. If that's not the case, then we need a zookeeper strategy to apply labels to at most one node, regardless of node identifier.
- We assume that the frequency of node metadata changes is approximately the same across all nodes. If it weren't then we might want to switch from a static `1/N` partitioning strategy to a dynamic partitioning strategy.
- [Kubernetes ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/) were chosen as the persistent storage mechanism here because it's a simple, built-in key-value store that works for this purpose. We create one `ConfigMap` for each node. We assume that the creation and replacement of `ConfigMap`s is cheap, and that any particular node will have a relatively small number of labels (not hundreds or thousands of labels per node).
- We assume that all labels on a node when it is deleted are exactly what we want to persist. If there is a label stored in a `ConfigMap` that is no longer on the node when it is deleted, it will be removed from the `ConfigMap`.
- We assume a label should be set on a node only if the label is missing. If the label is already set, we do not overwrite it.
- If a `ConfigMap` has no labels left in it, it is deleted. However, there is no mechanism implemented for deleting old, non-empty `ConfigMap` data, for example when a node is deleted forever and will not be re-entering the cluster. Hence, the `ConfigMap`s will accumulate. We will eventually want some way of deleting old data.
- A node label key might contain characters `[-._a-zA-Z0-9\/]+`, which is a superset of the characters allowed in a `ConfigMap` key: `[-._a-zA-Z0-9]+`. For example, the label key `beta.kubernetes.io/os` is not compatible with storage as a key in a `ConfigMap`. As a workaround, we encode all slashes as the reserved token `---SLASH---` when stored as keys in the `ConfigMap` and then decode those tokens into `/` when restoring labels.
- We run this service as a Docker containerized pod with multiple replicas.
- Up to 40,000 nodes in a cluster.
- Assume that we have the replicas of this service distributed across data centers, regions, zones, geographical areas, etc.
- We want 99.9% liveness. Losing a label change is to be avoided with high certainty.
- A delay of several minutes in restoring latest labels to a node should be unlikely but is not a big problem.
- We aren't dealing with security concerns at this level of the stack.
- We expect strong consistency from Kubernetes, ConfigMaps, and etcd. When you read a ConfigMap after writing to it, you can expect to get the value you wrote. We expect Kubernetes `POST`, `PUT`, and `DELETE` are atomic. The etcd datastore ensures that these operations either fully succeed or fully fail, maintaining consistency.
- If a change to node metadata is made seconds before the node is deleted, the change can be lost. We assume this is acceptable. If label modifications moments before deletion are expected to happen in production, an alternate implementation will be required, see [#6](https://github.com/xanderdunn/kube-state-rs/issues/6).
- Idempotency
    - Every time a label is set on a node, all labels are set, replacing all labels on the node. This includes a `label_version`, which is incremented each time there is a change in the labels.
    - Suppose a node is brought back into a cluster with a non-empty label set. If there is a missing or outdated `label_version`, all labels will be replaced. If it has a newer `label_version` than what we have in the DB, all labels in the DB will be replaced with what's on the node.
    - Suppose the node has the same `label_version` as what's in the DB, but the labels are different. The node's labels will be completely replaced with what's in the DB.
    - Because transactions are idempotent, we could have multiple replicas operating on the same partition of nodes to achieve 0 downtime.

### Fault Tolerance
We want to achieve liveness and eventual consistency in the face of:
- Crash: The service crashes. We have many replicas, so when one crashes another will take over. In addition, we have automatic restart.
- Transient: A particular node fails, and then returns to working. In this case it could be possible that an older `label_version` gets applied after a newer version. This will quickly be corrected on the next iteration of the nodes. Additionally, because all node label changes are idempotent, applying the same `label_version` more than once produces the same result.
- Permanent: In this case some other replica will eventually be chosen. Ideally a liveness and readiness check would fail and Kubernetes would replace this node.
- Hardware: A node's hardware dies. Kubernetes should remove this replica and replace it. Kubernetes node metadata updates are atomic, so any operation should completely succeed or completely fail.
- Network: This could be considered a crash or transient failure. Label updates both on nodes and in storage are atomic and idempotent.
- Response: Stale information should not occur because etcd has strong consistency guarantee. If it were to happen, it will be fixed as soon as current information is available.
