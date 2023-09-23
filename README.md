### Problem
Within a Kubernetes cluster, nodes are often added/deleted as they undergo maintenance with cloud providers. When this happens, metadata stored in the Kubernetes “Node” object is lost. This can be undesirable when using dedicated capacity, as you would like some data such as any Node labels to be kept across the node leaving/entering the cluster.

Write a service that will preserve Nodes’ labels if they are deleted from the cluster and re-apply them if they enter back into the cluster. This service itself should be stateless, but can use Kubernetes for any state storage.

### Architecture Overview
We have two processes, one responsible for storing node metadata, and one responsible for setting node metadata on nodes. Each process iterates over the nodes in the cluster in a loop, looking for changes to either store or set on nodes. Liveness is achieved by running both processes as Kubernetes pod services with `N` replicas. Each replica is responsible for `1/N` of the nodes in the cluster. Consistency is eventual, we can expect a short delay between a node entering a cluster and the metadata being set on the node. Similarly, we can expect a short delay between someone modifying the metadata on a node and our service storing the updated metadata. The delay is dependent on the number of replicas `N` and the number of nodes in the cluster, but is generally expected to be sub-minute.

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
- This repo uses a local minikube to run integration tests with `cargo test` both locally for dev and in GitHub Action CI.
- This service assumes that every node has a `metadata.name`, and it assumes that the name will be a unique identifier.
- [Kubernetes ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/) were chosen as the persistent storage mechanism here because it's a simple, built-in key-value store that works for this purpose. We create one `ConfigMap` for each node. We assume that the creation and replacement of `ConfigMap`s is cheap, and that any particular node will have a relatively small number of labels (not hundreds or thousands of labels per node).
- We assume that all labels on a node when it is deleted are exactly what we want to persist. If there is a label stored in a `ConfigMap` that is no longer on the node when it is deleted, it will be removed from the `ConfigMap`.
- We assume a label should be set on a node only if the label is missing. If the label is already set, we do not overwrite it.
- If a `ConfigMap` has no labels left in it, it is deleted. However, there is no mechanism implemented for deleting old, non-empty `ConfigMap` data, for example when a node is deleted forever and will not be re-entering the cluster. Hence, the `ConfigMap`s will accumulate. We will eventually want some way of deleting old data.
- A node label key might contain characters `[-._a-zA-Z0-9\/]+`, which is a superset of the characters allowed in a `ConfigMap` key: `[-._a-zA-Z0-9]+`. For example, the label key `beta.kubernetes.io/os` is not compatible with storage as a key in a `ConfigMap`. As a workaround, we encode all slashes as the reserved token `-SLASH-` when stored as keys in the `ConfigMap` and then decode those tokens into `/` when restoring labels.
- We run this service as a Docker containerized pod with multiple replicas.
- Assume up to 100,000 nodes in a cluster.
- Assume that our nodes have synchronized clocks.
- Assume that we have our nodes distributed across data centers, regions, zones, geographical areas, etc.
- We want 99.9% liveness. Losing a label change is to be avoided with high certainty.
- A delay of several minutes in restoring latest labels to a node should be unlikely but is not a big problem.
- We aren't dealing with security concerns at this level of the stack.
- We expect strong consistency from Kubernetes, ConfigMaps, and etcd. When you read a ConfigMap after writing to it, you can expect to get the value you wrote. We expect Kubernetes `POST`, `PUT`, and `DELETE` are atomic. The etcd datastore ensures that these operations either fully succeed or fully fail, maintaining consistency.
- If a change to node metadata is made seconds before the node is deleted, the change can be lost. We assume this is acceptable. If label modifications moments before deletion are expected to happen in production, an alternate implementation will be required.

### Fault Tolerance
- Crash: The service crashes. We have many replicas with leader election, so when one crashes another will take over. In addition, we have automatic restart.
- Transient: A particular node fails long enough that our leader election times out, and then returns to working. In this case we avoid duplicated work by checking an incrementing counter stored in the ConfigMap.
- Permanent: In this case some other replica will eventually be chosen. Ideally a liveness and readiness check would fail and Kubernetes would replace this node.
- Hardware: A node's hardware dies. Leader election should find another node to continue operation.
- Network: If a node experiences a network failure, a new leader will eventually be chosen.
- Response: Imagine a situation where etcd returns stale data on a particular node. We iterate through our nodes ever few minutes and make sure 

### TODO
- Fix test_not_overwriting_labels
- Split the iteration of nodes across `num_replicas`
- Increase `num_replicas` > 1
- Namespace the ConfigMap names. They could all start with `label_storage.`
- Split the storage and restoring into two separate processes
- Handle the situation where `num_replicas` > `num_nodes`
- Add logging - what output is used? DataDog?
- Add metrics - what output is used? DataDog?
    - An interesting metric would be (# of nodes we think have latest labels) / (total # of nodes)
    - Imagine something is continuously setting the labels to old values. Some metric to understand churn would be useful.
- Add Kubernetes liveness and readiness check with restart
