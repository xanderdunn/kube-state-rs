### Problem
Within a Kubernetes cluster, nodes are often added/deleted as they undergo maintenance with cloud providers. When this happens, metadata stored in the Kubernetes “Node” object is lost. This can be undesirable when using dedicated capacity, as you would like some data such as any Node labels to be kept across the node leaving/entering the cluster.

Write a service that will preserve Nodes’ labels if they are deleted from the cluster and re-apply them if they enter back into the cluster. This service itself should be stateless, but can use Kubernetes for any state storage.

### Setup
- Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- [Install minikube](https://minikube.sigs.k8s.io/docs/start/)

### Build
- `cargo build`

### Lint
- `./lint.sh`

### Test
- `cargo test`

### Usage
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
- To run this service in production, let's assume it will run as an Ubuntu service. For this we need a binary that starts a Kubernetes client from a particular Kubernetes config. In this binary we do not exit on error, we log the error and proceed. As a result, we will want to set up logging to some service such as DataDog and notify on error.
- Assume up to 100,000 nodes in a cluster.
