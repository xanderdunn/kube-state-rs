### Problem
Within a Kubernetes cluster, nodes are often added/deleted as they undergo maintenance with cloud providers. When this happens, metadata stored in the Kubernetes “Node” object is lost. This can be undesirable when using dedicated capacity, as you would like some data such as any Node labels to be kept across the node leaving/entering the cluster.

Write a service that will preserve Nodes’ labels if they are deleted from the cluster and re-apply them if they enter back into the cluster. This service itself should be stateless, but can use Kubernetes for any state storage.

### Setup
- Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- [Install minikube](https://minikube.sigs.k8s.io/docs/start/)

### Build
- `cargo build`

### Lint
- ./lint.sh

### Test
- `cargo test`

### Assumptions
- How will this service be run in production? This affects how the lib should be structured.
- This repo uses a local minikube to run integration tests with `cargo test` both locally for dev and in GitHub Action CI.
- This service assumes that every node has a metadata.name, and it assumes that the name will be a unique identifier.
- [Kubernetes ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/) were chosen as the persistent storage mechanism here because it's a simple, built-in key-value store that works for this purpose. We create one `ConfigMap` for each node.
- There's no mechanism here for deleting old stored node metadata, so it will endlessly accumulate in the `ConfigMap`. We will eventually want some way of deleting old data.
- We assume that all labels on a node when it is deleted are exactly what we want to persist. If there is a label stored in the `ConfigMap` that is no longer on the node when it is deleted, it will be removed from the `ConfigMap`.
- We assume a label should be set on a node only if the label is missing. If the label is already set, we do not overwrite it.
- If a `ConfigMap` has no labels left in it, it is deleted.
- A node label key might contain characters `[-._a-zA-Z0-9\/]+`, which is a superset of the characters allowed in a `ConfigMap` key: `[-._a-zA-Z0-9]+`. For example, the label key `beta.kubernetes.io/os` is not compatible with storage as a key in a `ConfigMap`.
