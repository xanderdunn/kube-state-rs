### Problem
Within a Kubernetes cluster, nodes are often added/deleted as they undergo maintenance with cloud providers. When this happens, metadata stored in the Kubernetes “Node” object is lost. This can be undesirable when using dedicated capacity, as you would like some data such as any Node labels to be kept across the node leaving/entering the cluster.

Write a service that will preserve Nodes’ labels if they are deleted from the cluster and re-apply them if they enter back into the cluster. This service itself should be stateless, but can use Kubernetes for any state storage.

### Setup
- Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- [Install minikube](https://minikube.sigs.k8s.io/docs/start/)

### Build
- `cargo build`

### Test
- `cargo test`

### Questions
- How will this service be run in production? This affects how the lib should be structured.
- This repo uses a local minikube to run integration tests with `cargo test` both locally for dev and in GitHub Action CI. This should be modified to match however tests are run for the team's other Kubernetes services.
- This service assumes that every node has a metadata.name, and it assumes that the name will be a unique identifier. If that's not a safe assumption, where will the unique identifier be?

### TODO
- Using logging rather than println
- Add a second integration test and deal with making sure minikube is in the same state at the start of each test
- Add docstrings
