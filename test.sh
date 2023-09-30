#!/usr/bin/env bash

# Kill the pod on exit
trap 'kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs' | xargs -I {} kubectl delete pod {}' EXIT

set -e

eval $(minikube -p minikube docker-env)

# Build Docker images
docker build -t kube-state-rs-watcher -f ./Dockerfile_watcher . &
pid1=$!
docker build -t kube-state-rs-processor -f ./Dockerfile_processor . &
pid2=$!
wait $pid1 $pid2

kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs' | xargs -I {} kubectl delete pod {}
kubectl apply -f kube-state-rs-watcher.yaml
kubectl apply -f kube-state-rs-processor.yaml
# kubectl describe pod
kubectl get pods -l app=kube-state-rs-watcher
kubectl get pods -l app=kube-state-rs-processor
# kubectl logs -f
# Wait for the pod to be ready
sleep 3
cargo test test_add_and_remove_nodes
