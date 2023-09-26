#!/usr/bin/env bash

# Kill the pod on exit
trap 'kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs' | xargs -I {} kubectl delete pod {}' EXIT

set -e

eval $(minikube -p minikube docker-env)

docker build -t kube-state-rs .
kubectl apply -f kube-state-rs.yaml
# kubectl describe pod
kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs' | xargs -I {} kubectl delete pod {}
kubectl get pods -l app=kube-state-rs
# kubectl logs -f
# Wait for the pod to be ready
sleep 3
kubectl logs -f kube-state-rs-0 &
kubectl logs -f kube-state-rs-1 &
kubectl logs -f kube-state-rs-2 &
kubectl logs -f kube-state-rs-3 &
cargo test
