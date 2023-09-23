#!/usr/bin/env bash

# Kill the pod on exit
trap 'kubectl scale deployment kube-state-rs --replicas=0' EXIT

set -e

eval $(minikube -p minikube docker-env)

docker build -t kube-state-rs .
kubectl apply -f kube-state-rs.yaml
# kubectl describe pod
kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs' | xargs -I {} kubectl delete pod {}
kubectl get pods -l app=kube-state-rs
# kubectl logs -f
sleep 1
kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs' | xargs -I {} kubectl logs -f {} &
cargo test
