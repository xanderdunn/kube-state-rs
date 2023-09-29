#!/usr/bin/env bash

# Kill the pod on exit
trap 'kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs-watcher' | xargs -I {} kubectl delete pod {}' EXIT

set -e

eval $(minikube -p minikube docker-env)

docker build -t kube-state-rs-watcher -f ./Dockerfile_watcher .
kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs-watcher' | xargs -I {} kubectl delete pod {}
kubectl apply -f kube-state-rs-watcher.yaml
# kubectl describe pod
kubectl get pods -l app=kube-state-rs-watcher
# kubectl logs -f
# Wait for the pod to be ready
# sleep 3
# cargo test
