#!/usr/bin/env bash

set -e

eval $(minikube -p minikube docker-env)

docker build -t kube-state-rs .
kubectl apply -f kube-state-rs.yaml
# kubectl describe pod
#kubectl delete pod 
kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep 'kube-state-rs' | xargs -I {} kubectl delete pod {}
kubectl get pods -l app=kube-state-rs
# kubectl logs -f
# cargo test
