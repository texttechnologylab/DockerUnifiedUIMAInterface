#! /usr/bin/bash
# Author: taken from https://helm.sh/docs/intro/install/

# Install helm, a package manager for Kubernetes
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
