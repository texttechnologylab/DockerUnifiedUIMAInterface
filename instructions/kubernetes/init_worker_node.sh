#! /usr/bin/bash
# Author: Markos Genios, Filip Fitzermann
# Assumes running on Ubuntu 24.04


# apt update
sudo apt update -y
sudo apt upgrade -y

# Permanently disable swap
sudo swapoff -a
sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab

# Install Docker
sudo apt install -y docker.io
sudo systemctl enable --now docker

# Install auxiliary packages
sudo apt-get install -y apt-transport-https ca-certificates curl gpg
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.30/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
sudo apt update

# Install kubelet, kubeadm and kubectl and prevent them from being updated in the future
sudo apt install -y kubelet kubeadm kubectl
sudo apt-mark hold kubelet kubeadm kubectl
sudo systemctl enable --now kubelet
