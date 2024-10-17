#! /usr/bin/bash
# Taken from https://github.com/NVIDIA/k8s-device-plugin#deployment-via-helm

# Install NVIDIA device plugin for Kubernetes
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm repo update
helm upgrade -i nvdp nvdp/nvidia-device-plugin \
	--namespace nvidia-device-plugin \
	--create-namespace \
	--version 0.16.2

