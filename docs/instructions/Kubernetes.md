---
layout: default
---

# Introduction
This guide is for the script-based setup and configuration of a Kubernetes cluster in order to be used later by DUUI and the associated Kubernetes cluster.

# Simple Kubernetes Install Scripts
These shell-scripts were made and used for an easier installation of a Kubernetes cluster on a network of Ubuntu 22.04 systems. I recommend to start on a fresh installation of Ubuntu 22.04.


# TLDR
- **Create cluster**: Run `init_master_node.sh` <a href="https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/blob/main/instructions/kubernetes/init_master_node.sh"><img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/scroll.svg" width="15" height="15"></a> to create the cluster and make the system a master node.
- **Add worker node**: Run `init_worker_node.sh` <a href="https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/blob/main/instructions/kubernetes/init_worker_node.sh"><img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/scroll.svg" width="15" height="15"></a> to prepare the system to be added as a worker node. Run `kubeadm_join_command.sh` <a href="https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/blob/main/instructions/kubernetes/kubeadm_join_command.sh"><img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/scroll.svg" width="15" height="15"></a>  on the master node and execute output on the to be worker node.
- **GPU capabilities**: Run `enable_gpu.sh` <a href="https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/blob/main/instructions/kubernetes/gpu/enable_gpu.sh"><img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/scroll.svg" width="15" height="15"></a> on a worker node with an NVIDIA GPU to make its gpu usable by the cluster. Change files depending on the chosen container runtime; more on this in the section [Configure Docker or Containerd](#configure-docker-or-containerd-for-gpu).



# Basic Cluster setup
## init_master_node
> Run on a system to initialze a cluster and make it a master node.

Installs Kubernetes packages and makes the system a master node (control plane). Works only for the first master node. For adding consecutive master nodes to the same cluster, the procedure is different.
Installs calico as the pod networking plugin.
Permanently disables swap on the system.

## init_worker_node
> Run on a system that will be a worker node.

Installs Kubernetes packages so the node is ready to be added to the cluster as a worker node. To add the system as a worker node run `kubeadm token create --print-join-command` on the master node and execute the output on the system to be added as a worker node.
On the worker node, check if the node has been successfully added by running `kubectl get nodes`.

## kubeadm_join_command
> Run output on worker nodes to add them to the cluster.

When executed on the initial master node, prints the command that needs to be executed on a system for it to be added to the cluster as a worker node.

## reset_kubeadm
Effectively removes a node from the cluster. Runs the `kubeadm reset` command and does some further cleanup.

## perma_disable_swap
Permanently disables swap by commenting out every line in the `/etc/fstab` file containing the word
" swap ". Not actually used as this functionality is included in the other scripts that need it.

## setup_networking
Makes changes to networking settings. Not used in the cluster setup. Left in just in case for future use.


# GPU
## enable_gpu
> Execute on a node for it to be able to run GPU workloads.

After running this scripts also run `kubectl describe node <gpu-node-name>` and look for the "Allocatable" section. If it has the `nvidia.com/gpu` with a value of "1" or more, like this:
```
Allocatable:
  cpu:                12
  ephemeral-storage:  423797574979
  hugepages-1Gi:      0
  hugepages-2Mi:      0
  memory:             32701280Ki
  nvidia.com/gpu:     1
```
then the installation was successful. Bear in mind that even after correctly installing all the necessary packages and plugins it can take some time until Kubernetes recognizes the GPU on a worker node.
This script has the combined functionality of all of the following gpu scripts.

### Configure Docker or Containerd for GPU
Before the device plugin can function changes must be made to some files depending on the chosen container runtime on a node. In this installation process we are using containerd, but if you are unsure, run `kubectl get nodes -o wide` to list all nodes and check their configured container runtime.


**For containerd as a runtime (used in this whole installation process):**
Create or modify the file `/etc/containerd/config.toml` to contain the following:
```
version = 2
[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
    [plugins."io.containerd.grpc.v1.cri".containerd]
      default_runtime_name = "nvidia"

      [plugins."io.containerd.grpc.v1.cri".containerd.runtimes]
        [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia]
          privileged_without_host_devices = false
          runtime_engine = ""
          runtime_root = ""
          runtime_type = "io.containerd.runc.v2"
          [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia.options]
            BinaryName = "/usr/bin/nvidia-container-runtime"
```
Afterwards run `sudo nvidia-ctk runtime configure --runtime=containerd` and restart the containerd service using `systemctl restart containerd`.

---

**For docker as a runtime:**
Create or modify the file `/etc/docker/daemon.json` to contain the following:
```
{
    "default-runtime": "nvidia",
    "runtimes": {
        "nvidia": {
            "path": "/usr/bin/nvidia-container-runtime",
            "runtimeArgs": []
        }
    }
}
```
Afterwards run `sudo nvidia-ctk runtime configure --runtime=docker` and restart the docker service using `systemctl restart docker`.


## helm_install
Installs helm, a package manager for Kubernetes. Helm is used to install the NVIDIA device plugin for Kubernetes. Run `helm version --short` to check the installation.

## nvidia_container_toolkit_install
Installs the nvidia-container-tookit, needed by the NVIDIA device plugin for Kubernetes. Run `nvidia-ctk --version` to check the installation.

## device_plugin_install
Installs the NVIDIA device plugin for Kubernetes using helm. Run `helm list -A` to check if the plugin was installed successfully.


# Possible errors
If there is a problem with a node, first try restarting the `docker`, `containerd` and `kubelet` services on that node by running `sudo systemctl restart docker containerd kubelet`.

## Container runtime is not running
```
root@kubemaster:~$ sudo kubeadm init
    [ERROR CRI]: container runtime is not running: output: time="2022-05-20T02:06:28Z"level=fatal msg="getting status of runtime: rpc error: code = Unimplemented desc = unknown service runtime.v1alpha2.RuntimeService"
```
If this error comes up while initializing the master node or adding a worker to the cluster, try restarting the `docker` and `containerd` services by running `systemctl restart docker containerd`. If this does not solve the problem delete the file `/etc/containerd/config.toml`, restart containerd and try again.

## kubectl: Connection refused
The error "Connection refused" when running a kubectl command often means that swap is not turned off.
Turn off swap temporarily by running `sudo swapoff -a` or run the `perma_disable_swap.sh` script.
Some PCs turn swap back on after reboot, even after "permanently" turning it off by changing the `/etc/fstab` file.

## Node does not have status READY
If a node does not change its status to READY after a while (typically max. 10 minutes) you can restart the services needed by Kubernetes by running `sudo systemctl restart docker containerd kubelet`. If that does not help, restart the system. If that also fails look at the events in the node by running `kubectl describe node <node-name>` and try to solve the problem from there.
