 #! /usr/bin/bash
 # Author: Filip Fitzermann

 # Does not reset iptables
 sudo kubeadm reset
 sudo rm -r /etc/cni/net.d
 sudo rm -r $HOME/.kube
