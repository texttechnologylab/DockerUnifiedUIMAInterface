#! /usr/bin/bash
# Author: Filip Fitzermann

sudo swapoff -a
sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab
