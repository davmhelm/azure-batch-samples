#!/bin/bash

# node_prep.bash
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.


# Create partition on empty disk
sed -e 's/\s*\([\+0-9a-zA-Z]*\).*/\1/' << FDISK_CMDS  | sudo fdisk /dev/sdc
o      # create new DOS partition table
n      # add new partition
p      # specify primary partition type
1      # partition number
       # partition start - default sector offset from 0
       # partition size - default use full disk
t      # set partition type (auto-selects only partition)
83     # Linux filesystem
w      # write partition table and exit
FDISK_CMDS

# Format new partition for use
sudo mkfs -t ext4 -b 4k -q /dev/sdc1

# Create directory for mountpoint
sudo mkdir /datadrive

# Mount file system
sudo mount -o defaults,nofail,barrier=0 /dev/sdc1 /datadrive

# Prepare paths for use
sudo mkdir -p /datadrive/var/lib/docker
sudo mkdir -p /datadrive/var/run/docker

# Reconfigure docker to use data paths
echo "{ \"data-root\": \"/datadrive/var/lib/docker\", \"exec-root\": \"/datadrive/var/run/docker\" }" | sudo tee /etc/docker/daemon.json

# Restart docker to uptake new config
sudo systemctl restart docker

