#!/bin/sh

# script must be run as root

#apt install -y nfs-common
#mkdir nfsmount

mount -v -t nfs -o vers=3,proto=tcp,nolock,noacl,sync bigdata1:/ nfsmount

# windows
#mount -o nolock bigdata1:/! Z:
