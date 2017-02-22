#!/bin/sh

if [ 1 -eq 0 ]; then
echo -e "\nMapr: Creating data disks\n"
gcloud compute disks create "mapr1-disk" "mapr2-disk" "mapr3-disk" "mapr4-disk" "mapr5-disk" --size "200" --type "pd-standard"

echo -e "\nMapr: Creating swap disks\n"
gcloud compute disks create "mapr1-swap-disk" "mapr2-swap-disk" "mapr3-swap-disk" "mapr4-swap-disk" "mapr5-swap-disk" --size "20" --type "pd-ssd"

for i in `seq 1 5`
do
     echo -e "\nMapr: Creating VM $i\n"
     gcloud compute instances create "mapr$i" --machine-type "n1-standard-4" --disk name=mapr$i-disk,device-name=mapr$i-disk --disk name=mapr$i-swap-disk,device-name=mapr$i-swap-disk --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20160114e" --boot-disk-size 40 --boot-disk-type "pd-standard" --boot-disk-device-name "mapr$i"
done
fi

if [ 1 -eq 0 ]; then
for i in `seq 1 5`
do
     echo -e "\nMapr$i: Updating VM $i\n"
     gcloud compute ssh ubuntu@mapr$i 'sudo apt-get update;sudo apt-get -y upgrade'
done
for i in `seq 1 5`
do
     echo -e "\nMapr$i: Configuring swap for VM $i\n"
     gcloud compute ssh ubuntu@mapr$i 'sudo mkswap /dev/sdc;sudo swapon /dev/sdc'
done
fi

if [ 1 -eq 0 ]; then
gcloud compute ssh ubuntu@mapr1 'wget http://package.mapr.com/releases/installer/mapr-setup.sh -P /tmp'
fi

gcloud compute ssh ubuntu@mapr1 'sudo bash /tmp/mapr-setup.sh'