#!/bin/bash
grubby --update-kernel=ALL --args="elevator=deadline";
yum install -y redhat-lsb  glibc gdb glibc.i686 cyrus-sasl cyrus-sasl-lib cyrus-sasl-md5;
if [ -f /lib64/libsasl2.so.3 ]; then
  ln -s /lib64/libsasl2.so.3 /lib64/libsasl2.so.2
fi;
systemctl disable firewalld;
chown -R daemon:daemon /opt/ML_Data/;
mkdir -p /opt/ML_Data/;
echo "vm.nr_hugepages=6000" > /etc/sysctl.d/ML.conf;
sysctl -p /etc/sysctl.d/ML.conf;