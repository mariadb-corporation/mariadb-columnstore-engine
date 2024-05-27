#!/bin/bash

mkdir -p /tmp/
chmod 777 /tmp/
echo 2 > /proc/sys/fs/suid_dumpable
echo /tmp/core-%e-%s-%u-%g-%p-%t > /proc/sys/kernel/core_pattern

echo "kernel.core_pattern = /tmp/core-%e-sig%s-user%u-group%g-pid%p-time%t" >> /etc/sysctl.d/core.conf
echo "kernel.core_uses_pid = 1" >> /etc/sysctl.d/core.conf
echo "fs.suid_dumpable = 2" >> /etc/sysctl.d/core.conf

echo "DefaultLimitCORE=infinity" >> /etc/systemd/system.conf

echo "*       hard        core        unlimited" >> /etc/security/limits.d/core.conf
echo "*       soft        core        unlimited" >> /etc/security/limits.d/core.conf
echo "*       soft     nofile           65536" >> /etc/security/limits.d/core.conf
echo "*       hard     nofile           65536" >> /etc/security/limits.d/core.conf


echo "fs.file-max = 65536" >>  /etc/sysctl.conf

systemctl daemon-reexec
sysctl -p
