echo "Start mariadb-columnstore-engine/build/preInstall_storage_engine.sh"
set -x

if [ -z $(cat /proc/cpuinfo | grep -E 'sse4_2|neon|asimd' | head -c1) ]; then
    echo "error: this machine CPU lacks of support for Intel SSE4.2 or ARM Advanced SIMD instructions.
Columnstore requires one of this instruction sets, installation aborted"
    exit 1
fi

if [ "$1" == "2" ]; then
    /bin/cp -f /etc/columnstore/storagemanager.cnf /etc/columnstore/storagemanager.cnf.rpmsave > /dev/null 2>&1
    bash -x columnstore-pre-uninstall 1> /tmp/columnstore-pre-uninstall.log 2>&1
    cp /tmp/columnstore-pre-uninstall.log /var/log/mariadb/columnstore/.
fi
exit 0
