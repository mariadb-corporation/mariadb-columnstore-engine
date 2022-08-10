
if [ -z $(cat /proc/cpuinfo | grep -E 'sse4_2|neon|asimd' | head -c1) ]; then
    echo "No support found for sse4 or neon simd instructions; aborting"
    exit 1
fi

if [ "$1" == "2" ]; then
    /bin/cp -f /etc/columnstore/storagemanager.cnf /etc/columnstore/storagemanager.cnf.rpmsave > /dev/null 2>&1
    columnstore-pre-uninstall
fi
exit 0
