SEC_TO_WAIT=15
echo -n "Waiting CMAPI to finish startup"
success=false
for i in  $(seq 1 $SEC_TO_WAIT); do
    echo -n "..$i"
    if ! $(curl -k -s --output /dev/null --fail https://127.0.0.1:8640/cmapi/ready); then
        sleep 1
    else
        success=true
        break
    fi
done

echo
if $success; then
    echo "CMAPI ready to handle requests."
else
    echo "CMAPI not ready after waiting $SEC_TO_WAIT seconds. Check log file for further details."
fi
