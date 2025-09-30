#!/bin/bash

DOCKER_IMAGE=$1

zhdun() {
    local cmd="$1"
    local expected="$2"
    local msg="$3"
    local retries="$4"
    local delay="$5"
    local count=1

    while true; do
        result=$($cmd 2>/dev/null | tr -d '\r\n')
        if [ "$result" = "$expected" ] || [ "$result" = "degraded" ]; then
            echo "Finished waiting: $cmd → $result"
            return 0
        fi
        echo "$msg Status: $result, attempt: $count"
        if [ $count -ge $retries ]; then
            echo "Tired of waiting: $count attempts"
            return 1
        fi
        count=$((count+1))
        sleep $delay
    done
}

check_result="running"
check_command="docker exec $DOCKER_IMAGE systemctl is-system-running"
waiting_message="Waiting for docker container to start systemd."

zhdun "$check_command" "$check_result" "$waiting_message" 60 2
