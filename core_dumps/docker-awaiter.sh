#!/bin/bash

DOCKER_IMAGE=$1

zhdun()
{
    command=$1
    expected_result=$2
    waiting_message=$3
    retries=$4
    sleep_delay=$5
    result=$($command)
    result="${result%%[[:cntrl:]]}"
    retries_counter=1

    while true;
    do
        if [ "$result" != "$expected_result" ]; then
            echo $waiting_message " Status: " $result ", attempt: " $retries_counter
            sleep $sleep_delay
        else
            echo Finished waiting for \'"$command"\' to return \'"$expected_result"\'
            exit
        fi

        if [ $retries_counter -ge $retries ]; then
            echo "Tired to wait for retry, $retries_counter attemps were made"
            exit
        fi
        retries_counter=$(($retries_counter + 1))
    done
}

check_result="running"
check_command="docker exec -t $DOCKER_IMAGE systemctl is-system-running"
waiting_message="Waiting for docker container to start systemd."

zhdun "$check_command" "$check_result" "$waiting_message" 60 2
