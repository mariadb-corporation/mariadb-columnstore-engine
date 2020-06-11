#!/bin/bash

/bin/kill -15 "$1"
timeout=$(mcsGetConfig SystemConfig DBRMTimeout)

while [ -n "$(pgrep -x controllernode)" ] && [ $timeout -gt 0 ]
do
    sleep 1
    ((--timeout))
done

if [ -n "$(pgrep -x controllernode)" ]; then
    /bin/kill -9 "$1"
fi
