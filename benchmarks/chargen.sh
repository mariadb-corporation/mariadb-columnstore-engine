#!/usr/bin/env bash

for (( VAR=1; VAR<=1000; VAR++ ))
do
    shuf -er -n3  {A..Z} {a..z} | tr -d '\n'
    echo
done
