#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../main/server/
    go build ../main/client/
    #go build ../cmd/
    rm -r logs
    mkdir logs/
    ./server -log_dir=logs -log_level=info -id 1.1 -algorithm=paxosRB >logs/out1.1.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=info -id 1.2 -algorithm=paxosRB >logs/out1.2.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=info -id 1.3 -algorithm=paxosRB >logs/out1.3.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=info -id 1.4 -algorithm=paxosRB >logs/out1.4.txt 2>&1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=info -id 1.5 -algorithm=paxosRB >logs/out1.5.txt 2>&1 &
    echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
