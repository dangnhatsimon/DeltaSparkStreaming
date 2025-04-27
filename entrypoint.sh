#!/bin/bash

SPARK_WORKLOAD=""
SPARK_MASTER_HOST="localhost"
SPARK_MASTER_PORT="7077"
SPARK_MASTER_WEBUI_PORT="8080"
SPARK_WORKER_CORES="2"
SPARK_WORKER_MEMORY="2g"
SPARK_WORKER_PORT="7078"
SPARK_WORKER_WEBUI_PORT="8081"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --workload) SPARK_WORKLOAD="$2"; shift ;;
        --master-host) SPARK_MASTER_HOST="$2"; shift ;;
        --master-port) SPARK_MASTER_PORT="$2"; shift ;;
        --master-webui-port) SPARK_MASTER_WEBUI_PORT="$2"; shift ;;
        --worker-cores) SPARK_WORKER_CORES="$2"; shift ;;
        --worker-memory) SPARK_WORKER_MEMORY="$2"; shift ;;
        --worker-port) SPARK_WORKER_PORT="$2"; shift ;;
        --worker-webui-port) SPARK_WORKER_WEBUI_PORT="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  start-master.sh -h $SPARK_MASTER_HOST -p $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT
elif [[ $SPARK_WORKLOAD =~ "worker" ]];
# if $SPARK_WORKLOAD contains substring "worker". try
# try "worker-1", "worker-2" etc.
then
  start-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT --webui-port $SPARK_WORKER_WEBUI_PORT -c $SPARK_WORKER_CORES -m $SPARK_WORKER_MEMORY
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
else
  echo "Unknown workload: $SPARK_WORKLOAD"
  exit 1
fi
