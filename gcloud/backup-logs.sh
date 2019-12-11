#!/bin/bash

#set -x

PARALLELISM=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

DATE_TIME_STAMP=$(printf -v date '%(%Y-%m-%d %H:%M:%S)T\n' -1)
DATE_TIME_STAMP=$(echo $date | tr ' ' _ )

GCLOUD_DIR_NAME="logs_$DATE_TIME_STAMP"

GCLOUD_PATH='gs://graphbolt-storage/testing/flink_logs/$GCLOUD_DIR_NAME'

echo "> Copying log files to $GCLOUD_PATH"

for (( i = 0; i < PARALLELISM; ++i )); do
    echo $i
    #ssh "graphbolt@graphbolt-cluster-w-$i" 'gsutil cp -p /usr/lib/flink/log/* $GCLOUD_PATH'
done