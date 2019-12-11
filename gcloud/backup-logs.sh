#!/bin/bash

PARALLELISM=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

printf -v date '%(%Y-%m-%d %H:%M:%S)T\n' -1
DATE_TIME_STAMP=$(echo $date | tr ' ' _ )

GCLOUD_DIR_NAME="logs_$DATE_TIME_STAMP"
GCLOUD_PATH="gs://graphbolt-storage/testing/flink_logs/$GCLOUD_DIR_NAME"

printf "> Timestamp:\t$DATE_TIME_STAMP\n"

printf "> Copying log files to:\t$GCLOUD_PATH\n"

for (( i = 0; i < PARALLELISM; ++i )); do
    ssh "graphbolt@graphbolt-cluster-w-$i" "gsutil cp -p /usr/lib/flink/log/* $GCLOUD_PATH"
done