#!/bin/bash

PARALLELISM=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

# Generate a timestamp for the current logs.
printf -v date '%(%Y-%m-%d %H:%M:%S)T\n' -1
DATE_TIME_STAMP=$(echo $date | tr ' ' _ )

# Prepare the Google Cloud Storage object names.
GCLOUD_DIR_NAME="logs_$DATE_TIME_STAMP"
GCLOUD_PATH="gs://graphbolt-storage/testing/flink_logs/$GCLOUD_DIR_NAME"

# Helpful information.
printf "> Timestamp:\t$DATE_TIME_STAMP\n"
printf "> Copying log files to:\t$GCLOUD_PATH\n"

# Copy master node Flink logs.
gsutil cp -p /usr/lib/flink/log/* $GCLOUD_PATH

# Order each worker to copy its Flink logs.
for (( i = 0; i < PARALLELISM; ++i )); do
    ssh "graphbolt@graphbolt-cluster-w-$i" "gsutil cp -p /usr/lib/flink/log/* $GCLOUD_PATH"
done