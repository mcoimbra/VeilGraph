#!/bin/bash

set -x

######################################
####################################### Automation functions.
#######################################

function run_complete() {
  local flink_parallelism=$1
  local cluster_parallelism=$2
  local dataset_dir=$3
  local dataset_name=$4
  
  gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i '$dataset_name' -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir '$dataset_dir' -cache "gs://graphbolt-storage/cache" -p '$flink_parallelism' -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081'
  
  
  gcloud dataproc clusters delete --region=us-east1 graphbolt-cluster
}

function run_summarized() {
  local flink_parallelism=$1
  local cluster_parallelism=$2
  local dataset_dir=$3
  local dataset_name=$4
  local r_param=$5
  local n_param=$6
  local delta_param=$7
  
  
  gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i '$dataset_name' -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir '$dataset_dir' -cache "gs://graphbolt-storage/cache" -p '$flink_parallelism' -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -summarized-only -l '$r_param' '$n_param' '$delta_param''
  
  gcloud dataproc clusters delete --region=us-east1 graphbolt-cluster

}


#######################################
####################################### eu-2005-40000-random
#######################################

### P = 1

run_complete 1 2 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random"

run_summarized 1 2 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random" 0.05 2 0.50

### P = 2

run_complete 2 2 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random"

run_summarized 2 2 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random" 0.05 2 0.50

### P = 4

run_complete 4 4 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random"

run_summarized 4 4 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random" 0.05 2 0.50

### P = 8

run_complete 8 8 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random"

run_summarized 8 8 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random" 0.05 2 0.50

### P = 16

run_complete 16 16 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random"

run_summarized 16 16 "/home/graphbolt/Documents/datasets/web" "eu-2005-40000-random" 0.05 2 0.50

#######################################
####################################### amazon-2008-40000-random
#######################################

### P = 1

run_complete 1 2 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random"

run_summarized 1 2 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random" 0.05 2 0.50

### P = 2

run_complete 2 2 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random"

run_summarized 2 2 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random" 0.05 2 0.50

### P = 4

run_complete 4 4 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random"

run_summarized 4 4 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random" 0.05 2 0.50

### P = 8

run_complete 8 8 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random"

run_summarized 8 8 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random" 0.05 2 0.50

### P = 16

run_complete 16 16 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random"

run_summarized 16 16 "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random" 0.05 2 0.50