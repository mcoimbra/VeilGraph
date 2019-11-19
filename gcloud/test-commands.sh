#!/bin/bash

######################################
####################################### Automation functions.
#######################################

function run_complete() {
  local flink_parallelism=$1
  local cluster_parallelism=$2
  local dataset_dir=$6
  local dataset_name=$7
  
  gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i $dataset_name -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "$dataset_dir" -cache "gs://graphbolt-storage/cache" -p $flink_parallelism -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081'
  
  
  gcloud dataproc clusters delete --region=us-east1 graphbolt-cluster
}

function run_summarized() {
  local flink_parallelism=$1
  local cluster_parallelism=$2
  local r_param=$3
  local n_param=$4
  local delta_param=$5
  local dataset_dir=$6
  local dataset_name=$7
  
  gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i $dataset_name -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "$dataset_dir" -cache "gs://graphbolt-storage/cache" -p $flink_parallelism -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -summarized-only -l $r_param $n_param $delta_param'
  
  gcloud dataproc clusters delete --region=us-east1 graphbolt-cluster

}


#######################################
####################################### eu-2005-40000-random
#######################################

### P = 1

## Complete
gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 2 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python &&
python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/web" -cache "gs://graphbolt-storage/cache" -p 1 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081'

gcloud dataproc clusters delete --region=us-east1 graphbolt-cluster

## Summarized

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 2 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command "cd /home/graphbolt/Documents/Projects/GraphBolt.git/python &&
python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/web" -cache "gs://graphbolt-storage/cache" -p 1 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -summarized-only -l 0.05 2 0.50"

gcloud dataproc clusters delete --region=us-east1 graphbolt-cluster

exit 0

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/web" -cache "gs://graphbolt-storage/cache" -p 1 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

exit 0

### P = 2

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 2 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/web" -cache "gs://graphbolt-storage/cache" -p 2 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

### P = 4

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 4 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/web" -cache "gs://graphbolt-storage/cache" -p 4 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

### P = 8

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 8 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/web" -cache "gs://graphbolt-storage/cache" -p 8 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

### P = 16

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 16 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/web" -cache "gs://graphbolt-storage/cache" -p 16 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

#######################################
####################################### amazon-2008-40000-random
#######################################

### P = 1

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 2 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i amazon-2008-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/social" -cache "gs://graphbolt-storage/cache" -p 1 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

### P = 2

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 2 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i amazon-2008-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/social" -cache "gs://graphbolt-storage/cache" -p 2 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

### P = 4

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 4 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i amazon-2008-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/social" -cache "gs://graphbolt-storage/cache" -p 4 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

### P = 8

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 8 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i amazon-2008-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/social" -cache "gs://graphbolt-storage/cache" -p 8 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50

### P = 16

gcloud dataproc clusters create graphbolt-cluster --image graphbolt-debian --zone us-east1-b --initialization-actions 
	gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers 16 --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i amazon-2008-40000-random -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir "/home/graphbolt/Documents/datasets/social" -cache "gs://graphbolt-storage/cache" -p 16 -size 5000 -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -l 0.05 2 0.50