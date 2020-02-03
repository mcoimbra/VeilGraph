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
  
  rbo_length=5000
  pagerank_iterations=30
  damp="0.85"

  gcloud dataproc clusters create graphbolt-cluster --region us-east1 --image graphbolt-debian --zone us-east1-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i '$dataset_name' -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir '$dataset_dir' -cache "gs://graphbolt-storage/cache" -p '$flink_parallelism' -size '$rbo_length' -iterations '$pagerank_iterations' -damp '$damp' -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081'
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'source /home/graphbolt/.bash_profile && ssh-add /home/graphbolt/.ssh/cluster && cd /home/graphbolt/Documents/Projects/GraphBolt.git/gcloud && ./backup-logs.sh '$dataset_name'_'$pagerank_iterations'_'$rbo_length'_P'$flink_parallelism'_'$damp'_complete'

  echo "Y" | gcloud dataproc clusters delete graphbolt-cluster
}

function run_summarized() {
  local flink_parallelism=$1
  local cluster_parallelism=$2
  local dataset_dir=$3
  local dataset_name=$4
  local r_param=$5
  local n_param=$6
  local delta_param=$7
  
  
  rbo_length=5000
  pagerank_iterations=30
  damp="0.85"

  gcloud dataproc clusters create graphbolt-cluster --region us-east1 --image graphbolt-debian --zone us-east1-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i '$dataset_name' -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir '$dataset_dir' -cache "gs://graphbolt-storage/cache" -p '$flink_parallelism' -size '$rbo_length' -iterations '$pagerank_iterations' -damp '$damp' -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address graphbolt-cluster-m -flink-port 8081 -summarized-only -l '$r_param' '$n_param' '$delta_param''
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "us-east1-b" "graphbolt@graphbolt-cluster-m" --command 'source /home/graphbolt/.bash_profile && ssh-add /home/graphbolt/.ssh/cluster && cd /home/graphbolt/Documents/Projects/GraphBolt.git/gcloud && ./backup-logs.sh '$dataset_name'_'$pagerank_iterations'_'$rbo_length'_P'$flink_parallelism'_'$damp'_model_'$r_param'_'$n_param'_'$delta_param''
  


  echo "Y" | gcloud dataproc clusters delete graphbolt-cluster

}

# Establish run order.
main() {
	set -x

  # ./test-commands "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random" 0.05 2 0.50

  DATA_DIR=$1
  DATASET_PREFIX=$2
  R=$3
	N=$4
	DELTA=$5

  
  run_complete 1 2 $DATA_DIR $DATASET_PREFIX

  run_summarized 1 2 $DATA_DIR $DATASET_PREFIX $R $N $DELTA

  for d in 2 4 8 16; do
    run_complete $d $d $DATA_DIR $DATASET_PREFIX

    run_summarized $d $d $DATA_DIR $DATASET_PREFIX $R $N $DELTA
  done
}

main "$@"
