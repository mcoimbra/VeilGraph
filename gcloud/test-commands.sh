#!/bin/bash

#######################################
####################################### Automation functions.
#######################################

function run_complete() {
  local flink_parallelism=$1
  local cluster_parallelism=$2
  local dataset_dir=$3
  local dataset_name=$4
  local rbo_length=$5
  local region=$6
  pagerank_iterations=30
  damp="0.85"

  #local cluster_name=graphbolt-cluster-P$cluster_parallelism-$dataset_name\_$rbo_length\_$pagerank_iterations\_$damp
  local cluster_name=complete-p$cluster_parallelism-f$flink_parallelism-$dataset_name

  echo "> Cluster name: $cluster_name"

  #return

  #gcloud dataproc clusters create $cluster_name --region $region --image graphbolt-debian --zone $region-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

  gcloud dataproc clusters create $cluster_name --region $region --image graphbolt-debian --zone $region-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-26368
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "$region-b" "graphbolt@$cluster_name-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i '$dataset_name' -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir '$dataset_dir' -cache "gs://graphbolt-storage/cache" -p '$flink_parallelism' -size '$rbo_length' -iterations '$pagerank_iterations' -damp '$damp' -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address '$cluster_name-m' -flink-port 8081'
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "$region-b" "graphbolt@$cluster_name-m" --command 'source /home/graphbolt/.bash_profile && ssh-add /home/graphbolt/.ssh/cluster && cd /home/graphbolt/Documents/Projects/GraphBolt.git/gcloud && ./backup-logs.sh '$cluster_name' '$dataset_name'_'$pagerank_iterations'_'$rbo_length'_P'$flink_parallelism'_'$damp'_complete'

  echo "Y" | gcloud dataproc clusters delete $cluster_name --region=$region
}

function run_summarized() {
  local flink_parallelism=$1
  local cluster_parallelism=$2
  local dataset_dir=$3
  local dataset_name=$4
  local r_param=$5
  local n_param=$6
  local delta_param=$7
  local rbo_length=$8
  local region=$9
  
  
  
  pagerank_iterations=30
  damp="0.85"

  #local cluster_name=graphbolt-cluster-P$cluster_parallelism-$dataset_name\_$rbo_length\_$pagerank_iterations\_$damp\_$r_param\_$n_param\_$delta_param

  local cluster_name=summarized-p$cluster_parallelism-f$flink_parallelism-$dataset_name

  echo "> Cluster name: $cluster_name"

  #return

  #gcloud dataproc clusters create $cluster_name --region $region --image graphbolt-debian --zone $region-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-16384

  gcloud dataproc clusters create $cluster_name --region $region --image graphbolt-debian --zone $region-b --initialization-actions gs://graphbolt-storage/dataproc-initialization-actions/flink/flink.sh --num-workers $cluster_parallelism --master-machine-type custom-4-16384 --worker-machine-type custom-4-26368
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "$region-b" "graphbolt@$cluster_name-m" --command 'cd /home/graphbolt/Documents/Projects/GraphBolt.git/python && python3 -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i '$dataset_name' -chunks 50 -out-dir "/home/graphbolt/Documents/Projects/GraphBolt.git/testing" -data-dir '$dataset_dir' -cache "gs://graphbolt-storage/cache" -p '$flink_parallelism' -size '$rbo_length' -iterations '$pagerank_iterations' -damp '$damp' -periodic-full-dump -temp "/home/graphbolt/Documents/Projects/GraphBolt.git/testing/Temp" -flink-address '$cluster_name'-m -flink-port 8081 -summarized-only -l '$r_param' '$n_param' '$delta_param''
  
  gcloud beta compute --project "datastorm-1083" ssh --zone "$region-b" "graphbolt@$cluster_name-m" --command 'source /home/graphbolt/.bash_profile && ssh-add /home/graphbolt/.ssh/cluster && cd /home/graphbolt/Documents/Projects/GraphBolt.git/gcloud && ./backup-logs.sh '$dataset_name'_'$pagerank_iterations'_'$rbo_length'_P'$flink_parallelism'_'$damp'_model_'$r_param'_'$n_param'_'$delta_param''
  


  echo "Y" | gcloud dataproc clusters delete $cluster_name --region=$region

}

# Establish run order.
main() {
	set -x

  # ./test-commands "/home/graphbolt/Documents/datasets/social" "amazon-2008-40000-random" 0.05 2 0.50 5000

  DATA_DIR=$1
  DATASET_PREFIX=$2
  R=$3
	N=$4
	DELTA=$5
  RBO_LEN=$6
  REGION=$7

  
  #run_complete 1 2 $DATA_DIR $DATASET_PREFIX $RBO_LEN $REGION &

  #run_summarized 1 2 $DATA_DIR $DATASET_PREFIX $R $N $DELTA $RBO_LEN $REGION &

  #for d in 2 4 8 16; do
  #for d in 4 8 16; do
  #for d in 8 16; do
  #  run_complete $d $d $DATA_DIR $DATASET_PREFIX $RBO_LEN $REGION &

  #  run_summarized $d $d $DATA_DIR $DATASET_PREFIX $R $N $DELTA $RBO_LEN $REGION &
  #done

  #run_complete 16 16 /home/graphbolt/Documents/datasets/web eu-2015-host-40000-random 500000 us-east1 &
  #run_summarized 16 16 /home/graphbolt/Documents/datasets/web eu-2015-host-40000-random 0.05 2 0.50 500000 us-east1 &

  #run_summarized 4 4 /home/graphbolt/Documents/datasets/web eu-2015-host-40000-random 0.05 2 0.50 500000 us-east4 &
  #run_complete 8 8 /home/graphbolt/Documents/datasets/web eu-2015-host-40000-random 500000 us-east4 &
  #run_summarized 8 8 /home/graphbolt/Documents/datasets/web eu-2015-host-40000-random 0.05 2 0.50 500000 us-west2 &

  #run_summarized 1 2 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 0.05 2 0.50 500000 us-west1 &
  #run_complete 1 2 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 500000 us-west1 &

  #run_summarized 2 2 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 0.05 2 0.50 500000 us-west1 &
  #run_complete 2 2 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 500000 us-west1 &
  
  run_summarized 4 4 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 0.05 2 0.50 500000 us-east4 &
  #run_complete 4 4 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 500000 us-east4 &
  
  #run_summarized 8 8 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 0.05 2 0.50 500000 us-east1 &
  #run_complete 8 8 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 500000 us-east1 &

  #run_summarized 16 16 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 0.05 2 0.50 500000 us-central1 &

  #run_complete 16 16 /home/graphbolt/Documents/datasets/social hollywood-2011-40000-random 500000 us-east1 &

  wait
  echo "> All tasks finished."
}

main "$@"
