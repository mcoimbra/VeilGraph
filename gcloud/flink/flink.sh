#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script installs Apache Flink (http://flink.apache.org) on a Google Cloud
# Dataproc cluster. This script is based on previous scripts:
# https://github.com/GoogleCloudPlatform/bdutil/tree/master/extensions/flink
#
# To use this script, you will need to configure the following variables to
# match your cluster. For information about which software components
# (and their version) are included in Cloud Dataproc clusters, see the
# Cloud Dataproc Image Version information:
# https://cloud.google.com/dataproc/concepts/dataproc-versions

set -euxo pipefail

# Install directories for Flink and Hadoop.
readonly FLINK_INSTALL_DIR='/usr/lib/flink'
readonly FLINK_WORKING_DIR='/var/lib/flink'
readonly FLINK_YARN_SCRIPT='/usr/bin/flink-yarn-daemon'
readonly FLINK_WORKING_USER='yarn'
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly FLINK_LOG_DIR='/usr/lib/flink/log'
readonly FLINK_SLAVES_FILE="$FLINK_INSTALL_DIR/conf/slaves"
readonly FLINK_MASTERS_FILE="$FLINK_INSTALL_DIR/conf/masters"

# The number of buffers for the network stack.
# Flink config entry: taskmanager.network.numberOfBuffers.
readonly FLINK_NETWORK_NUM_BUFFERS=2048

# Heap memory used by the job manager (master) determined by the physical (free) memory of the server.
# Flink config entry: jobmanager.heap.mb.
readonly FLINK_JOBMANAGER_MEMORY_FRACTION='1.0'

# Heap memory used by the task managers (slaves) determined by the physical (free) memory of the servers.
# Flink config entry: taskmanager.heap.mb.
readonly FLINK_TASKMANAGER_MEMORY_FRACTION='1.0'

readonly START_FLINK_YARN_SESSION_METADATA_KEY='flink-start-yarn-session'

# Set this to true to start a flink yarn session at initialization time.
readonly START_FLINK_YARN_SESSION_DEFAULT=false

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function retry_apt_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function update_apt_get() {
  retry_apt_command "apt-get update"
}

function install_apt_get() {
  local pkgs="$*"
  retry_apt_command "apt-get install -y $pkgs"
}

function install_flink_snapshot() {
  local work_dir
  work_dir="$(mktemp -d)"
  local flink_url
  
  flink_url='https://archive.apache.org/dist/flink/flink-1.6.2/flink-1.6.2-bin-hadoop28-scala_2.11.tgz'
  local flink_local="${work_dir}/flink.tgz"
  local flink_toplevel_pattern="${work_dir}/flink-*"

  pushd "${work_dir}"

  curl -o "${flink_local}" "${flink_url}"
  tar -xzvf "${flink_local}"
  rm "${flink_local}"

  # Only the first match of the flink toplevel pattern is used.
  local flink_toplevel
  flink_toplevel=$(compgen -G "${flink_toplevel_pattern}" | head -n1)
  mv "${flink_toplevel}" "${FLINK_INSTALL_DIR}"

  popd # work_dir
  
  sudo mkdir -p "${FLINK_LOG_DIR}"
  chmod -R 0777 "${FLINK_LOG_DIR}"
}

function configure_flink() {
  # Number of worker nodes in your cluster.
  local num_workers
  num_workers=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

  # Number of Flink TaskManagers to use. Reserve 1 node for the JobManager.
  # NOTE: This assumes > 1 worker nodes.
  local num_taskmanagers="$num_workers"

  # Determine the number of task slots per worker.
  # NOTE: Dataproc does not currently set the number of worker cores on the
  # master node. However, the spark configuration sets the number of executors
  # to be half the number of CPU cores per worker. We use this value to
  # determine the number of worker cores. Fix this hack when
  # yarn.nodemanager.resource.cpu-vcores is correctly populated.
  local spark_executor_cores
  spark_executor_cores=$(
    grep 'spark\.executor\.cores' /etc/spark/conf/spark-defaults.conf |
      tail -n1 |
      cut -d'=' -f2
  )
  #local flink_taskmanager_slots="$((spark_executor_cores * 2))"
  local flink_taskmanager_slots="$((spark_executor_cores))"

  # Determine the default parallelism.
  local flink_parallelism
  flink_parallelism=$(python2 -c \
    "print ${num_taskmanagers} * ${flink_taskmanager_slots}")

  # Get worker memory from yarn config.
  local worker_total_mem
  worker_total_mem="$(hdfs getconf \
    -confKey yarn.nodemanager.resource.memory-mb)"
  local flink_jobmanager_memory
  flink_jobmanager_memory=$(python2 -c \
    "print int(${worker_total_mem} * ${FLINK_JOBMANAGER_MEMORY_FRACTION})")

  local flink_taskmanager_memory
  flink_taskmanager_memory=$(python2 -c \
    "print int(${worker_total_mem} * ${FLINK_TASKMANAGER_MEMORY_FRACTION})")

  # Fetch the primary master name from metadata.
  local master_hostname
  master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

  # Create working directory.
  mkdir -p "${FLINK_WORKING_DIR}"
  
  # Testing directory creation was successful.
  ls ${FLINK_INSTALL_DIR}
  sudo ls ${FLINK_INSTALL_DIR}

  # Apply Flink settings by appending them to the default config.
  #taskmanager.numberOfTaskSlots: ${flink_taskmanager_slots}
  cat <<EOF >>${FLINK_INSTALL_DIR}/conf/flink-conf.yaml
# Settings applied by Cloud Dataproc initialization action
jobmanager.rpc.address: ${master_hostname}
jobmanager.rpc.port: 6123
rest.address: ${master_hostname}
rest.port: 8081
rest.idleness-timeout: 6000000
jobmanager.heap.size: 4096m
taskmanager.heap.size: ${flink_taskmanager_memory}m
taskmanager.numberOfTaskSlots: 1
parallelism.default: ${flink_parallelism}
taskmanager.network.numberOfBuffers: ${FLINK_NETWORK_NUM_BUFFERS}
fs.hdfs.hadoopconf: ${HADOOP_CONF_DIR}
env.log.dir: ${FLINK_LOG_DIR}
env.hadoop.conf.dir: /etc/hadoop/conf
env.ssh.opts: -oStrictHostKeyChecking=no

query.server.ports: 30000-35000
query.proxy.ports: 35001-40000
taskmanager.rpc.port: 45001-50000
taskmanager.data.port: 50001
blob.server.port: 55001-60000
blob.client.socket.timeout: 6000000

akka.transport.heartbeat.pause: 6000s
akka.tcp.timeout: 60000s
akka.ask.timeout: 60000s
web.timeout: 600000
EOF


  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  if [[ ! "${role}" == 'Master' ]]; then
    cat <<EOF >>${FLINK_INSTALL_DIR}/conf/flink-conf.yaml
taskmanager.host: $HOSTNAME
EOF
  fi

  # See 'here-documents' to know what this is doing:
  # https://wiki.bash-hackers.org/syntax/redirection#here_documents
  cat >"${FLINK_YARN_SCRIPT}" <<EOF
#!/bin/bash
set -exuo pipefail
sudo -u yarn -i \
HADOOP_CONF_DIR=${HADOOP_CONF_DIR} \
  ${FLINK_INSTALL_DIR}/bin/yarn-session.sh \
  -n "${num_taskmanagers}" \
  -s "${flink_taskmanager_slots}" \
  -jm "${flink_jobmanager_memory}" \
  -tm "${flink_taskmanager_memory}" \
  -nm flink-dataproc \
  --detached
EOF
  chmod +x "${FLINK_YARN_SCRIPT}"

  # Only leave log4j log configuration files.
  rm ${FLINK_INSTALL_DIR}/conf/logback*

  # Set the Flink slaves file
  local cluster_name
  cluster_name=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

  echo "Writing to slaves file."
  echo "$FLINK_SLAVES_FILE"
  truncate -s 0 $FLINK_SLAVES_FILE

  for (( i = 0; i < num_workers; ++i )); do
    echo "Adding: $cluster_name-w-$i"
    echo "$cluster_name-w-$i" >> $FLINK_SLAVES_FILE
  done


  # Set the Flink masters file
  echo "Writing to masters file."
  echo "$FLINK_MASTERS_FILE"
  truncate -s 0 $FLINK_MASTERS_FILE
  echo "$cluster_name-m:8081" >> $FLINK_MASTERS_FILE
  
  # Get Google Cloud Storage libraries.
  wget -O ${FLINK_INSTALL_DIR}/lib/gcs-connector-latest-hadoop2.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar
}

function start_flink_master() {
  local master_hostname
  master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

  echo "master_hostname: ${master_hostname}"
  local start_yarn_session
  start_yarn_session="$(/usr/share/google/get_metadata_value \
    "attributes/${START_FLINK_YARN_SESSION_METADATA_KEY}" ||
    echo "${START_FLINK_YARN_SESSION_DEFAULT}")"

  # Start Flink master only on the master node ("0"-master in HA mode)
  if [[ "${start_yarn_session}" == "true" && "${HOSTNAME}" == "${master_hostname}" ]]; then
    "${FLINK_YARN_SCRIPT}"
  else
    echo "Doing nothing"
  fi
}

function start_flink_standalone() {
  local master_hostname
  master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
  
  sudo -i -u veilgraph bash << EOF
cd ${FLINK_INSTALL_DIR}/bin
./start-cluster.sh
EOF

}

function main() {

  # Allow port usage for the TaskManagers.
  sudo iptables -A INPUT -p tcp -m tcp --dport 30000:60000 -j ACCEPT

  sudo pip3 install google
  sudo pip3 install google-auth
  sudo pip3 install google-cloud-core
  sudo pip3 install google-cloud-storage

  # Configure Flink (TaskManager/JobManager memory, akka timeouts, etc.)
  configure_flink || err "Flink configuration failed"
  
  # Prepare VeilGraph code.
  readonly VEILGRAPH_USER="veilgraph"
  readonly VEILGRAPH_ROOT="/home/$VEILGRAPH_USER"
  readonly VEILGRAPH_CODE_DIR=$VEILGRAPH_ROOT/Documents/Projects/VeilGraph.git
  
  # Fetch from GitHub and compile it.
  sudo -i -u veilgraph bash << EOF
ssh-keyscan -t rsa github.com | tee /tmp/github-key-temp | ssh-keygen -lf -
cat /tmp/github-key-temp >> \$HOME/.ssh/known_hosts
rm /tmp/github-key-temp
cd $VEILGRAPH_CODE_DIR
git pull github master
mvn clean install
EOF

  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  local cluster_name
  cluster_name="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"

  if [[ "${role}" == 'Master' ]]; then

  
    # Add all the cluster workers' host fingerprint to /home/veilgraph/.ssh/known_hosts.
	local num_workers
	num_workers=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
    for (( i = 0; i < num_workers; ++i )); do
      echo "[master] Adding: $cluster_name-w-$i to /home/veilgraph/.ssh/known_hosts"
	  sudo -i -u veilgraph bash << EOF
ssh-keyscan -t rsa $cluster_name-w-$i | tee /tmp/$cluster_name-w-$i-key-temp | ssh-keygen -lf -
cat /tmp/$cluster_name-w-$i-key-temp >> \$HOME/.ssh/known_hosts
rm /tmp/$cluster_name-w-$i-key-temp
EOF
    done
    
    # Start the Flink standalone cluster.
    start_flink_standalone  || err "Unable to start Flink master in standalone mode"
  else
    # Add the cluster master host finperprint to /home/veilgraph/.ssh/known_hosts.
	sudo -i -u veilgraph bash << EOF
ssh-keyscan -t rsa $cluster_name-m | tee /tmp/$cluster_name-m-key-temp | ssh-keygen -lf -
cat /tmp/$cluster_name-m-key-temp >> \$HOME/.ssh/known_hosts
rm /tmp/$cluster_name-m-key-temp
EOF
  fi

}

main
