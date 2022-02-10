#! /usr/bin/bash

# Update the system packages:
apt-get -y update

# Install Java 8 and Maven:
apt-get -y install openjdk-8-jdk
apt-get -y install maven

# Install git
apt-get -y install git

# Install htop
apt-get -y install htop

################################################
################################################ Setup VeilGraph user and directories.
################################################

readonly VEILGRAPH_USER="veilgraph"
readonly VEILGRAPH_ROOT="/home/$VEILGRAPH_USER"

# Create Debian user only allowing .ssh public key access.
# See: https://askubuntu.com/questions/94060/run-adduser-non-interactively
adduser --disabled-password --gecos "First Last,RoomNumber,WorkPhone,HomePhone" $VEILGRAPH_USER

# Add to sudo group.
usermod -aG sudo $VEILGRAPH_USER

# Generate SSH private/public keys.
# Info: https://www.digitalocean.com/community/tutorials/how-to-set-up-ssh-keys-on-ubuntu-1604
# ssh-keygen is run as user $VEILGRAPH_USER

# readonly GS_BUCKET="veilgraph-bucket"
readonly GS_BUCKET="veilgraph-storage"
readonly GS_BUCKET_CODE_DIR="$GS_BUCKET/github"

readonly CLUSTER_SSH_PKEY="cluster"
readonly GITHUB_SSH_PKEY="github"
sudo -i -u veilgraph bash << EOF
  ssh-keygen -t rsa -f ~/.ssh/$CLUSTER_SSH_PKEY -N "" -C "Flink Dataproc Access"
  touch ~/.ssh/authorized_keys
  cat ~/.ssh/$CLUSTER_SSH_PKEY.pub > ~/.ssh/authorized_keys
  chmod -R go= ~/.ssh
  chmod 600 ~/.ssh/$CLUSTER_SSH_PKEY
  
  ssh-keygen -t rsa -f ~/.ssh/$GITHUB_SSH_PKEY -N "" -C "VeilGraph GitHub Access"
  gsutil cp ~/.ssh/$GITHUB_SSH_PKEY.pub gs://$GS_BUCKET_CODE_DIR/$GITHUB_SSH_PKEY.pub
  chmod 600 ~/.ssh/$GITHUB_SSH_PKEY
EOF

# Create the VeilGraph dataset directories.

mkdir -p $VEILGRAPH_ROOT/Documents/datasets/web/
mkdir -p $VEILGRAPH_ROOT/Documents/datasets/social/

# Personal

readonly GS_BUCKET_DATASETS_DIR="$GS_BUCKET/datasets"

mkdir -p $VEILGRAPH_ROOT/Documents/datasets/web/eu-2005-40000-random
gsutil cp -r gs://$GS_BUCKET_DATASETS_DIR/web/eu-2005-40000-random/* $VEILGRAPH_ROOT/Documents/datasets/web/eu-2005-40000-random/

mkdir -p $VEILGRAPH_ROOT/Documents/datasets/social/amazon-2008-40000-random
gsutil cp -r gs://$GS_BUCKET_DATASETS_DIR/social/amazon-2008-40000-random/* $VEILGRAPH_ROOT/Documents/datasets/social/amazon-2008-40000-random/

# Create and copy the VeilGraph code directories.
readonly VEILGRAPH_CODE_DIR=$VEILGRAPH_ROOT/Documents/Projects/VeilGraph.git
mkdir -p $VEILGRAPH_CODE_DIR
mkdir -p $VEILGRAPH_CODE_DIR/testing/Temp
mkdir -p $VEILGRAPH_CODE_DIR/cache


readonly GS_VEILGRAPH_ZIP_NAME="VeilGraph.git.zip"
gsutil cp -r gs://$GS_BUCKET_CODE_DIR/$GS_VEILGRAPH_ZIP_NAME $VEILGRAPH_CODE_DIR/
cd $VEILGRAPH_CODE_DIR
unzip $GS_VEILGRAPH_ZIP_NAME

readonly GS_KEY_NAME="datastorm-1083-f24ebf51869d.json"

# Prepare .bash_profile and misc utilities.
sudo touch ${VEILGRAPH_ROOT}/.bash_profile
sudo cat <<EOF >>${VEILGRAPH_ROOT}/.bash_profile
if [ -n "\$BASH_VERSION" ]; then
    if [ -f \$HOME/.bashrc ]; then
        . \$HOME/.bashrc
    fi
fi

SSH_ENV="\$HOME/.ssh/environment"

function start_agent {
    echo "Initialising new SSH agent..."
    /usr/bin/ssh-agent | sed 's/^echo/#echo/' > "\$SSH_ENV"
    echo succeeded
    chmod 600 "\${SSH_ENV}"
    . "\${SSH_ENV}" > /dev/null
    /usr/bin/ssh-add;
}

# Source SSH settings, if applicable

if [ -f "\${SSH_ENV}" ]; then
    . "\${SSH_ENV}" > /dev/null
    #ps \${SSH_AGENT_PID} doesn't work under cywgin
    ps -ef | grep \${SSH_AGENT_PID} | grep ssh-agent$ > /dev/null || {
        start_agent;
    }
else
    start_agent;
fi

ssh-add \$HOME/.ssh/$CLUSTER_SSH_PKEY
ssh-add \$HOME/.ssh/$GITHUB_SSH_PKEY

export HADOOP_CONF_DIR=/etc/hadoop/conf
export GOOGLE_APPLICATION_CREDENTIALS=\$HOME/.gcloud/$GS_KEY_NAME

EOF

# Copy misc UNIX program configurations (.bashrc, .vim/, .viminfo, .screenrc).
readonly GS_UNIX_DIR="$GS_BUCKET/home_utils"
gsutil cp -r gs://$GS_UNIX_DIR/* $VEILGRAPH_ROOT/

# Copy the appropriate service key.

readonly GS_DS_KEY="$GS_BUCKET/iam/$GS_KEY_NAME"
sudo mkdir $VEILGRAPH_ROOT/.gcloud
gsutil cp gs://$GS_DS_KEY $VEILGRAPH_ROOT/.gcloud/
sudo chown -R veilgraph:veilgraph $VEILGRAPH_ROOT/.gcloud
sudo chmod -R 640 $VEILGRAPH_ROOT/.gcloud/*

# Set appropriate .ssh permissions
sudo chown -R veilgraph:veilgraph $VEILGRAPH_ROOT
sudo chmod -R 640 $VEILGRAPH_ROOT/Documents/datasets/web/eu-2005-40000-random/*
sudo chmod -R 640 $VEILGRAPH_ROOT/Documents/datasets/social/amazon-2008-40000-random/*
readonly VEILGRAPH_SSH_DIR=$VEILGRAPH_ROOT/.ssh
sudo chmod 700 $VEILGRAPH_SSH_DIR
sudo chmod 644 $VEILGRAPH_SSH_DIR/*.pub
sudo chmod 600 $VEILGRAPH_SSH_DIR/$CLUSTER_SSH_PKEY
sudo chmod 600 $VEILGRAPH_SSH_DIR/$GITHUB_SSH_PKEY
sudo chmod 600 $VEILGRAPH_SSH_DIR/authorized_keys

################################################
################################################ Download and compile Python 3.6.9
################################################

# Based on https://www.rosehosting.com/blog/how-to-install-python-3-6-4-on-debian-9/
# Python 3.6 (at least) is necessary to support f-expressions.

# Install dependencies required to compile Python 3.6.9.
apt-get install -y make build-essential libssl-dev zlib1g-dev
apt-get install -y libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm
apt-get install -y libncurses5-dev libncursesw5-dev xz-utils tk-dev

# We need libssl-dev so pip3.6 can install things.
# https://www.linuxquestions.org/questions/linux-software-2/where-can-i-download-openssl-devel-for-debian-727415/
apt-get install -y libssl-dev


# Download Python 3.6.9.
readonly PYTHON_BIN_DIR="$VEILGRAPH_ROOT/Downloads"
mkdir -p "$PYTHON_BIN_DIR"
cd $PYTHON_BIN_DIR
wget https://www.python.org/ftp/python/3.6.9/Python-3.6.9.tgz
tar -zxvf Python-3.6.9.tgz
cd Python-3.6.9

# Configure and install Python 3.6.9.
./configure --enable-optimizations
make -j8
sudo make altinstall

# Set aliases for *3.6 binaries that were just installed.
ln -s /usr/local/bin/pip3.6 /usr/local/bin/pip3
ln -s /usr/local/bin/python3.6 /usr/local/bin/python3

# Install the pip packages.
pip3 install networkx
pip3 install pathlib
pip3 install psutil
pip3 install pytz
pip3 install matplotlib
pip3 install numpy
pip3 install datetime
pip3 install google
pip3 install google-auth
pip3 install google-cloud-core
pip3 install google-cloud-storage


sudo chown -R veilgraph:veilgraph $PYTHON_BIN_DIR

################################################
################################################ Configure Flink 1.6.2.
################################################

readonly FLINK_INSTALL_DIR='/usr/lib/flink'
readonly FLINK_LOG_DIR='/usr/lib/flink/log'

readonly WORK_DIR="$(mktemp -d)"
readonly FLINK_URL='https://archive.apache.org/dist/flink/flink-1.6.2/flink-1.6.2-bin-hadoop28-scala_2.11.tgz'
readonly FLINK_LOCAL="${WORK_DIR}/flink.tgz"
readonly FLINK_TOPLEVEL_PATTERN="${WORK_DIR}/flink-*"

pushd "${WORK_DIR}"

curl -o "${FLINK_LOCAL}" "${FLINK_URL}"
tar -xzvf "${FLINK_LOCAL}"
rm "${FLINK_LOCAL}"

# Only the first match of the flink toplevel pattern is used.
readonly FLINK_TOPLEVEL=$(compgen -G "${FLINK_TOPLEVEL_PATTERN}" | head -n1)
mv "${FLINK_TOPLEVEL}" "${FLINK_INSTALL_DIR}"

popd # work_dir

sudo mkdir -p "${FLINK_LOG_DIR}"

# Set permissions and ownership for Flink files.
sudo chown -R veilgraph ${FLINK_INSTALL_DIR}

sudo chmod -x ${FLINK_INSTALL_DIR}/LICENSE
sudo chmod -x ${FLINK_INSTALL_DIR}/NOTICE
sudo chmod -x ${FLINK_INSTALL_DIR}/README.txt

sudo chmod 755 ${FLINK_INSTALL_DIR}/log

sudo chmod 755 ${FLINK_INSTALL_DIR}/opt
sudo chmod -x ${FLINK_INSTALL_DIR}/opt/*.jar

sudo chmod 755 ${FLINK_INSTALL_DIR}/lib

# We need to fetch the gcs connector library and put it in Flink's lib directory.
# https://stackoverflow.com/questions/51860988/flink-checkpoints-to-google-cloud-storage
wget -O ${FLINK_INSTALL_DIR}/lib/gcs-connector-latest-hadoop2.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar
sudo chmod -x ${FLINK_INSTALL_DIR}/lib/*.jar


sudo chmod 755 ${FLINK_INSTALL_DIR}/examples

sudo chmod 755 ${FLINK_INSTALL_DIR}/examples/batch
sudo chmod -x ${FLINK_INSTALL_DIR}/examples/batch/*.jar

sudo chmod 755 ${FLINK_INSTALL_DIR}/examples/gelly
sudo chmod -x ${FLINK_INSTALL_DIR}/examples/gelly/*.jar

sudo chmod 755 ${FLINK_INSTALL_DIR}/examples/python
sudo chmod 755 ${FLINK_INSTALL_DIR}/examples/python/batch
sudo chmod -x ${FLINK_INSTALL_DIR}/examples/python/batch/*.py

sudo chmod 755 ${FLINK_INSTALL_DIR}/examples/python/streaming
sudo chmod -x ${FLINK_INSTALL_DIR}/examples/python/streaming/*.py

sudo chmod 755 ${FLINK_INSTALL_DIR}/examples/streaming
sudo chmod -x ${FLINK_INSTALL_DIR}/examples/streaming/*.jar

sudo chmod 755 ${FLINK_INSTALL_DIR}/bin
sudo chmod -x ${FLINK_INSTALL_DIR}/bin/*.bat

sudo chmod 755 ${FLINK_INSTALL_DIR}/conf
sudo chmod -x ${FLINK_INSTALL_DIR}/conf/*

