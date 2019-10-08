#! /usr/bin/bash

# Update the system packages:
apt-get -y update

# Install Java 8 and Maven:
apt-get -y install openjdk-8-jdk
apt-get -y install maven

# Install git
apt-get -y install git


################################################
################################################ Setup GraphBolt user and directories.
################################################


GRAPHBOLT_USER="graphbolt"

# Create Debian user only allowing .ssh public key access.
# See: https://askubuntu.com/questions/94060/run-adduser-non-interactively
adduser --disabled-password --gecos "First Last,RoomNumber,WorkPhone,HomePhone" $GRAPHBOLT_USER

# Add to sudo group.
usermod -aG sudo $GRAPHBOLT_USER

# Generate SSH private/public keys.
# Info: https://www.digitalocean.com/community/tutorials/how-to-set-up-ssh-keys-on-ubuntu-1604
# ssh-keygen is run as user $GRAPHBOLT_USER
sudo -i -u graphbolt bash << EOF
  ssh-keygen -t rsa -f ~/.ssh/cluster -N "" -C "Flink Dataproc Access"
  touch ~/.ssh/authorized_keys
  cat ~/.ssh/cluster > ~/.ssh/authorized_keys
  chmod -R go= ~/.ssh
EOF

# Create the GraphBolt dataset directories.
GRAPHBOLT_ROOT="/home/$GRAPHBOLT_USER"
mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/web/
mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/social/

GS_BUCKET="graphbolt-bucket"
GS_BUCKET_DATASETS_DIR="$GS_BUCKET/datasets"

mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/web/eu-2005-40000-random
gsutil cp -r gs://$GS_BUCKET_DATASETS_DIR/web/eu-2005-40000-random/* $GRAPHBOLT_ROOT/Documents/datasets/web/eu-2005-40000-random/

mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/social/amazon-2008-40000-random
gsutil cp -r gs://$GS_BUCKET_DATASETS_DIR/social/amazon-2008-40000-random/* $GRAPHBOLT_ROOT/Documents/datasets/social/amazon-2008-40000-random/

# Create and copy the GraphBolt code directories.
GRAPHBOLT_CODE_DIR=$GRAPHBOLT_ROOT/Documents/Projects/GraphBolt
mkdir -p $GRAPHBOLT_CODE_DIR
mkdir -p $GRAPHBOLT_CODE_DIR/testing/Temp
mkdir -p $GRAPHBOLT_CODE_DIR/cache

GS_BUCKET_CODE_DIR="$GS_BUCKET/github"
GS_GRAPHBOLT_ZIP_NAME="GraphBolt.git.zip"
gsutil cp -r gs://$GS_BUCKET_CODE_DIR/$GS_GRAPHBOLT_ZIP_NAME $GRAPHBOLT_CODE_DIR/
cd $GRAPHBOLT_CODE_DIR
unzip $GS_GRAPHBOLT_ZIP_NAME


# Make the files readable and writable by everyone.
chmod -R 777 $GRAPHBOLT_ROOT


################################################
################################################ Download and compile Python 3.6.9
################################################

# Based on https://www.rosehosting.com/blog/how-to-install-python-3-6-4-on-debian-9/
# Python 3.6 (at least) is necessary to support f-expressions.

# Install dependencies required to compile Python 3.6.9.
apt-get install -y make build-essential libssl-dev zlib1g-dev
apt-get install -y libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm
apt-get install -y libncurses5-dev  libncursesw5-dev xz-utils tk-dev

# We need libssl-dev so pip3.6 can install things.
# https://www.linuxquestions.org/questions/linux-software-2/where-can-i-download-openssl-devel-for-debian-727415/
apt-get install -y libssl-dev


# Download Python 3.6.9.
PYTHON_BIN_DIR="$GRAPHBOLT_ROOT/bin"
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

################################################
################################################ Configure Flink 1.6.2.
################################################

readonly FLINK_INSTALL_DIR='/usr/lib/flink'
readonly FLINK_LOG_DIR='/usr/lib/flink/log'

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

# only the first match of the flink toplevel pattern is used
local flink_toplevel
flink_toplevel=$(compgen -G "${flink_toplevel_pattern}" | head -n1)
mv "${flink_toplevel}" "${FLINK_INSTALL_DIR}"

popd # work_dir

sudo mkdir -p "${FLINK_LOG_DIR}"
sudo chmod -R 0777 "${FLINK_LOG_DIR}"