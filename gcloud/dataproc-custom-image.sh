#! /usr/bin/bash

## Update the system packages:
apt-get -y update



## Make sure Java 8 and Maven are available:
apt-get -y install openjdk-8-jdk
apt-get -y install maven

## Install git
apt-get -y install git

## Create the GraphBolt dataset directories.
GRAPHBOLT_ROOT="/home/GraphBolt"
mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/web/
mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/social/

GS_BUCKET="graphbolt-bucket"
GS_BUCKET_DATASETS_DIR="$GS_BUCKET/datasets"

mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/web/eu-2005-40000-random
gsutil cp -r gs://$GS_BUCKET_DATASETS_DIR/web/eu-2005-40000-random/* /home/GraphBolt/Documents/datasets/web/eu-2005-40000-random/

mkdir -p $GRAPHBOLT_ROOT/Documents/datasets/social/amazon-2008-40000-random
gsutil cp -r gs://$GS_BUCKET_DATASETS_DIR/social/amazon-2008-40000-random/* /home/GraphBolt/Documents/datasets/social/amazon-2008-40000-random/

## Create and copy the GraphBolt code directories.
GRAPHBOLT_CODE_DIR=$GRAPHBOLT_ROOT/Documents/Projects/GraphBolt
mkdir -p $GRAPHBOLT_CODE_DIR
mkdir -p $GRAPHBOLT_CODE_DIR/testing/Temp

GS_BUCKET_CODE_DIR="$GS_BUCKET/GraphBolt"
GS_GRAPHBOLT_ZIP_NAME="GraphBolt.git.zip"
gsutil cp -r gs://$GS_BUCKET_CODE_DIR/$GS_GRAPHBOLT_ZIP_NAME $GRAPHBOLT_CODE_DIR/
cd $GRAPHBOLT_CODE_DIR
unzip $GS_GRAPHBOLT_ZIP_NAME
#gsutil cp -r gs://$GS_BUCKET_CODE_DIR/python $GRAPHBOLT_CODE_DIR/
#gsutil cp -r gs://$GS_BUCKET_CODE_DIR/src $GRAPHBOLT_CODE_DIR/
#gsutil cp gs://$GS_BUCKET_CODE_DIR/pom.xml $GRAPHBOLT_CODE_DIR/
#gsutil cp gs://$GS_BUCKET_CODE_DIR/README.md $GRAPHBOLT_CODE_DIR/
#gsutil cp gs://$GS_BUCKET_CODE_DIR/LICENSE-2.0.txt $GRAPHBOLT_CODE_DIR/

## Make the files readable and writable by everyone.
chmod -R 777 $GRAPHBOLT_ROOT

#cd $GRAPHBOLT_CODE_DIR
#/usr/bin/mvn clean install

## Install Python 3 and necessary packages.
#apt-get -y install python-dev
#apt-get -y install python-pip
#apt-get -y install python3
#apt-get -y install python3-dev
#apt-get -y install python3-pip

apt-get install -y make build-essential libssl-dev zlib1g-dev
apt-get install -y libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm
apt-get install -y libncurses5-dev  libncursesw5-dev xz-utils tk-dev

# We need libssl-dev so pip3.6 can install things.
# https://www.linuxquestions.org/questions/linux-software-2/where-can-i-download-openssl-devel-for-debian-727415/
apt-get install -y libssl-dev

########
######## Install Python 3.6
########

# https://www.rosehosting.com/blog/how-to-install-python-3-6-4-on-debian-9/

## Necessary to support f-expressions introduced in Python 3.6

# Download and install Python 3.6.9.
PYTHON_BIN_DIR="$GRAPHBOLT_ROOT/bin"
mkdir -p "$PYTHON_BIN_DIR"
cd $PYTHON_BIN_DIR
wget https://www.python.org/ftp/python/3.6.9/Python-3.6.9.tgz
tar -zxvf Python-3.6.9.tgz
cd Python-3.6.9

# Configure and install.
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

