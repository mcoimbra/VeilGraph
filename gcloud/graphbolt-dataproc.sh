#! /usr/bin/bash

## Update the system packages:
apt-get -y update

## Install Python 3 and necessary packages.
apt-get install python-dev
apt-get install python-pip
apt-get install python3
apt-get install python3-dev
apt-get install python3-pip

## pip packages
pip3 install networkx
pip3 install pathlib
pip3 install psutil
pip3 install pytz
pip3 install matplotlib
pip3 install numpy

## Make sure Java 8 and Maven are available:
apt install openjdk-8-jdk
apt install maven


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

GS_BUCKET_CODE_DIR="$GS_BUCKET/GraphBolt"
gsutil cp -r gs://$GS_BUCKET_CODE_DIR/python $GRAPHBOLT_CODE_DIR/
gsutil cp -r gs://$GS_BUCKET_CODE_DIR/src $GRAPHBOLT_CODE_DIR/
gsutil cp gs://$GS_BUCKET_CODE_DIR/pom.xml $GRAPHBOLT_CODE_DIR/
gsutil cp gs://$GS_BUCKET_CODE_DIR/README.md $GRAPHBOLT_CODE_DIR/
gsutil cp gs://$GS_BUCKET_CODE_DIR/LICENSE-2.0.txt $GRAPHBOLT_CODE_DIR/

## Make the files readable and writable by everyone.
chmod -R 777 $GRAPHBOLT_ROOT

#cd $GRAPHBOLT_CODE_DIR
#/usr/bin/mvn clean install



