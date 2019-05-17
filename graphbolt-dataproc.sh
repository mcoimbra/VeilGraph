#!/usr/bin/env bash

## Create the GraphBolt dataset directories.
mkdir -p /home/GraphBolt/datasets/web/eu-2005-40000-random
mkdir -p /home/GraphBolt/datasets/social/amazon-2008-40000-random

## Copy the datasets from Google Cloud Storage.
gsutil cp gs://staging.winged-precept-150902.appspot.com/eu-2005-40000-random/* /home/GraphBolt/datasets/web/eu-2005-40000-random/
gsutil cp gs://staging.winged-precept-150902.appspot.com/amazon-2008-40000-random/* /home/GraphBolt/datasets/social/amazon-2008-40000-random/

## Create and copy the GraphBolt code directories.
mkdir -p /home/graphbolt/code
gsutil cp gs://staging.winged-precept-150902.appspot.com/GraphBolt/* /home/GraphBolt/code/

## Make sure Python 3 and desired packages are available:

## Make sure Java 8 and Maven are available:
