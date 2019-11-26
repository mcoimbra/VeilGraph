#!/bin/bash

#set -x

CURR_DIR=$(pwd)
cd "$1"

# If the 'Statistics' directory does not exist, create it extract all .zip files to it.
if [ ! -d "Statistics" ]
then
	# List directory with .zip files.
	for f in *.zip; do
		unzip -o $f #-o to overwrite
	done
fi

# Process the statistics files.
ZIP_TARGET_OUT=$1/Statistics/pagerank

for d in 1 2 4 8 16; do
	COMPLETE_DIR=$(echo $ZIP_TARGET_OUT/*_P$d\_*_complete_D)
	FILE_NAME=$(ls "$COMPLETE_DIR" | grep start.tsv)
	COMPLETE_PATH="$COMPLETE_DIR/$FILE_NAME"
	printf "> Complete path:\t$COMPLETE_PATH\n"
	
	cat $COMPLETE_PATH | cut -d ";" -f1,4,5,6 | tr ';' '\t' > "$COMPLETE_DIR"/columns.tsv
	
	SUMMARIZED_DIR=$(echo $ZIP_TARGET_OUT/*_P$d\_*_model_*_D)
	FILE_NAME=$(ls "$SUMMARIZED_DIR" | grep start.tsv)
	SUMMARIZED_PATH="$SUMMARIZED_DIR/$FILE_NAME"
	printf "> Summarized path:\t$SUMMARIZED_PATH\n"
	
	cat $SUMMARIZED_PATH | cut -d ";" -f1,4,5,6 | tr ';' '\t' > "$SUMMARIZED_DIR"/columns.tsv
	
	paste -d , "$COMPLETE_DIR"/columns.tsv "$SUMMARIZED_DIR"/columns.tsv | tr ',' '\t'
	
done

cd "$CURR_DIR"