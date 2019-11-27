#!/bin/bash

process_scenario () {
	DATASET_PREFIX=$1
	R=$2
	N=$3
	DELTA=$4
	RBO_LEN=$5
	DAMPENING=$6

	declare -a COLUMNS_PATH_ARRAY=()

	for d in 1 2 4 8 16; do

		printf "> Parallelism: %d\n" $d

		COMPLETE_DIR=$(echo "$ZIP_TARGET_OUT"/$DATASET_PREFIX\_$RBO_LEN\_P$d\_$DAMPENING\_complete_D)
		FILE_NAME=$(ls "$COMPLETE_DIR" | grep start.tsv)
		COMPLETE_PATH="$COMPLETE_DIR/$FILE_NAME"
		printf "> Complete path:\t$COMPLETE_PATH\n"

		cat "$COMPLETE_PATH" | cut -d ";" -f1,4,5,6 | tr ';' '\t' > "$COMPLETE_DIR"/columns.tsv

		SUMMARIZED_DIR=$(echo "$ZIP_TARGET_OUT"/$DATASET_PREFIX\_$RBO_LEN\_P$d\_$DAMPENING\_model_$R\_$N\_$DELTA\_D)
		FILE_NAME=$(ls "$SUMMARIZED_DIR" | grep start.tsv)
		SUMMARIZED_PATH="$SUMMARIZED_DIR/$FILE_NAME"
		printf "> Summarized path:\t$SUMMARIZED_PATH\n"

		cat "$SUMMARIZED_PATH" | cut -d ";" -f1,4,5,6 | tr ';' '\t' > "$SUMMARIZED_DIR"/columns.tsv

		# Create file for gnuplot to compare complete and summarizex execution times.
		# One _columns file is created for each value of parallelism $d.
		COLUMNS_OUT_FILE=$DATASET_PREFIX\_$RBO_LEN\_P"$d"\_$DAMPENING\_model_$R\_$N\_$DELTA\_D_columns.tsv
		echo $COLUMNS_OUT_FILE
		paste -d , "$COMPLETE_DIR"/columns.tsv "$SUMMARIZED_DIR"/columns.tsv | tr ',' '\t' > $COLUMNS_OUT_FILE
		
		# Store the current columns file path in array.
		COLUMNS_PATH_ARRAY+=($COLUMNS_OUT_FILE)
	
	done

	# Parse the files in the array to generate single big file.
	paste -d , $(printf "%s " "${COLUMNS_PATH_ARRAY[@]}")
}

# Establish run order.
main() {
	#set -x

	TARGET_STATS_DIR=$1
	
	CURR_DIR=$(pwd)
	cd "$TARGET_STATS_DIR"

	# If the 'Statistics' directory does not exist, create it extract all .zip files to it.
	if [ ! -d "Statistics" ]
	then
		# List directory with .zip files.
		for f in *.zip; do
			unzip -o $f #-o to overwrite
		done
	fi

	# Process the statistics files.
	ZIP_TARGET_OUT=$TARGET_STATS_DIR/Statistics/pagerank

	DATASET_PREFIX="$2"
	R=$3
	N=$4
	DELTA=$5
	RBO_LEN=$6
	DAMPENING=$7

	process_scenario $DATASET_PREFIX $R $N $DELTA $RBO_LEN $DAMPENING

    cd "$CURR_DIR"
}

main "$@"