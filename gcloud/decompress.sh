#!/bin/bash

append_to_column_names () {

	SOURCE_FILE_PATH=$1
	PREFIX=$2
	PARALLELISM=$3

	declare -a FIXED_NAMES_ARRAY=()

	# Iterate each line of the input given to the while-loop.
	# Because the input comes from <(head -n 1 ...), the while loops exactly once.
	while IFS=$'\t' read -r -a COL_NAME_ARRAY
	do
		# Prepend $PREFIX to each column name.
		for COL_NAME in "${COL_NAME_ARRAY[@]}"
		do
			FIXED_NAMES_ARRAY+=(P$PARALLELISM\_$PREFIX\_$COL_NAME)
		done
	done < <(head -n 1 "$SOURCE_FILE_PATH")

	# Output of this function is the tab-separated now-prefixed column names.
	FINAL_STRING=$(printf "%s\t" "${FIXED_NAMES_ARRAY[@]}")

	# Remove the whitespace at the end and produce function output.
	echo ${FINAL_STRING::-1} | sed -e 's/ /'\\t'/g'

	unset FIXED_NAMES_ARRAY
}



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

		######### Complete data file.

		COMPLETE_DIR=$(echo "$ZIP_TARGET_OUT"/$DATASET_PREFIX\_$RBO_LEN\_P$d\_$DAMPENING\_complete_D)
		FILE_NAME=$(ls "$COMPLETE_DIR" | grep start.tsv)
		COMPLETE_PATH="$COMPLETE_DIR/$FILE_NAME"

		#exit 0

		printf "\tComplete path:\t$COMPLETE_PATH\n"

		# Delete the columns file if it already existed.
		rm -f "$COMPLETE_DIR"/columns.tsv

		# Store in a temporary file.
		cat "$COMPLETE_PATH" | cut -d ";" -f1,4,5,6 | tr ';' '\t' > "$COMPLETE_DIR"/columns.tsv.tmp

		# Set the headers.
		COMPLETE_HEADERS=$(append_to_column_names "$COMPLETE_DIR"/columns.tsv.tmp "complete" $d)
		echo "$COMPLETE_HEADERS" > "$COMPLETE_DIR"/columns.tsv
		tail -n +2 "$COMPLETE_DIR"/columns.tsv.tmp >> "$COMPLETE_DIR"/columns.tsv

		# Remove temporary data file.
		rm -f "$COMPLETE_DIR"/columns.tsv.tmp

		######### Summarized data file.

		SUMMARIZED_DIR=$(echo "$ZIP_TARGET_OUT"/$DATASET_PREFIX\_$RBO_LEN\_P$d\_$DAMPENING\_model_$R\_$N\_$DELTA\_D)
		FILE_NAME=$(ls "$SUMMARIZED_DIR" | grep start.tsv)
		SUMMARIZED_PATH="$SUMMARIZED_DIR/$FILE_NAME"
		printf "\tSummarized path:\t$SUMMARIZED_PATH\n"

		# Delete the columns file if it already existed.
		rm -f "$SUMMARIZED_DIR"/columns.tsv

		# Store in a temporary file.
		cat "$SUMMARIZED_PATH" | cut -d ";" -f1,4,5,6 | tr ';' '\t' > "$SUMMARIZED_DIR"/columns.tsv.tmp

		# Set the headers.
		SUMMARIZED_HEADERS=$(append_to_column_names "$SUMMARIZED_DIR"/columns.tsv.tmp "summarized" $d)
		echo "$SUMMARIZED_HEADERS" > "$SUMMARIZED_DIR"/columns.tsv
		tail -n +2 "$SUMMARIZED_DIR"/columns.tsv.tmp >> "$SUMMARIZED_DIR"/columns.tsv

		# Remove temporary data file.
		rm -f "$SUMMARIZED_DIR"/columns.tsv.tmp

		######### Merge the complete and summarized statistics in the same file.

		# Create file for gnuplot to compare complete and summarizex execution times.
		# One _columns file is created for each value of parallelism $d.
		COLUMNS_OUT_FILE=$DATASET_PREFIX\_$RBO_LEN\_P"$d"\_$DAMPENING\_model_$R\_$N\_$DELTA\_D_columns.tsv
		paste -d , "$COMPLETE_DIR"/columns.tsv "$SUMMARIZED_DIR"/columns.tsv | tr ',' '\t' > $COLUMNS_OUT_FILE
		
		# Store the current columns file path in array.
		COLUMNS_PATH_ARRAY+=($COLUMNS_OUT_FILE)
	
	done

	#echo "${COLUMNS_PATH_ARRAY[@]}"
	paste -d , $(echo "${COLUMNS_PATH_ARRAY[@]}") | tr ',' '\t' > "$DATASET_PREFIX"_data.tsv

	unset COLUMNS_PATH_ARRAY

	PY_SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
	
	python3 $GCLOUD_DIR/get_avg_std.py "$DATASET_PREFIX"_data.tsv
}

# Establish run order.
main() {
	set -x

	TARGET_STATS_DIR=$1
	
	CURR_DIR=$(pwd)

	GCLOUD_DIR="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

	cd "$TARGET_STATS_DIR"

	DATASET_PREFIX="$2"

	# If the 'Statistics' directory does not exist, create it extract all .zip files to it.
	#if [ ! -d "Statistics" ]
	#then
	# List directory with .zip files.
	for f in $DATASET_PREFIX*.zip; do
		unzip -o $f #-o to overwrite
	done
	#fi

	# Process the statistics files.
	TARGET_STATS_DIR=$(pwd)
	ZIP_TARGET_OUT=$TARGET_STATS_DIR/Statistics/pagerank

	
	R=$3
	N=$4
	DELTA=$5
	RBO_LEN=$6
	DAMPENING=$7

	process_scenario $DATASET_PREFIX $R $N $DELTA $RBO_LEN $DAMPENING

    cd "$CURR_DIR"
}

main "$@"