#!/usr/bin/env python3
__copyright__ = """ Copyright 2018 Miguel E. Coimbra

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. """
__license__ = "Apache 2.0"

# Uniformly sample edges from a .tsv file.
# Number of samples may be given as an absolute value or percentage of
# edges of the provided file.
# The user may choose to randomize the resulting stream file.

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
import argparse
import io
from typing import List
import os
import pathlib
import random
import sys

# 2. related third party imports
# 3. custom local imports
from graphbolt import localutil

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################

# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
DESCRIPTION_TEXT = "GraphBolt edge sampler. Take a graph, sample some edges and produce a new graph file and a stream file."
parser = argparse.ArgumentParser(description=DESCRIPTION_TEXT, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-i', '--input-file', help="dataset name.", required=True, type=str)
parser.add_argument('-o', '--out-dir', help="output directory.", required=False, type=str)
parser.add_argument('-p', '--sample-probability', help='set desired sampling probability to generate the stream.', required=False, type=float, nargs='?', const=-1.0)
parser.add_argument('-c', '--sample-count', help='number of samples to include in the generated stream.', required=False, type=int, nargs='?', const=-1)
parser.add_argument('-r', '--randomize', help='randomize the sampled edges.', required=False, action="store_true")
parser.add_argument('-d', '--debug', help='debug mode outputs helpful information.', required=False, action="store_true")
parser.add_argument("-q", "--query-count", help="stream query count.", required=True, type=int)
parser.add_argument("-deletion-ratio", help="number of edges to be deleted, as a fraction of stream chunk size.", required=False, type=float, default=0.20)



args = parser.parse_args()

# Sanitize arguments.
if (not args.out_dir is None):
    if len(args.out_dir) == 0:
        print("> -out-dir must be a non-empty string. Exiting")
        sys.exit(1)
    if not (os.path.exists(args.out_dir) and os.path.isdir(args.out_dir)):
        print("> Provided output directory does not exist: {}. Exiting".format(args.out_dir))
        sys.exit(1)

# This condition checks if either are both true or both false.
if args.sample_probability == args.sample_count:
    print("> Must supply exactly one of '-sample-count' or '-sample-probability'. Exiting.")
    sys.exit(1)
if args.sample_count != None and args.sample_count <= 0:
    print("> '-sample-count' value must be a positive integer. Exiting.")
    sys.exit(1)
if args.sample_probability != None and (args.sample_probability <= 0 or args.sample_probability > 1):
    print("> '-sample_probability' value must be a positive float in ]0; 1]. Exiting.")
    sys.exit(1)
if args.query_count < 0:
    print("> '-query-count' must be positive. Exiting.")
    sys.exit(1)
if args.deletion_ratio < 0.0:
    print("> '-deletion-ratio' must be positive. Exiting.")
    sys.exit(1)

###########################################################################
############################### APPLICATION ###############################
###########################################################################


# Count the number of valid edge lines and note indexes of invalid input lines.
if args.input_file.startswith('~'):
    args.input_file = os.path.expanduser(args.input_file).replace('\\', '/')

input_line_count, bad_indexes = localutil.file_stats(args.input_file)



bad_index_count = len(bad_indexes)



# Calculate the sampling probability and stream size.
if args.sample_count != None:
    stream_size = args.sample_count
    p = stream_size / (input_line_count - bad_index_count)
else:
    p = args.sample_probability
    stream_size = int((input_line_count - bad_index_count) * p)
    
out_file_name = args.input_file[:args.input_file.rfind(".")]
if os.path.sep in out_file_name:
    out_file_name = out_file_name[args.input_file.rfind(os.path.sep) + 1:]
else:
    out_file_name = out_file_name[args.input_file.rfind("/") + 1:]

# Output directory will be the same as the input file's directory if not provided.
if args.out_dir is None:
    out_dir = os.path.dirname(args.input_file)
    out_file_name = out_file_name.replace('-original', '')
    out_file_name = out_file_name + "-" + str(stream_size)

    if args.randomize:
        if args.debug:
            print("> Randomizing stream file.")

        # Explicitly state in the file names that the stream edges were randomized.
        out_file_name = out_file_name + "-random"

    # Get parent dir of provided input file.
    input_file_parent_dir = os.path.abspath(os.path.join(out_dir, os.pardir))

    # Create the output directory based on the target output file name.
    out_dir = os.path.join(input_file_parent_dir, out_file_name)

    # Create the output directory if it does not exist.
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)

else:
    out_dir = args.out_dir

out_graph_path = os.path.join(out_dir, "{}-start.tsv".format(out_file_name))
out_stream_path = os.path.join(out_dir, "{}-stream.tsv".format(out_file_name))
out_deletions_path = os.path.join(out_dir, "{}-deletions.tsv".format(out_file_name))

if args.debug:
    print("> Output directory:\t{}".format(out_dir))
    print("> Out file name base:\t{}".format(out_file_name))
    print("> Base graph file:\t{}".format(out_graph_path))
    print("> Edge stream file:\t{}".format(out_stream_path))
    print("> Edge deletions file:\t{}".format(out_deletions_path))
    print("> Probability:\t{}".format(p))
    print("> input_line_count:\t{}".format(input_line_count))
    print("> bad_indexes:\t{}".format(bad_index_count))

# Sample and write resulting base graph and edge stream files.
with open(args.input_file, 'r') as dataset, open(out_graph_path, 'w') as out_graph_file, open(out_stream_path, 'w') as out_stream_file:
    #https://stackoverflow.com/questions/19286657/index-all-except-one-item-in-python
    base_lines = []
    stream_indexes = []
    sample_count = 0
    valid_line_count = input_line_count# - bad_index_count

    if args.debug:
        print("> valid_line_count: " + str(valid_line_count))
        print("> Probability:\t{}".format(p))

    valid_ctr = 0

    for i, l in enumerate(dataset):

        ### If the line is not empty and is not a comment (begins with '#')
        if not i in bad_indexes:

            p = (stream_size - sample_count) / (valid_line_count - valid_ctr)

            valid_ctr = valid_ctr + 1

            if sample_count != stream_size and random.random() < p:
                sample_count = sample_count + 1
                stream_indexes.append(l.strip())
            else:
                base_lines.append(l.strip())
                
                if len(base_lines) == io.DEFAULT_BUFFER_SIZE:
                    #out_graph_file.write('\n'.join(base_lines) + "\n")
                    out_graph_file.write('\n'.join(base_lines) + "\n")
                    base_lines = []

            if valid_ctr == valid_line_count:
                break
        
    

    # Is stream order randomization required?
    if args.randomize:
        random.shuffle(stream_indexes)

    out_stream_file.write('\n'.join(stream_indexes) + "\n")
    out_stream_file.flush()


    #out_graph_file.flush()

    if len(base_lines) > 0:
        out_graph_file.write('\n'.join(base_lines) + "\n")

    # Get the chunk properties for the generated stream.
    chunk_size, chunk_sizes, _, edge_count, _ = localutil.prepare_stream(out_stream_path, args.query_count)

    deletion_size = int(args.deletion_ratio * chunk_size)

    #prev_chunk = []
    #curr_index = 0
    #already_deleted = []
    deletions = []
    block_acc = 0
    base_graph_index_limit = valid_ctr + bad_index_count
    for i in range(len(chunk_sizes)):

        # Draw a sample from the original graph plus all the previous chunk blocks we've already iterated over.
        del_block = random.sample(range(bad_index_count, base_graph_index_limit + block_acc), deletion_size)
        
        # Keep sampling until there isn't a single repetition.
        checking_repetitions = True
        while checking_repetitions:
            repetitions = 0
            for j in del_block:
                if j in deletions:
                    repetitions = repetitions + 1
                    break
            if repetitions == 0:
                break
            else:
                del_block = random.sample(range(bad_index_count, base_graph_index_limit + block_acc), deletion_size)
        
        deletions = deletions + del_block

        # 'bloack_acc' sets the upper limit for the sampling procedure.
        block_acc = block_acc + chunk_sizes[i]

    # 'deletions' contains indexes associated with the sampled edges to be deleted.
    # Need to retrieve the associated edge strings.

    deletion_strings = len(deletions) * ['']#[ind for ind in deletions]
    base_deletion_indexes = []
    aux = {}
    for i in range(len(deletions)):
        # String is part of the base graph file.
        if deletions[i] < base_graph_index_limit:
            base_deletion_indexes.append(deletions[i])
            aux[deletions[i]] = i
        # String is part of a specific block.
        else:
            # Find the index inside the specific block and retrieve the string.
            print("deletion_strings[{}] = {}".format(i, stream_indexes[deletions[i] - base_graph_index_limit]))
            deletion_strings[i] = stream_indexes[deletions[i] - base_graph_index_limit]
            #stream_index = deletions[i] / chunk_size
            #block_index = deletions[i] - (stream_index * chunk_size)
            #deletion_strings[i] = 

with open(args.input_file, 'r') as dataset:
    for i, l in enumerate(dataset):
        #print("AYYLMAO")
        if i in base_deletion_indexes:
            #print("i={} in base_deletion_indexes".format(i))
            deletion_strings[aux[i]] = l.strip()

    #print(base_deletion_indexes)
    #print(deletion_strings)
    #print(str(len(deletion_strings)))

    deletion_set = set(deletion_strings)

    #print(str(len(deletion_strings)))

    #sys.exit(0)

    with open(out_deletions_path, 'w') as out_deletions_file:
        out_deletions_file.write('\n'.join(deletion_strings) + "\n")
        out_deletions_file.flush()


with open(out_deletions_path, 'r') as out_deletions_file, open(out_stream_path, 'r') as out_stream_file:
    deletion_lines = out_deletions_file.readlines()
    stream_lines = out_stream_file.readlines()
    curr_del_block = 1
    for i in range(len(deletion_lines)):
        # set current max allowed stream file index
        if i % deletion_size == 0:
            curr_del_block = curr_del_block + 1
        max_stream_index = curr_del_block * chunk_size

        curr_del_line = deletion_lines[i]
        # If the current deletion line exists in the edge stream.
        if curr_del_line in stream_lines:
            curr_index = stream_lines.index(curr_del_line)
            # If its index is below the currently allowed max index.
            if curr_index >= max_stream_index:
                print("Illegal deletion position.")
                print("edge {} is at pos {} in the stream and the limit is {}".format(curr_del_line, curr_index, max_stream_index))

        
        # check if the next deletion_size elements in deletion_lines ocurr before the max.
    