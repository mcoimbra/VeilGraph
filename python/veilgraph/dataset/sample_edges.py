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
from VEILGRAPH import localutil

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################

# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
DESCRIPTION_TEXT = "VeilGraph edge sampler. Take a graph, sample some edges and produce a new graph file and a stream file."
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

if args.input_file.startswith('~'):
    args.input_file = os.path.expanduser(args.input_file).replace('\\', '/')

###########################################################################
############################### APPLICATION ###############################
###########################################################################

# Count the number of valid edge lines and note indexes of invalid input lines.
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

# Output directory, if it doesn't exist, will be created based on the input file name.
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

if out_dir.startswith('~'):
    out_dir = os.path.expanduser(out_dir).replace('\\', '/')

out_graph_path = os.path.join(out_dir, "{}-start.tsv".format(out_file_name))
out_stream_path = os.path.join(out_dir, "{}-stream.tsv".format(out_file_name))
out_deletions_path = os.path.join(out_dir, "{}-deletions.tsv".format(out_file_name))




print("> Output directory:\t\t{}".format(out_dir))
print("> Out file name base:\t\t{}".format(out_file_name))
print("> Input file:\t\t\t{}".format(args.input_file))
print("> Input line count:\t\t{}".format(input_line_count))
print("> Invalid line count:\t\t{}".format(bad_index_count))
print("> Valid line count:\t\t{}".format(input_line_count - bad_index_count))
print("\n")
print("> Bad indexes:\t{}".format(bad_indexes))
print("\n")
print("> Target stream file:\t\t{}".format(out_stream_path))
print("> Target stream size:\t\t{}".format(stream_size))
print("> Stream sampling probability:\t\t{}".format(p))
print("\n")
print("> Target deletions file:\t{}".format(out_deletions_path))
    

    

# Sample and write resulting base graph and edge stream files.
with open(args.input_file, 'r') as dataset, open(out_graph_path, 'w') as out_graph_file, open(out_stream_path, 'w') as out_stream_file:
    #https://stackoverflow.com/questions/19286657/index-all-except-one-item-in-python
    base_lines = []
    stream_lines = []
    sample_count = 0
    valid_ctr = 0       

    for i, l in enumerate(dataset):

        # If the line is not empty and is not a comment (begins with '#').
        if not i in bad_indexes:
            p = (stream_size - sample_count) / (input_line_count - valid_ctr)
            valid_ctr = valid_ctr + 1
            if sample_count != stream_size and random.random() < p:
                #if args.debug:
                    #print("> Stream sampled (p={}) {}/{}\t(input position: {}).".format(p, sample_count+1, stream_size, i))
                sample_count = sample_count + 1
                stream_lines.append(l.strip())
            else:
                base_lines.append(l.strip())
                
                # Efficient storage of the base graph lines to disk.
                if len(base_lines) == io.DEFAULT_BUFFER_SIZE:
                    out_graph_file.write('\n'.join(base_lines) + "\n")
                    base_lines = []

            if valid_ctr == input_line_count:
                break
        
    # Write the last set of edges, whose number was less than 'io.DEFAULT_BUFFER_SIZE'.
    if len(base_lines) > 0:
        len_diff = stream_size - len(stream_lines)
        if args.debug:
            print("> Length difference:\t{}".format(len_diff))
        if len_diff > 0:
            aux_draw = random.sample(base_lines, len_diff)
            print("> Stream supplement draw:\t{}".format(aux_draw))
            stream_lines = stream_lines + aux_draw
            base_lines = [l for l in base_lines if l not in aux_draw]
        if len(base_lines) > 0:
            out_graph_file.write('\n'.join(base_lines) + "\n")

    # Is stream order randomization required?
    if args.randomize:
        random.shuffle(stream_lines)

    # Store the stream edges to disk.
    out_stream_file.write('\n'.join(stream_lines) + "\n")
    out_stream_file.flush()

    # Get the chunk properties for the generated stream.
    chunk_size, chunk_sizes, _, stream_edge_count, _ = localutil.prepare_stream(out_stream_path, args.query_count)
    print("> Stream chunk size:\t\t{}".format(chunk_size))
    print("> Stream chunk count:\t\t{}".format(len(chunk_sizes)))
    print("> Stream edge count:\t\t{}".format(stream_edge_count))
    print('> Stream line count:\t\t{}'.format(len(stream_lines)))
    print('\n')

    

    # Number of edges deletions sent each time a block of the update stream is sent.
    deletion_size = int(args.deletion_ratio * chunk_size)
    

    # On what stream block are we?
    block_acc = 0
    base_graph_index_limit = valid_ctr + bad_index_count - stream_edge_count
    deletions = []
    for i in range(len(chunk_sizes)):

        if args.debug:
            print('> Drawing {} lines from interval [{};{}={} + {}]'.format(
                deletion_size, 
                bad_index_count, 
                base_graph_index_limit + block_acc,
                base_graph_index_limit,
                block_acc))

        # Draw a sample from the original graph plus all the previous chunk blocks we've already iterated over.
        draw_interval_range = range(bad_index_count, base_graph_index_limit + block_acc)


        # PROBLEM: since the stream was shuffled, for a base graph with 10000 edges and a stream of size 1000, it's possible that stream element 0015 (belonging to first stream chunk before shuffling) was shuffled into position 0859. This means there will be a deletion order for an edge that hasn't been added yet.
        print("> Draw interval range:\t{}".format(draw_interval_range))
        
        # Keep drawing samples until there isn't a single repetition in 'deletions'.
        
        while True:
            del_block = random.sample(draw_interval_range, deletion_size)
            repetitions = 0
            for j in del_block:
                if j in deletions:
                    repetitions = repetitions + 1
                    break
            if repetitions == 0:
                break
        
        deletions = deletions + del_block

        # 'block_acc' sets the upper limit for the sampling procedure.
        block_acc = block_acc + chunk_sizes[i]

    # 'deletions' contains indexes associated with the sampled edges to be deleted.
    # Need to retrieve the associated edge strings.
    deletion_strings = len(deletions) * ['']

    if args.debug:
        print("> Deletion count:\t{}".format(len(deletions)))
        print("> Base graph index limit:\t{}".format(base_graph_index_limit))

    #base_deletion_indexes = []
    base_deletion_indexes = set()
    aux = {}
    for i in range(len(deletions)):
        # String is part of the base graph file.
        if args.debug:
            print("> Current deletion:\t{}\t{}".format(i, deletions[i]))

        # If current edge to delete is part of the base graph ('..-start.tsv')
        if deletions[i] < base_graph_index_limit:
            #base_deletion_indexes.append(deletions[i])
            base_deletion_indexes.add(deletions[i])
            aux[deletions[i]] = i
        # String is part of a specific stream block.
        else:
            # Find the index inside the specific block and retrieve the string.
            global_stream_index = deletions[i] - base_graph_index_limit
            single_block_index = chunk_size * int((global_stream_index / chunk_size))
            inner_block_index = global_stream_index - single_block_index
            single_block_index = int(single_block_index / chunk_size)
            
            if args.debug:
                print("> Drawing from block:\t{}\t({}:{})".format(global_stream_index, single_block_index, inner_block_index))
                print("> deletion_strings[{}] = {}".format(i, stream_lines[global_stream_index]))
            deletion_strings[i] = stream_lines[global_stream_index]

# Reread the input file to write the deletions file.

#with open(args.input_file, 'r') as dataset, open(out_deletions_path, 'w') as out_deletions_file:
with open(out_graph_path, 'r') as dataset, open(out_deletions_path, 'w') as out_deletions_file:
    for i, l in enumerate(dataset):
        #if (i % 20000 == 0):
        #    print('> Input file line:\t{}'.format(i))
        #    print('> {}'.format(l.strip()))
        if i in base_deletion_indexes:
            deletion_strings[aux[i]] = l.strip()
    
    out_deletions_file.write('\n'.join(deletion_strings) + "\n")
    out_deletions_file.flush()

if args.debug:
    print("\n")

# Check if the deletion stream order is coherent with the update stream order.
with open(out_deletions_path, 'r') as out_deletions_file, open(out_stream_path, 'r') as out_stream_file:
    deletion_lines = out_deletions_file.readlines()
    stream_lines = out_stream_file.readlines()
    curr_del_block = 1
    for i in range(len(deletion_lines)):

        # Set current max allowed stream file index.
        if i % deletion_size == 0:
            curr_del_block = curr_del_block + 1
        max_stream_index = curr_del_block * chunk_size
        curr_del_line = deletion_lines[i].strip()

        # If the current deletion line exists in the edge stream.
        if curr_del_line in stream_lines:
            curr_index = stream_lines.index(curr_del_line)

            # If its index is below the currently allowed max index.
            if curr_index >= max_stream_index:
                print("> Illegal deletion position.")
                print("> edge {} is at pos {} in the stream and the limit is {}".format(curr_del_line, curr_index, max_stream_index))
                sys.exit(1)