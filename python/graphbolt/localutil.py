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

from argparse import ArgumentParser
import os
from typing import Tuple, List

# Global variables.
# Delete STREAM_DELETION_RATIO * stream chunk size edges on each query application.
STREAM_DELETION_RATIO = 0.2

# Utility functions used in GraphBolt's code.


#def init_argparse(parser: ArgumentParser) -> None:
#    parser.add_argument("-m", help="python -m option.", required=False, type=str)

""" def init_argv(argv: List) -> None:
    argc = len(argv)
    for i in range(0, argc):
        if argv[i] == 'python' and (i + 1 < argc) and (i + 2 < argc) and argv[i + 1] == '-m' and argv[i + 2].startswith('graphbolt'):
            del argv[i + 2]
            del argv[i + 1]
            del argv[i]
            break """

# Get the number of lines in a file.
# Used on edge .tsv files.
def file_len(fname: str) -> int:
    with open(fname) as f:
        for i, _ in enumerate(f):
            pass
    return i + 1

def get_big_vertex_params() -> [List, List, List]:
    ##r_values = [0.05, 0.20]
    ##n_values = [0, 1]
    ##delta_values = [0.1, 1]

    r_values = [0.05]
    n_values = [3]
    delta_values = [0.1]
    return r_values, n_values, delta_values

def get_pagerank_data_paths(out_dir: str) -> [str, str, str, str, str, str]:


    if out_dir.startswith('~'):
        out_dir = os.path.expanduser(out_dir).replace('\\', '/')

    evaluation_dir = out_dir + "/Eval/pagerank"
    statistics_dir = out_dir + "/Statistics/pagerank"
    figure_dir = out_dir + "/Figures/pagerank"
    results_dir = out_dir + "/Results/pagerank"

    output_dir = out_dir + "/Outputs"
    streamer_log_dir = out_dir + "/streamer-log/pagerank"

    return evaluation_dir, statistics_dir, figure_dir, results_dir, output_dir, streamer_log_dir

def rreplace(s: str, old: str, new: str, occurrence: str) -> str:
    li = s.rsplit(old, occurrence)
    return new.join(li)


def file_stats(fname: str) -> List:
    acc = 0
    bad_indexes = []
    with open(fname) as f:
        for i, l in enumerate(f):
            # If the line is empty or is a comment (begins with '#')
            if len(l) == 0 or l.startswith("#"):
                acc = acc - 1
                bad_indexes.append(i)
    acc = acc + i + 1
    return acc, bad_indexes

def prepare_stream(stream_file_path: str, query_count: int, deletions_file_path: str = '') -> Tuple[int, List, List, int]:

    with open(stream_file_path, 'r') as edge_file:
        edge_lines = edge_file.readlines()

    if len(deletions_file_path) > 0:
        with open(deletions_file_path, 'r') as deletions_file:
            deletion_lines = deletions_file.readlines()
    else:
        deletion_lines = []
        
    edge_count = file_len(stream_file_path)
    chunk_size = int(edge_count / query_count)
    chunk_sizes = (query_count - 1) * [chunk_size]
    chunk_sizes.append(edge_count - sum(chunk_sizes))

    return chunk_size, chunk_sizes, edge_lines, edge_count, deletion_lines