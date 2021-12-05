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
import sys
import time
from typing import Tuple, List


# Global variables.
# Delete STREAM_DELETION_RATIO * stream chunk size edges on each query application.
STREAM_DELETION_RATIO = 0.2

# Utility functions used in VeilGraph's code.


# Get the number of lines in a file.
# Used on edge .tsv files.
def file_len(fname: str) -> int:
    with open(fname, 'r') as f:
        for i, _ in enumerate(f):
            pass
        return i + 1

# def get_big_vertex_params() -> [List, List, List]:

#     # r_values = [0.15]
#     # n_values = [1]
#     # delta_values = [0.5]

#     r_values = [0.05, 0.15, 0.25]
#     n_values = [0, 1, 2, 3]
#     delta_values = [0.1, 0.5, 1.0]
#     return r_values, n_values, delta_values

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
    bad_indexes = []
    with open(fname) as f:
        for i, l in enumerate(f):
            # If the line is empty or is a comment (begins with '#')
            if len(l) == 0 or l.startswith("#"):
                bad_indexes.append(i)
    return i + 1, bad_indexes

def prepare_stream(stream_file_path: str, query_count: int, deletions_file_path: str = '') -> Tuple[int, List, List, int]:

    print("> Stream file path:\t{}".format(stream_file_path))
    with open(stream_file_path, 'r') as stream_file:
        stream_lines = stream_file.readlines()
        stream_line_count = len(stream_lines)

    # If deletions path is empty, we are not considering an edge deletions file.
    if len(deletions_file_path) > 0:
        print("> Deletions file path:\t{}".format(deletions_file_path))
        with open(deletions_file_path, 'r') as deletions_file:
            deletion_lines = deletions_file.readlines()
    else:
        deletion_lines = []
    
    # The stream blocks all have a size of 'chunk_size' except for the last one.
    chunk_size = int(stream_line_count / query_count)
    chunk_sizes = (query_count - 1) * [chunk_size]

    # Last block is has the leftover at the end of the stream.
    chunk_sizes.append(stream_line_count - sum(chunk_sizes))

    return chunk_size, chunk_sizes, stream_lines, stream_line_count, deletion_lines