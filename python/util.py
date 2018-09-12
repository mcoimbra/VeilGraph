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

from typing import List

# Utility functions used in GraphBolt's code.

# Get the number of lines in a file.
# Used on edge .tsv files.
def file_len(fname: str) -> int:
    with open(fname) as f:
        for i, _ in enumerate(f):
            pass
    return i + 1

def get_big_vertex_params() -> [List, List, List]:
    r_values = [0.05, 0.20]
    n_values = [0, 1]
    delta_values = [0.1, 1]
    return r_values, n_values, delta_values

def get_pagerank_data_paths(out_dir: str) -> [str, str, str, str, str, str]:
    evaluation_dir = out_dir + "/Eval/pagerank"
    statistics_dir = out_dir + "/Statistics/pagerank"
    figure_dir = out_dir + "/Figures/pagerank"
    results_dir = out_dir + "/Results/pagerank"

    output_dir = out_dir + "/Outputs"
    streamer_log_dir = out_dir + "/streamer-log/pagerank"

    return evaluation_dir, statistics_dir, figure_dir, results_dir, output_dir, streamer_log_dir




    