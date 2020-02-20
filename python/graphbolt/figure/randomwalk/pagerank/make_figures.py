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

# Used to launch combinations of parameters for different GraphBolt 
# PageRank executions.
# http://sbillaudelle.de/2015/02/20/matplotlib-with-style.html
#
# On PercentFormatter arguments: https://matplotlib.org/api/ticker_api.html#module-matplotlib.ticker
# Custom ticker: https://matplotlib.org/examples/pylab_examples/custom_ticker1.html
# LaTeX embedding: http://sbillaudelle.de/2015/02/23/seamlessly-embedding-matplotlib-output-into-latex.html

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
import argparse
from collections import OrderedDict
import getpass
from io import TextIOWrapper
import os
import pathlib # https://docs.python.org/3.6/library/pathlib.html#pathlib.Path.mkdir
import string
import sys
from typing import Dict, List

# 2. related third party imports
from matplotlib.lines import Line2D
import matplotlib.ticker as mtick
import matplotlib.pyplot as plt
from matplotlib import rc
from matplotlib import rcParams

import numpy as np

# 3. custom imports
# https://stackoverflow.com/questions/48324056/in-python-how-can-i-import-from-a-grandparent-folder-with-a-relative-path?noredirect=1&lq=1
##from graphbolt import util
#FILE_ABSOLUTE_PATH = os.path.abspath(__file__)  # get absolute filepath
#CURRENT_DIR = os.path.dirname(FILE_ABSOLUTE_PATH)  # get directory path of file
#PARENT_DIR = os.path.dirname(CURRENT_DIR)  # get parent directory path
#BASE_DIR = os.path.dirname(PARENT_DIR)  # get grand parent directory path
# or you can directly get grandparent directory path as below
#GRAPHBOLT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#sys.path.append(GRAPHBOLT_ROOT_DIR)  # append the path to system
from graphbolt import localutil
from graphbolt.figure import matplotlib_config


###########################################################################
############################### FUNCTIONS #################################
###########################################################################

#TODO: write this in standard function documentation for Python 3
#TODO: perhaps this function could be moved into localutil.py

# This function is used to read the values of a .tsv file into 
# a dictionary.
# The first line of the file is expected to contain the names 
# of the fields.
# Each field name will be used as a key in the dictionary, while 
# the corresponding dictionary value will be a list of statistics.
def read_statistics_into_dic(statistics_file_path: List, query_count: int) -> Dict:

    #[execution_count, total_vertex_num, total_edge_num, summary_vertex_num, summary_edge_num, internal_edge_count, external_edge_count, ranks_to_send, edges_to_inside, expansion_length, computation_time, pagerank_iterations] = stat_lines[0].split(';')

    matrix = {}

    for stat_file_path in statistics_file_path:

        print("Reading statistics file:\t{}".format(stat_file_path))

        with open(stat_file_path, 'r') as statistics:
            stat_lines = statistics.readlines()
            
            # Get the column names from the statistics file.
            #print(stat_lines)
            stat_names = stat_lines[0].strip().split(';')
            
            for ind in range(len(stat_names)):
                matrix[stat_names[ind]] = [0] * query_count

            # If the file has summary graph statistics, prepare to store them too.
            if 'summary_vertex_num' in stat_names:
                matrix['summary_vertex_ratio'] = [0] * query_count
            if 'summary_edge_num' in stat_names:
                matrix['summary_edge_ratio'] = [0] * query_count
            
            for line_index, line in enumerate(stat_lines[1:]):
                
                stat_values = line.strip().split(';')

                for ind in range(len(stat_values)):
                    # Index 10 of the stat_values tuple contains the PageRank execution time, hence we store as float.

                    if stat_names[ind].endswith("_time"):
                        stat_values[ind] = float(stat_values[ind]) / 1000
                    else:
                        stat_values[ind] = int(stat_values[ind])

                    matrix[stat_names[ind]][line_index] = stat_values[ind]

                # If the file has summary graph statistics, store them too.
                if 'summary_vertex_num' in matrix:
                    matrix['summary_vertex_ratio'][line_index] = 100 * matrix['summary_vertex_num'][line_index] / matrix['total_vertex_num'][line_index]
                if 'summary_edge_num' in matrix:
                    matrix['summary_edge_ratio'][line_index] = 100 * matrix['summary_edge_num'][line_index] / matrix['total_edge_num'][line_index]

    return matrix


def save_figure(path: str, extension_list: List) -> None:
    for ext in extension_list:
        figure_path = path + ext
        print('> Saving:\t{}'.format(figure_path))
        plt.savefig(figure_path, transparent = True)

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################

# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
DESCRIPTION_TEXT = "GraphBolt PageRank figure generator."
parser = argparse.ArgumentParser(description=DESCRIPTION_TEXT, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-plot-single-figures", help="skip parameter-specific figure generation. Only plot groups of combinations of parameters", required=False, action="store_true") # if ommited default value is false
parser.add_argument("-png", help="output .png files too", required=False, action="store_true") # if ommited default value is false
parser.add_argument("-pdf", help="output .pdf files too", required=False, action="store_true") # if ommited default value is false

# GraphBolt/PageRank-specific parameters.
parser.add_argument("-skip-flink-job-stats", help="skip Flink job operator statistics figure generation.", required=False, action="store_true") # if ommited default value is false

parser.add_argument("-delete-edges", help="should edge deletions be sent in the stream?", required=False, action="store_true") # if ommited, default value is false
parser.add_argument("-dataset-name", help="dataset name.", required=True, type=str, default="")
parser.add_argument("-data-dir", help="dataset directory name.", required=True, type=str, default="")
parser.add_argument("-out-dir", help="base output directory where directories for statistics, RBO results, logging, evaluation and figures will be created.", required=True, type=str, default="")
parser.add_argument("-chunks", "--chunk-count", help="set desired number of chunks to be sent by the streamer.", required=True, type=int, default=50)
parser.add_argument("-size", help="set desired GraphBolt RBO rank length.", required=True, type=int, default=1000)
parser.add_argument("-damp", "--dampening", help="set desired PageRank dampening factor.", required=False, type=float, default=0.85)
parser.add_argument("-iterations", help="set desired PageRank power-method iteration count.", required=False, type=int, default=30)

parser.add_argument("-skip-rbo", help="skip RBO plots?", required=False, action="store_true") # if ommited, default value is false

parser.add_argument('-l','--list', nargs='+', help='<Required> Set flag', required=False, default=None)

parser.add_argument("-p", "--parallelism", help="set desired GraphBolt TaskManager parallelism.", required=False, type=int, default=1)

args = parser.parse_args()

# Sanitize arguments and exit on invalid values.
if (args.list == None) or (args.list != None) and (len(args.list) <= 0 or (len(args.list) % 3 ) != 0):
    print("> Big vertex parameter list must be a multiple of three. Exiting.")
    sys.exit(1)

if args.chunk_count <= 0 or not isinstance(args.chunk_count, int):
    print("> '-chunks' must be a positive integer. Exiting.")
    sys.exit(1)
if args.data_dir.startswith('~'):
        args.data_dir = os.path.expanduser(args.data_dir).replace('\\', '/')
if not (os.path.exists(args.data_dir) and os.path.isdir(args.data_dir)):
    print("> 'data_dir' must be an existing directory.\nProvided:\t{}\nExiting.".format(args.data_dir))
    sys.exit(1)
if args.iterations <= 0 or not isinstance(args.iterations, int):
    print("> '-iterations' must be a positive integer. Exiting.")
    sys.exit(1)
if args.dampening <= 0 or not isinstance(args.dampening, float):
    print("> '-dampening' must be a positive float in ]0; 1[. Exiting.")
    sys.exit(1)
if args.size <= 0 or not isinstance(args.size, int):
    print("> '-size' must be a positive integer. Exiting.")
    sys.exit(1)
if args.out_dir.startswith('~'):
        args.out_dir = os.path.expanduser(args.out_dir).replace('\\', '/')
if not (os.path.exists(args.out_dir) and os.path.isdir(args.out_dir)):
    print("> '-out-dir' must be a non-empty string. Exiting")
    sys.exit(1)
if args.parallelism <= 0 or not isinstance(args.parallelism, int):
    print("> '-parallelism' must be a positive integer. Exiting.")
    sys.exit(1)

DELETE_TOKEN = '_A'
if args.delete_edges:
    DELETE_TOKEN = '_D'

print("\n> Arguments: {}".format(args))

# Default is to print figures only in LaTeX-friendly .pgf format.
# Check if user asked for additional formats.
fig_file_types = [".pgf"]
if args.png:
    fig_file_types.append(".png")
if args.pdf:
    fig_file_types.append(".pdf")


###########################################################################
############################ CHECK DIRECTORIES ############################
###########################################################################



EVAL_DIR, STATISTICS_DIR, FIGURES_DIR, _, _, _ = localutil.get_pagerank_data_paths(args.out_dir)


# Make necessary GraphBolt directories if they don't exist.
print("> Checking GraphBolt directories...\n")


if (not args.skip_rbo) and (not os.path.exists(EVAL_DIR)):
    print("Evaluation directory not found:\t\t'{}'".format(EVAL_DIR))
    sys.exit(1)
elif (not args.skip_rbo):
    print("Evaluation directory found:\t\t'{}'".format(EVAL_DIR))

if not os.path.exists(STATISTICS_DIR):
    print("Statistics directory not found:\t\t'{}'".format(STATISTICS_DIR))
    sys.exit(1)
else:
    print("Statistics directory found:\t\t'{}'".format(STATISTICS_DIR))

if not os.path.exists(FIGURES_DIR):
    print("Creating figures directory:\t\t'{}'\n".format(FIGURES_DIR))
    pathlib.Path(FIGURES_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Figures directory found:\t\t'{}'\n".format(FIGURES_DIR))


###########################################################################
############################ GENERATE FIGURES #############################
###########################################################################

# Execute figure generation for different parameters.
#r_values, n_values, delta_values = localutil.get_big_vertex_params()

# Dictionary to store GraphBolt PageRank statistics for each parameter combination that was found.
result_statistic_matrices = {}

# Dictionary to store PageRank RBO values for each parameter combination that was found.
result_rbo_matrices = {}

complete_pagerank_stats_path = '{KW_STATISTICS_DIR}/{KW_DATASET_NAME}-start_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_complete{KW_DELETE_TOKEN}/{KW_DATASET_NAME}-start.tsv'.format(
            KW_STATISTICS_DIR = STATISTICS_DIR, KW_DELETE_TOKEN = DELETE_TOKEN, KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening)
complete_pagerank_stats_matrix = read_statistics_into_dic([complete_pagerank_stats_path], args.chunk_count)

stream_file_path = "{KW_DATA_DIR}/{KW_DATASET_NAME}/{KW_DATASET_NAME}-stream.tsv".format(KW_DATA_DIR = args.data_dir, KW_DATASET_NAME = args.dataset_name)

# Parse statistics and generate figures for individual summarized executions.
# for r in r_values:
#     for n in n_values:
#         for delta in delta_values:
if args.list != None:
    arg_indexes = 0
    print("Iterating summarized PageRank parameters...")
    while arg_indexes < len(args.list):
        r = float(args.list[arg_indexes])
        n = int(args.list[arg_indexes+1])
        delta = float(args.list[arg_indexes+2])

        arg_indexes = arg_indexes + 3
            
        indexes = []
        rbos = []
        
        # Label to use in subsequent plots. Also used as a key for the map of GraphBolt statistic matrices.
        key_label = r'\textit{r} = KW_r, \textit{n} = KW_n, $\Delta$ = KW_delta'.replace("KW_r", "{KW_r:.2f}".format(KW_r = r)).replace("KW_n", "{KW_n}".format(KW_n = n)).replace("KW_delta", "{KW_delta:.2f}".format(KW_delta = delta)) 


        graphbolt_output_eval_path = '{KW_EVAL_DIR}/{KW_DATASET_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}{KW_DELETE_TOKEN}.csv'.format(
        KW_EVAL_DIR = EVAL_DIR, KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_r = r, KW_n = n, KW_delta = delta, KW_DELETE_TOKEN = DELETE_TOKEN)


        if (not args.skip_rbo):
            ##### Make RBO figure.
            print("> Using eval path for figures:\t{}".format(graphbolt_output_eval_path)) #####
            if os.path.isfile(graphbolt_output_eval_path) and os.stat(graphbolt_output_eval_path).st_size > 0:
                file_sz = localutil.file_len(graphbolt_output_eval_path)
            else:
                file_sz = -1
            print("> File has {} lines.".format(file_sz))
            
            if os.path.isfile(graphbolt_output_eval_path) and file_sz  == args.chunk_count + 1:
                with open(graphbolt_output_eval_path, 'r') as eval_results:
                    rbo_lines = eval_results.readlines()

                    for l in rbo_lines:
                        i, rbo = l.split(";")
                        rbos.append(float(rbo))
                        indexes.append(int(i))

                # Create a dedicated directory for the current dataset and PageRank parameters.
                figure_base_name = '{KW_DATASET_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}{KW_DELETE_TOKEN}'.format(KW_FIGURES_DIR = FIGURES_DIR, KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_DELETE_TOKEN = DELETE_TOKEN)

                # Single-plot figures will be crowded together in a specific 'singles' directory.
                figure_singles_new_dir_path = '{KW_FIGURES_DIR}/{KW_FIGURE_BASE_NAME}/singles'.format(KW_FIGURES_DIR = FIGURES_DIR, KW_FIGURE_BASE_NAME = figure_base_name)

                # Create 'singles' directory.
                pathlib.Path(figure_singles_new_dir_path).mkdir(parents=True, exist_ok=True)

                # Set the figure base name for the GraphBolt vertex K set parameters.
                figure_path_name = '{KW_SINGLE_FIGURES_DIR}/{KW_FIGURE_BASE_NAME}_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}'.format(KW_SINGLE_FIGURES_DIR = figure_singles_new_dir_path, KW_FIGURE_BASE_NAME = figure_base_name, KW_r = r, KW_n = n, KW_delta = delta)
                
                
                result_rbo_matrices[key_label] = rbos

                print("r: {}\tn: {}\tdelta: {}".format(r, n, delta))
                #print(result_rbo_matrices)

                if(args.plot_single_figures):
                    print('> Generating figures for parameters r:{KW_r:.2f} n:{KW_n} delta:{KW_delta:.2f}.'.format(KW_r = r, KW_n = n, KW_delta = delta))
                    fig, ax = plt.subplots()
                    plt.xlabel("Number of PageRank executions")
                    plt.ylabel("PageRank similarity measure (RBO)")
                    plt.yticks(rotation=45)

                    plot_rbo = [100 * y for y in rbos]

                    ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))
                    plt.plot(indexes, plot_rbo, marker=matplotlib_config.styles[0], alpha=matplotlib_config.PLOT_ALPHA)
                    plt.title(key_label)
                    ax.set_xlim(left=0, right=args.chunk_count)
                    x_step = int(args.chunk_count / 10)
                    new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [0]
                    plt.xticks(new_xticks)
                    #plt.legend()
                    save_figure(figure_path_name + '-RBO', fig_file_types)

                    plt.close(fig) #####


        
        # Read the statistics file for the current parameter combination.
        summary_pagerank_stats_path = '{KW_STATISTICS_DIR}/{KW_DATASET_NAME}-start_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_model_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}{KW_DELETE_TOKEN}/{KW_DATASET_NAME}-start.tsv'.format(
        KW_STATISTICS_DIR = STATISTICS_DIR, KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_r = r, KW_n = n, KW_delta = delta, KW_DELETE_TOKEN = DELETE_TOKEN)

        big_vertex_stats_path = '{KW_STATISTICS_DIR}/{KW_DATASET_NAME}-start_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_model_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}{KW_DELETE_TOKEN}/model_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}.tsv'.format(
        KW_STATISTICS_DIR = STATISTICS_DIR, KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_r = r, KW_n = n, KW_delta = delta, KW_DELETE_TOKEN = DELETE_TOKEN)


        pagerank_file_ok = os.path.isfile(summary_pagerank_stats_path) and os.stat(summary_pagerank_stats_path).st_size > 0 and localutil.file_len(summary_pagerank_stats_path) == args.chunk_count + 1

        big_vertex_file_ok = os.path.isfile(big_vertex_stats_path) and os.stat(big_vertex_stats_path).st_size > 0 and localutil.file_len(big_vertex_stats_path) == args.chunk_count + 1

        if not pagerank_file_ok:
            print("PageRank results file had a problem: " + summary_pagerank_stats_path)
            sys.exit(1)
        elif not big_vertex_file_ok:
            print("Big vertex file had a problem: " + summary_pagerank_stats_path)
            sys.exit(1)
        else:

            stat_files = [summary_pagerank_stats_path, big_vertex_stats_path]
            summary_pagerank_stats_matrix = read_statistics_into_dic(stat_files, args.chunk_count)

            #print("r: {}\tn: {}\tdelta: {}".format(r, n, delta))
            #print(summary_pagerank_stats_matrix)

            # Programmatically store the speedup values.
            summary_pagerank_stats_matrix['speedup'] = [exact_time / summarized_time for summarized_time,exact_time in zip(summary_pagerank_stats_matrix["computation_time"], complete_pagerank_stats_matrix["computation_time"])]

            summary_pagerank_stats_matrix['inv_speedup'] = [summarized_time / exact_time for summarized_time,exact_time in zip(summary_pagerank_stats_matrix["computation_time"], complete_pagerank_stats_matrix["computation_time"])]

            result_statistic_matrices[key_label] = summary_pagerank_stats_matrix

            ##### Make statistics figures specific to each individual parameter combination.
            if(args.plot_single_figures):
                ###### PLOT summary graph vertex and edge count as fraction of complete graph.
                # Custom ticker: https://matplotlib.org/examples/pylab_examples/custom_ticker1.html
                fig, ax = plt.subplots()
                # On PercentFormatter arguments: https://matplotlib.org/api/ticker_api.html#module-matplotlib.ticker
                ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))
                plt.xlabel("Number of PageRank executions")
                plt.ylabel("Summary vertices and edges as fractions of total graph")
                plt.yticks(rotation=45)
                plt.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['summary_vertex_ratio'], label=r"$\vert$V$\vert$", marker=matplotlib_config.styles[0], alpha=matplotlib_config.PLOT_ALPHA)
                plt.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['summary_edge_ratio'], label=r"$\vert$E$\vert$", marker=matplotlib_config.styles[1], alpha=matplotlib_config.PLOT_ALPHA)
                ax.set_title(key_label)
                ax.set_xlim(left=1, right=args.chunk_count)
                plt.title(key_label)
                plt.legend()
                
                x_step = int(args.chunk_count / 10)
                new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
                plt.xticks(new_xticks)
                save_figure(figure_path_name + '-Savings', fig_file_types)


                #####################
                ###### PLOT execution time AND summary graph vertex and edge count as fraction of complete graph.
                fig, ax = plt.subplots()
                # On PercentFormatter arguments: https://matplotlib.org/api/ticker_api.html#module-matplotlib.ticker
                ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))

                plt.xlabel("Number of PageRank executions")
                plt.ylabel("Summary vertices and edges as fractions of total graph")
                plt.yticks(rotation=45)
                plt.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['summary_vertex_ratio'], label=r"$\vert$V$\vert$", marker=matplotlib_config.styles[0], color=matplotlib_config.colors[0], alpha=matplotlib_config.PLOT_ALPHA)
                plt.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['summary_edge_ratio'], label=r"$\vert$E$\vert$", marker=matplotlib_config.styles[1], color=matplotlib_config.colors[1], alpha=matplotlib_config.PLOT_ALPHA)
                ax.set_title(key_label)
                ax.set_xlim(left=1, right=args.chunk_count)
                plt.title(key_label)
                plt.legend()
                
                
                x_step = int(args.chunk_count / 10)
                new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
                plt.xticks(new_xticks)
                ax_c = ax.twinx()
                ax_c.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['computation_time'], label="PageRank Time", marker=matplotlib_config.styles[2], color=matplotlib_config.colors[2], alpha=matplotlib_config.PLOT_ALPHA)
                ax_c.set_ylabel("Summarized PageRank execution time")
                ax_c.yaxis.set_major_formatter(mtick.FormatStrFormatter('%d s'))
                plt.legend()

                save_figure(figure_path_name + '-Savings_Time', fig_file_types)

                plt.close(fig)

                ###### PLOT PageRank time figure.
                fig = plt.figure()

                # Custom ticker: https://matplotlib.org/examples/pylab_examples/custom_ticker1.html
                # On PercentFormatter arguments: https://matplotlib.org/api/ticker_api.html#module-matplotlib.ticker

                plt.xlabel("Number of PageRank executions")
                plt.ylabel("Summarized PageRank execution time")
                plt.title(key_label)
                plt.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['computation_time'], label = key_label, alpha=matplotlib_config.PLOT_ALPHA)
                ax = plt.gca()
                x_step = int(args.chunk_count / 10)
                new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
                plt.xticks(new_xticks)
                ax.set_xlim(left=1, right=args.chunk_count)
                plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%d s'))
                plt.legend()
                
                save_figure(figure_path_name + '-Time', fig_file_types)
                plt.close(fig)

                ###### PLOT PageRank speedup figure.
                fig = plt.figure()
                plt.xlabel("Number of PageRank executions")
                plt.ylabel("Summarized PageRank execution speedup")
                plt.title(key_label)
                plt.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['speedup'], label = key_label, alpha=matplotlib_config.PLOT_ALPHA)
                #
                #plt.plot(summary_pagerank_stats_matrix['execution_count'], summary_pagerank_stats_matrix['computation_time'], label = key_label, alpha=matplotlib_config.PLOT_ALPHA)
                ax = plt.gca()
                x_step = int(args.chunk_count / 10)
                new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
                plt.xticks(new_xticks)
                ax.set_xlim(left=1, right=args.chunk_count)
                #plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%d s'))
                plt.legend()
                
                save_figure(figure_path_name + '-Speedup', fig_file_types)

                plt.close(fig)

                #print(summary_pagerank_stats_matrix)

                    

# Create a directory for the figures of the (current dataset, iteration count, RBO length, dampening factor) combination.
figure_path_dir = '{KW_FIGURES_ROOT}/{KW_DATASET_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}{KW_DELETE_TOKEN}'.format(
            KW_FIGURES_ROOT = FIGURES_DIR, KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_DELETE_TOKEN = DELETE_TOKEN)

pathlib.Path(figure_path_dir).mkdir(parents=True, exist_ok=True)

figure_path_name = '{KW_FIGURES_DIR}/{KW_DATASET_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}{KW_DELETE_TOKEN}'.format(
            KW_FIGURES_DIR = figure_path_dir, KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism,  KW_DAMPENING_FACTOR = args.dampening, KW_DELETE_TOKEN = DELETE_TOKEN)

sorted_matrices = sorted(list(result_statistic_matrices.items()), key=lambda m: np.mean(m[1]["computation_time"]))
time_ordered_matrices = OrderedDict(sorted_matrices)

CATEGORY_BEST_PLOT_COUNT = 3
CATEGORY_WORST_PLOT_COUNT = 3

fig, ax = plt.subplots()
plt.ylabel("Summarized PageRank execution time")

PLOT_LIMIT = 5
print("PLOT_ALPHA:\t{}".format(matplotlib_config.PLOT_ALPHA))
##### PLOT the five lowest summarized PageRank execution times.
for plot_counter, k in enumerate(time_ordered_matrices.keys()):
    matrix = time_ordered_matrices[k]
    if plot_counter == PLOT_LIMIT:
        break
    color = matplotlib_config.colors[plot_counter % len(matplotlib_config.colors)]
    style = matplotlib_config.styles[(plot_counter - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]

    plt.plot(matrix["execution_count"], matrix["computation_time"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%d s'))
plt.legend()
save_figure(figure_path_name + '-Time-top-{}'.format(PLOT_LIMIT), fig_file_types)
plt.close(fig)



##### PLOT the five lowest summarized PageRank execution times divided by the exact version time.
fig, ax = plt.subplots()
plt.ylabel("Summarized PageRank execution time as a fraction of complete time")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))

for plot_counter, k in enumerate(time_ordered_matrices.keys()):
    matrix = time_ordered_matrices[k]
    if plot_counter == PLOT_LIMIT:
        break
    color = matplotlib_config.colors[plot_counter % len(matplotlib_config.colors)]
    style = matplotlib_config.styles[(plot_counter - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
    plt.plot(matrix["execution_count"], matrix["inv_speedup"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)
x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
plt.legend()



save_figure(figure_path_name + '-Time-vs-exact-inv-speedup-top-{}'.format(PLOT_LIMIT), fig_file_types)

plt.close(fig)

##### PLOT the three lowest and three highest summarized PageRank execution times divided by the exact version time.
fig, ax = plt.subplots()
plt.ylabel("Summarized PageRank execution time as a fraction of complete time")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))

matrix_count = len(time_ordered_matrices.keys())
style_ctr = 0
for plot_counter, k in enumerate(time_ordered_matrices.keys()):
    matrix = time_ordered_matrices[k]
    if plot_counter < CATEGORY_BEST_PLOT_COUNT or plot_counter >= matrix_count  - CATEGORY_WORST_PLOT_COUNT:
        color = matplotlib_config.colors[style_ctr % len(matplotlib_config.colors)]
        style = matplotlib_config.styles[(style_ctr - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
        style_ctr = style_ctr + 1
        plt.plot(matrix["execution_count"], matrix["inv_speedup"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
plt.legend()


save_figure(figure_path_name + '-Time-vs-exact-inv-speedup-top-{}-bottom-{}'.format(CATEGORY_BEST_PLOT_COUNT, CATEGORY_WORST_PLOT_COUNT), fig_file_types)


plt.close(fig)

##### PLOT the five highest summarized PageRank speedups.
fig, ax = plt.subplots()
plt.ylabel("Summarized PageRank execution speedup")
for plot_counter, k in enumerate(time_ordered_matrices.keys()):
    matrix = time_ordered_matrices[k]
    if plot_counter == PLOT_LIMIT:
        break
    color = matplotlib_config.colors[plot_counter % len(matplotlib_config.colors)]
    style = matplotlib_config.styles[(plot_counter - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
    plt.plot(matrix["execution_count"], matrix["speedup"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
plt.legend()

top_5_speedups = "-Time-vs-exact-speedup-top-{}".format(PLOT_LIMIT)

save_figure(figure_path_name + top_5_speedups, fig_file_types)

plt.close(fig)

##### PLOT the three highest and three lowest summarized PageRank speedups.
fig, ax = plt.subplots()
plt.ylabel("Summarized PageRank execution speedup")
matrix_count = len(time_ordered_matrices.keys())

style_ctr = 0
for plot_counter, k in enumerate(time_ordered_matrices.keys()):

    matrix = time_ordered_matrices[k]

    if plot_counter < CATEGORY_BEST_PLOT_COUNT or plot_counter >= matrix_count  - CATEGORY_WORST_PLOT_COUNT:
        color = matplotlib_config.colors[style_ctr % len(matplotlib_config.colors)]
        style = matplotlib_config.styles[(style_ctr - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
        style_ctr = style_ctr + 1

        plt.plot(matrix["execution_count"], matrix["speedup"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
plt.legend()

top_3_bottom_3_speedups = "-Time-vs-exact-speedup-top-{}-bottom-{}".format(CATEGORY_BEST_PLOT_COUNT, CATEGORY_WORST_PLOT_COUNT)


save_figure(figure_path_name + top_3_bottom_3_speedups, fig_file_types)

plt.gca().axes.get_xaxis().set_ticks([])
save_figure(figure_path_name + top_3_bottom_3_speedups + '_nolabels', fig_file_types)

plt.close(fig)

##### PLOT the five lowest summarized fraction of vertices.
fig, ax = plt.subplots()
plt.ylabel("Summary vertex count as a fraction of total vertices")
plt.yticks(rotation=45)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))

sorted_matrices = sorted(list(result_statistic_matrices.items()), key=lambda m: np.mean(m[1]["summary_vertex_ratio"]))
summary_vertex_ordered_matrices = OrderedDict(sorted_matrices)

for plot_counter, k in enumerate(summary_vertex_ordered_matrices.keys()):

    if plot_counter == PLOT_LIMIT:
        break

    matrix = summary_vertex_ordered_matrices[k]
    color = matplotlib_config.colors[plot_counter % len(matplotlib_config.colors)]
    style = matplotlib_config.styles[(plot_counter - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]

    plt.plot(matrix["execution_count"], matrix["summary_vertex_ratio"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.legend()


top_5_vertex_savings = "-Savings-vertex-top-{}".format(PLOT_LIMIT)

save_figure(figure_path_name + top_5_vertex_savings, fig_file_types)

plt.close(fig)

##### PLOT the three lowest and three highest summarized fraction of vertices.
fig, ax = plt.subplots()
plt.ylabel("Summary vertex count as a fraction of total vertices")
plt.yticks(rotation=45)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))

sorted_matrices = sorted(list(result_statistic_matrices.items()), key=lambda m: np.mean(m[1]["summary_vertex_ratio"]))
summary_vertex_ordered_matrices = OrderedDict(sorted_matrices)

matrix_count = len(summary_vertex_ordered_matrices.keys())

style_ctr = 0
for plot_counter, k in enumerate(summary_vertex_ordered_matrices.keys()):
    if plot_counter < CATEGORY_BEST_PLOT_COUNT or plot_counter >= matrix_count  - CATEGORY_WORST_PLOT_COUNT:
        matrix = summary_vertex_ordered_matrices[k]
        color = matplotlib_config.colors[style_ctr % len(matplotlib_config.colors)]
        style = matplotlib_config.styles[(style_ctr - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
        style_ctr = style_ctr + 1
        plt.plot(matrix["execution_count"], matrix["summary_vertex_ratio"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.legend()


top_3_bottom3_vertex_savings = "-Savings-vertex-top-{}-bottom-{}".format(CATEGORY_BEST_PLOT_COUNT, CATEGORY_WORST_PLOT_COUNT)
save_figure(figure_path_name + top_3_bottom3_vertex_savings, fig_file_types)

plt.close(fig)


##### PLOT the five lowest summarized fraction of edges.
fig, ax = plt.subplots()
plt.ylabel("Summary edge count as a fraction of total edges")
plt.yticks(rotation=45)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))
sorted_matrices = sorted(list(result_statistic_matrices.items()), key=lambda m: np.mean(m[1]["summary_edge_ratio"]))
summary_edge_ordered_matrices = OrderedDict(sorted_matrices)

for plot_counter, k in enumerate(summary_edge_ordered_matrices.keys()):

    if plot_counter == PLOT_LIMIT:
        break

    matrix = summary_edge_ordered_matrices[k]
    color = matplotlib_config.colors[plot_counter % len(matplotlib_config.colors)]
    style = matplotlib_config.styles[(plot_counter - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
    plt.plot(matrix["execution_count"], matrix["summary_edge_ratio"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.legend()

top_5_edge_savings = "-Savings-edge-top-{}".format(PLOT_LIMIT)

save_figure(figure_path_name + top_5_edge_savings, fig_file_types)

plt.close(fig)

##### PLOT the three lowest and three highest summarized fraction of edges.
fig, ax = plt.subplots()
plt.ylabel("Summary edge count as a fraction of total edges")
plt.yticks(rotation=45)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))
sorted_matrices = sorted(list(result_statistic_matrices.items()), key=lambda m: np.mean(m[1]["summary_edge_ratio"]))
summary_edge_ordered_matrices = OrderedDict(sorted_matrices)
matrix_count = len(summary_edge_ordered_matrices.keys())

style_ctr = 0
for plot_counter, k in enumerate(summary_edge_ordered_matrices.keys()):

    if plot_counter < CATEGORY_BEST_PLOT_COUNT or plot_counter >= matrix_count  - CATEGORY_WORST_PLOT_COUNT:

        matrix = summary_edge_ordered_matrices[k]     
        color = matplotlib_config.colors[style_ctr % len(matplotlib_config.colors)]
        style = matplotlib_config.styles[(style_ctr - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
        style_ctr = style_ctr + 1
        plt.plot(matrix["execution_count"], matrix["summary_edge_ratio"], label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

x_step = int(args.chunk_count / 10)
new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
plt.xticks(new_xticks)
ax.set_xlim(left=1, right=args.chunk_count)
plt.legend()


top_3_bottom_3_edge_savings = "-Savings-edge-top-{}-bottom-{}".format(CATEGORY_BEST_PLOT_COUNT, CATEGORY_WORST_PLOT_COUNT)
save_figure(figure_path_name + top_3_bottom_3_edge_savings, fig_file_types)

#plt.gca().axes.get_xaxis().set_ticks([])
#save_figure(figure_path_name + top_3_bottom_3_edge_savings + '_nolabels', fig_file_types)

plt.close(fig)


if (not args.skip_rbo):

    ##### PLOT the five highest RBO results.
    fig, ax = plt.subplots() ######
    plt.ylabel("PageRank similarity measure (RBO)")
    plt.yticks(rotation=45)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))

    sorted_rbos = sorted(list(result_rbo_matrices.items()), reverse=True, key=lambda rbo_list: np.mean(rbo_list[1]))
    sorted_rbos_dict = OrderedDict(sorted_rbos)
    indexes = range(args.chunk_count + 1)

    min_y_tick = -1
    for plot_counter, k in enumerate(sorted_rbos_dict.keys()):

        if plot_counter == PLOT_LIMIT:
            break

        rbos = [100 * y for y in sorted_rbos_dict[k]]

        curr_min_rbo = min(rbos)
        if min_y_tick == -1 or curr_min_rbo < min_y_tick:
            min_y_tick = curr_min_rbo

        
        color = matplotlib_config.colors[plot_counter % len(matplotlib_config.colors)]
        style = matplotlib_config.styles[(plot_counter - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]

        # We are starting at index 1 to skip the first index which is a comparison between two complete PageRank executions
        plt.plot(indexes[1:], rbos[1:],   label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

    x_step = int(args.chunk_count / 10)
    new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
    plt.xticks(new_xticks)
    ax.set_xlim(left=1, right=args.chunk_count)
    y_divisor = 6
    y_step = (100 - min_y_tick) / y_divisor
    new_yticks = [min_y_tick + y_step * i for i in range(0, y_divisor + 1)]
    plt.yticks(new_yticks)
    plt.legend()

    top_5_rbo = "-RBO-top-{}".format(PLOT_LIMIT)
    save_figure(figure_path_name + top_5_rbo, fig_file_types)

    plt.close(fig)

    ##### PLOT the three highest and three lowest RBO results.
    fig, ax = plt.subplots()
    plt.ylabel("PageRank similarity measure (RBO)")
    plt.yticks(rotation=45)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(is_latex=False))
    sorted_rbos = sorted(list(result_rbo_matrices.items()), reverse=True, key=lambda rbo_list: np.mean(rbo_list[1]))
    sorted_rbos_dict = OrderedDict(sorted_rbos)
    indexes = range(args.chunk_count + 1)

    min_y_tick = -1

    matrix_count = len(sorted_rbos_dict.keys())

    style_ctr = 0
    for plot_counter, k in enumerate(sorted_rbos_dict.keys()):

        
        if plot_counter < CATEGORY_BEST_PLOT_COUNT or plot_counter >= matrix_count  - CATEGORY_WORST_PLOT_COUNT:

            #print('> Plotting counter #{}/{}'.format(plot_counter, matrix_count))

            rbos = [100 * y for y in sorted_rbos_dict[k]]

            curr_min_rbo = min(rbos)
            if min_y_tick == -1 or curr_min_rbo < min_y_tick:
                min_y_tick = curr_min_rbo

            
            color = matplotlib_config.colors[style_ctr % len(matplotlib_config.colors)]
            style = matplotlib_config.styles[(style_ctr - len(matplotlib_config.linestyles)) % len(matplotlib_config.styles)]
            style_ctr = style_ctr + 1

            # We are starting at index 1 to skip the first index which is a comparison between two complete PageRank executions
            plt.plot(indexes[1:], rbos[1:],   label = k, marker=style, color=color, alpha=matplotlib_config.PLOT_ALPHA)

    x_step = int(args.chunk_count / 10)
    new_xticks = list(range(x_step, args.chunk_count + x_step, x_step)) + [1]
    plt.xticks(new_xticks)
    ax.set_xlim(left=1, right=args.chunk_count)

    y_divisor = 6
    y_step = (100 - min_y_tick) / y_divisor
    new_yticks = [min_y_tick + y_step * i for i in range(0, y_divisor + 1)]
    plt.yticks(new_yticks)
    plt.legend()

    top_3_bottom_3_rbo = "-RBO-top-{}-bottom-{}".format(CATEGORY_BEST_PLOT_COUNT, CATEGORY_WORST_PLOT_COUNT)
    save_figure(figure_path_name + top_3_bottom_3_rbo, fig_file_types)

    plt.gca().axes.get_xaxis().set_ticks([])
    save_figure(figure_path_name + top_3_bottom_3_rbo + '_nolabels', fig_file_types)

    plt.close(fig) #####



###########################################################################
##################### Plot Flink job operator stats #######################
###########################################################################

if not args.skip_flink_job_stats:
    #TODO: parse jsons with id > 0, group them into arrays and them plot a heatmap or normal curve graphs
    pass

###########################################################################
########################### LaTeX IMPORT CODE #############################
###########################################################################

latex_fig_dir = '{KW_DATASET_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}{KW_DELETE_TOKEN}'.format(
            KW_DATASET_NAME = args.dataset_name, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_DELETE_TOKEN = DELETE_TOKEN)

stream_idx = stream_file_path.rfind('-stream')
stream_last_sep = stream_file_path.rfind('/')
stream_hyph_idx = stream_last_sep + 1 + stream_file_path[stream_last_sep + 1:stream_idx].rfind('-')
stream_size = stream_file_path[stream_hyph_idx + 1: stream_idx]
dataset_name = stream_file_path[stream_last_sep + 1: stream_hyph_idx]

# Create GraphBolt LaTeX image-including code (4 graphs in a single figure).
latex_code_path = figure_path_name + "-LaTeX-1-fig-4-graphs.tex"
with open(latex_code_path, 'w') as latex_file:

    tex_code_str = """\\begin{{figure*}}
    \\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_VERTEX_SAVINGS}.pgf}}}}}}
    \\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_EDGE_SAVINGS}.pgf}}}}}}\\\\
    \\vskip-0.70cm
    \\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_RBO}.pgf}}}}}}
    \\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_SPEEDUPS}.pgf}}}}}}
    \\caption{{\\texttt{{{KW_LATEX_FIG_LABEL}}} dataset statistics for randomized $|S| = {KW_STREAM_SIZE}$. Upper left and right are the summary graph's vertex and edge counts as a fraction of the complete graph's (best five savings). Bottom left and right are the best five RBOs and speedups respectively.}}
    \\label{{fig:{KW_LATEX_FIG_LABEL}}}
\\end{{figure*}}""".format(KW_FIGURE = latex_fig_dir, KW_TOP_RBO = top_5_rbo, KW_TOP_EDGE_SAVINGS = top_5_edge_savings, KW_TOP_VERTEX_SAVINGS = top_5_vertex_savings, KW_TOP_SPEEDUPS = top_5_speedups, KW_STREAM_SIZE = stream_size, KW_LATEX_FIG_LABEL = dataset_name)

    latex_file.write(tex_code_str)



# Create GraphBolt LaTeX image-including code (4 graphs, 1 figure for each).
latex_code_path = figure_path_name + "-LaTeX-4-fig-1-graph.tex"
with open(latex_code_path, 'w') as latex_file:

    tex_code_str = """\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_VERTEX_SAVINGS}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} lowest five average vertex ratios.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_VERTEX_SAVINGS}}}
\\end{{figure}}
\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_EDGE_SAVINGS}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} lowest five average edge ratios.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_EDGE_SAVINGS}}}
\\end{{figure}}
\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_RBO}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} top five average RBOs.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_RBO}}}
\\end{{figure}}
\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_SPEEDUPS}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} top five average speedups.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_SPEEDUPS}}}
\\end{{figure}}""".format(KW_FIGURE = latex_fig_dir, KW_TOP_RBO = top_5_rbo, KW_TOP_EDGE_SAVINGS = top_5_edge_savings, KW_TOP_VERTEX_SAVINGS = top_5_vertex_savings, KW_TOP_SPEEDUPS = top_5_speedups, KW_STREAM_SIZE = stream_size, KW_LATEX_FIG_LABEL = dataset_name, KW_CAPTION_NAME = dataset_name[:dataset_name.rfind('-')])

    latex_file.write(tex_code_str)


# Create GraphBolt LaTeX image-including code (4 graphs, 1 figure for each) for best-3 and worst-3 results.
latex_code_path = figure_path_name + "-LaTeX-4-fig-1-graph-best-3-worst-3.tex"
with open(latex_code_path, 'w') as latex_file:

    tex_code_str = """\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_VERTEX_SAVINGS}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} best three and worst three average vertex ratios.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_VERTEX_SAVINGS}}}
\\end{{figure}}
\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_EDGE_SAVINGS}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} best three and worst three average edge ratios.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_EDGE_SAVINGS}}}
\\end{{figure}}
\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_RBO}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} best three and worst three average RBOs.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_RBO}}}
\\end{{figure}}
\\begin{{figure}}
\\subfloat{{\\scalebox{{\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_SPEEDUPS}.pgf}}}}}}
\\caption{{\\texttt{{{KW_CAPTION_NAME}}} best three and worst three average speedups.}}
\\label{{fig:{KW_LATEX_FIG_LABEL}-{KW_TOP_SPEEDUPS}}}
\\end{{figure}}""".format(KW_FIGURE = latex_fig_dir, KW_TOP_RBO = top_3_bottom_3_rbo, KW_TOP_EDGE_SAVINGS = top_3_bottom_3_edge_savings, KW_TOP_VERTEX_SAVINGS = top_3_bottom3_vertex_savings, KW_TOP_SPEEDUPS = top_3_bottom_3_speedups, KW_STREAM_SIZE = stream_size, KW_LATEX_FIG_LABEL = dataset_name, KW_CAPTION_NAME = dataset_name[:dataset_name.rfind('-')])

    latex_file.write(tex_code_str)


# Create GraphBolt LaTeX image-including code (3 graphs in a column).
latex_code_path = figure_path_name + "-LaTeX-3-fig-1-graph-column.tex"
with open(latex_code_path, 'w') as latex_file:

    tex_code_str = """\\begin{{figure}}
    \\subfloat{{\\scalebox{{\\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_SPEEDUPS}_nolabels.pgf}}}}}}
    \\newline
    \\vskip-1.74cm
    \\subfloat{{\\scalebox{{\\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_RBO}_nolabels.pgf}}}}}}
    \\newline
    \\vskip-1.74cm
    \\subfloat{{\\scalebox{{\\figlen}}{{\\input{{img/{KW_FIGURE}/{KW_FIGURE}-{KW_TOP_EDGE_SAVINGS}.pgf}}}}}}
    \\caption{{\\texttt{{{KW_CAPTION_NAME}}}. Top-3/Bottom-3 speedups (upper). Top-3/Bottom-3 RBO results (middle). Top-3/Bottom-3 summary graph edge savings (bottom).}}
\\end{{figure}}""".format(KW_FIGURE = latex_fig_dir, KW_TOP_RBO = top_3_bottom_3_rbo, KW_TOP_EDGE_SAVINGS = top_3_bottom_3_edge_savings, KW_TOP_SPEEDUPS = top_3_bottom_3_speedups, KW_LATEX_FIG_LABEL = dataset_name, KW_CAPTION_NAME = dataset_name[:dataset_name.rfind('-')])

    latex_file.write(tex_code_str)