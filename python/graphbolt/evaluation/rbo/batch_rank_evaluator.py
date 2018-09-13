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

# Used to compute the accuracy between complete and summarized 
# executions of GraphBolt's PageRank.

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
import io
import glob
import os
import re
import sys

# 2. related third party imports
# 3. custom local imports
from .rank_evaluator import evaluate

###########################################################################
############################### FUNCTIONS #################################
###########################################################################

def compute_rbo(summarized_pagerank_directory, complete_pagerank_directory,  out: io.TextIOWrapper = sys.stdout, debugging: bool = False, p_value: float = 0.99) -> None:
    pattern = re.compile("\\d{4}")
    file_names = glob.glob(summarized_pagerank_directory + '/summary_*')

    # Sort paths by lexicographical order of the PageRank result files.
    file_names.sort(key = lambda x: x.split(os.path.sep)[-1])

    for name in file_names:
        name2 = name.replace('summary', 'complete', 1).replace(summarized_pagerank_directory, complete_pagerank_directory, 1).replace(os.path.sep, '/')
        #TODO: if name or name2 is not found, exit with -1
        RBO_index = pattern.findall(name)[-1]

        print(RBO_index, ";", evaluate(name, name2, p_value), sep='', file=out)


if __name__ == "__main__":

    ###########################################################################
    ############################# READ ARGUMENTS ##############################
    ###########################################################################

    summarized_pagerank_directory = sys.argv[1]
    if not os.path.exists(summarized_pagerank_directory):
        print("> 'summarized_pagerank_directory' directory not found:\t{}".format(summarized_pagerank_directory))
        print("> Exiting.")
        sys.exit(1)
    if not os.path.isdir(summarized_pagerank_directory):
        print("> 'summarized_pagerank_directory' not a valid directory:\t{}".format(summarized_pagerank_directory))
        print("> Exiting.")
        sys.exit(1)

    complete_pagerank_directory = sys.argv[2]
    if not os.path.exists(complete_pagerank_directory):
        print("> 'complete_pagerank_directory' directory not found:\t{}".format(complete_pagerank_directory))
        print("> Exiting.")
        sys.exit(1)
    if not os.path.isdir(complete_pagerank_directory):
        print("> 'complete_pagerank_directory' not a valid directory:\t{}".format(complete_pagerank_directory))
        print("> Exiting.")
        sys.exit(1)

    p_value = float(sys.argv[3])


    ###########################################################################
    ################################## RBO ####################################
    ###########################################################################

    # This (RBO) is based on: 
    # 'A Similarity Measure for Indefinite Rankings'
    # WILLIAM WEBBER, ALISTAIR MOFFAT, and JUSTIN ZOBEL
    # The University of Melbourne, Australia

    # If this program is called with
    # python python/batch-rank-evaluator.py /home/mcoimbra/Results/PR/CitHepPh-0.20-2 /home/mcoimbra/Results/PR/CitHepPh-exact 0.99
    # then:
    # summarized_pagerank_directory = /home/mcoimbra/Results/PR/CitHepPh-0.20-2
    # complete_pagerank_directory = /home/mcoimbra/Results/PR/CitHepPh-exact
    # Following the example, we would also have
    # file_names = /home/mcoimbra/Results/PR/CitHepPh-0.20-2/exact-repeat_*

    compute_rbo(summarized_pagerank_directory, complete_pagerank_directory)