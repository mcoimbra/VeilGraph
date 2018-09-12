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

# This program tests differnt networkx PageRank algorithms with the 
# test datasets included in this project.
# Files are located in relative directory: ../src/test/resources

###########################################################################
################################# IMPORTS #################################
###########################################################################

import networkx as nx
from collections import OrderedDict
import os

# Info on these different networkx implementations.
# https://stackoverflow.com/questions/13040548/networkx-differences-between-pagerank-pagerank-numpy-and-pagerank-scipy



# Edit step_number 
step_number = 1

debugging = False

resource_dir = os.path.dirname(os.path.abspath(__file__)) + os.sep + os.sep.join(("..", "src", "test", "resources"))
test_path = os.path.dirname(os.path.abspath(__file__)) + os.sep + os.sep.join(("..", "src", "test", "resources", "step_{0}_graph.tsv".format(str(step_number))))

fh=open(test_path, 'rb')
G=nx.read_edgelist(fh, nodetype=int, delimiter='\t', comments='#')
fh.close()

python_power_pr = nx.pagerank(G,tol=1e-10)
scipy_sparse_matrix_power_pr = nx.pagerank_scipy(G,tol=1e-10)
numpy_eigenvector_pr = nx.pagerank_numpy(G)

if debugging:
    print("Resource directory:\t{0}".format(resource_dir))

    print("Python power-method PageRank")
    print(python_power_pr)

    print("SciPy sparse matrix power-method PageRank")
    print(scipy_sparse_matrix_power_pr)

    print("NumPy eigenvector PageRank")
    print(numpy_eigenvector_pr)

header_line = '#VERTEX_ID\tRANK\n'

with open(resource_dir + os.sep + 'step_{0}_python_powermethod_pr.tsv'.format(str(step_number)), 'w') as python_pr_file:
    descending = OrderedDict(sorted(python_power_pr.items(), reverse=True))
    python_pr_file.write(header_line)
    for id, rank in descending.items():
        python_pr_file.write('{0}\t{1}\n'.format(id, rank))

with open(resource_dir + os.sep + 'step_{0}_scipy_powermethod_pr.tsv'.format(str(step_number)), 'w') as scipy_sparse_pr_file:
    descending = OrderedDict(sorted(scipy_sparse_matrix_power_pr.items(), reverse=True))
    scipy_sparse_pr_file.write(header_line)
    for id, rank in descending.items():
        scipy_sparse_pr_file.write('{0}\t{1}\n'.format(id, rank))

with open(resource_dir + os.sep + 'step_{0}_numpy_algebra_pr.tsv'.format(str(step_number)), 'w') as numpy_eigenvector_pr_file:
    descending = OrderedDict(sorted(numpy_eigenvector_pr.items(), reverse=True))
    numpy_eigenvector_pr_file.write(header_line)
    for id, rank in descending.items():
        numpy_eigenvector_pr_file.write('{0}\t{1}\n'.format(id, rank))