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

# Used to split a .tsv file with specific percentages of lines of the 
# initial file to use as part of a stream.

###########################################################################
################################# IMPORTS #################################
###########################################################################

from operator import methodcaller
import os
import random
import sys


dataset_path = sys.argv[1]
with open(dataset_path, 'r') as dataset:
	# Prepare file base graph and update stream file names.
	dataset_name = dataset_path[dataset_path.rfind(os.sep)+1:dataset_path.rfind('.tsv')]
	graph_percentages = sorted([0.5, 0.6, 0.7, 0.8])	
	graph_names = ['{0}-init-{1}.tsv'.format(dataset_name, str(int(p * 100))) for p in graph_percentages]
	stream_names = ['{0}-cont-{1}.tsv'.format(dataset_name, str(int(p * 100))) for p in graph_percentages]

	# Open all output files.
	graph_files = [open(file_name, 'w') for file_name in graph_names]
	stream_files = [open(file_name, 'w') for file_name in stream_names]
	
	# Process the edges.
	edges = dataset.readlines()
	edge_count = len(edges)
	initial_graph_line_counts = [int(edge_count * p) for p in graph_percentages]
	print('Generating initial graph files:\n{0}'.format('\n'.join(map(str, initial_graph_line_counts))))

	# Get the biggest stream of edge updates and shuffle it.
	largest_stream = edges[initial_graph_line_counts[0]:]
	random.shuffle(largest_stream)

	# Reminder: Python lists are 0-indexed.
	shuffled = edges[0:initial_graph_line_counts[0]] + largest_stream
	shuffled = list(map(methodcaller('strip'), shuffled))

	# Reminder: Python arrays are 0-indexed and the right-side of the ':' operator is an exclusive array limit.
	indices = range(0, len(graph_percentages))
	[graph_files[i].write('#SOURCE_ID\TARGET_ID\n{0}'.format('\n'.join(shuffled[0:initial_graph_line_counts[i]]))) for i in indices]
	[stream_files[i].write('{0}'.format('\n'.join(shuffled[initial_graph_line_counts[i]:]))) for i in indices]

	# Close all output files.
	[f.close() for f in graph_files]
	[f.close() for f in stream_files]