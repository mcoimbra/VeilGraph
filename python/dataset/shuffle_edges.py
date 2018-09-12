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

# Shuffle a given file's lines.
# Used to shuffle edges in .tsv files.

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
import os
import random
import sys

# 2. related third party imports
# 3. custom local imports

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################

if len(sys.argv) != 2:
	print("> Expected a single argument which is a path to a .tsv edge file. Exiting")
	sys.exit(1)

dataset_path = sys.argv[1]
if os.path.isdir(dataset_path) or not (os.path.exists(dataset_path)):
    print("> First argument must be an existing file.\nProvided: {}\nExiting.".format(dataset_path))
    sys.exit(1)

# Name of output file.
out_path = "shuffled.tsv"

with open(dataset_path, 'r') as dataset, open(out_path, 'w') as out_file:
	edges = dataset.readlines()
	random.shuffle(edges)
	out_file.write('{0}'.format(''.join(edges)))