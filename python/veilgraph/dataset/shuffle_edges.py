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
import argparse
import os
import random
import sys

# 2. related third party imports
# 3. custom local imports
from VEILGRAPH import localutil

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################

# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
DESCRIPTION_TEXT = "VeilGraph edge shuffler. Take a stream of edge updates, shuffle them and save to disk."
parser = argparse.ArgumentParser(description=DESCRIPTION_TEXT, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-i', '--input-file', help="dataset name.", required=True, type=str)
parser.add_argument('-o', '--out-dir', help="output directory.", required=False, type=str)
parser.add_argument('-d', '--debug', help='debug mode outputs helpful information.', required=False, action="store_true")

args = parser.parse_args()

# Sanitize arguments.
out_dir = None
if (not args.out_dir is None):
	if len(args.out_dir) == 0:
		print("> -out-dir must be a non-empty string. Exiting")
		sys.exit(1)
	elif not (os.path.exists(args.out_dir) and os.path.isdir(args.out_dir)):
		print("> Provided output directory does not exist: {}. Exiting".format(args.out_dir))
		sys.exit(1)
	out_dir = args.out_dir

if len(args.input_file) == 0:
	print("> -input-file must be a non-empty string. Exiting")
	sys.exit(1)
elif not os.path.exists(args.input_file):
	print("> Provided input file does not exist: {}. Exiting".format(args.input_file))
	sys.exit(1)
elif os.path.isdir(args.input_file):
	print("> Provided input file was a directory but should be a file: {}. Exiting".format(args.input_file))
	sys.exit(1)

path_sep_index = args.input_file.rfind(os.path.sep)

if out_dir is None:
	out_dir =  args.input_file[0:path_sep_index]

input_name = localutil.rreplace(args.input_file, '-stream', '-shuffled-stream', 1)
out_path = os.path.join(out_dir, input_name)

print('> Output file:\t{}'.format(out_path))

with open(args.input_file, 'r') as dataset, open(out_path, 'w') as out_file:
	edges = dataset.readlines()

	if not edges[len(edges) - 1].endswith('\n'):
		edges[len(edges) - 1] = edges[len(edges) - 1] + '\n'

	if args.debug:
		print('> Lines before shuffle:\t{}'.format(len(edges)))
	if args.debug:
		print(' > Last line:\t' + edges[len(edges) - 1])

	random.shuffle(edges)

	edges[len(edges) - 1] = edges[len(edges) - 1].strip()

	if args.debug:
		print('> Lines after shuffle:\t{}'.format(len(edges)))
	out_file.write('{0}'.format(''.join(edges)))