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

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
import argparse
import os
import sys

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################

# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
DESCRIPTION_TEXT = "GraphBolt deletion stream duplicate check."
parser = argparse.ArgumentParser(description=DESCRIPTION_TEXT, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("-data-dir", help="dataset directory name.", required=True, type=str, default="")

args = parser.parse_args()

if len(args.data_dir) <= 0:
	print("> Invalid dataset directory.\nProvided:\t{}\nExiting.".format(args.data_dir))
	sys.exit(1)
if args.data_dir.startswith('~'):
        args.data_dir = os.path.expanduser(args.data_dir).replace('\\', '/')
if not (os.path.exists(args.data_dir) and os.path.isdir(args.data_dir)):
    print("> 'data_dir' must be an existing directory.\nProvided:\t{}\nExiting.".format(args.data_dir))
    sys.exit(1)

dataset_name = args.data_dir[args.data_dir.rfind('/') + 1:]

stream_edges = []
stream_file = "{}-stream.tsv".format(os.path.join(args.data_dir, dataset_name))
with open(stream_file, 'r') as stream:
	lines = stream.readlines()
	for l in lines:
		stream_edges.append(l.strip())

with open("{}-start.tsv".format(os.path.join(args.data_dir, dataset_name)), 'r') as start:
	for _, l in enumerate(start):
		if l.strip() in stream_edges:
			print("Duplicate: " + l.strip())

deletion_edges = []
deleted_map = {}
with open("{}-deletions.tsv".format(os.path.join(args.data_dir, dataset_name)), 'r') as deletions:
	lines = deletions.readlines()
	for l in lines:
		stipped_line = l.strip()
		deletion_edges.append(stipped_line)
		if not stipped_line in deleted_map:
			deleted_map[stipped_line] = 1
		else: 
			deleted_map[stipped_line] = deleted_map[stipped_line] + 1


repetitions = [edge for edge, ctr in deleted_map.items() if ctr > 1]
for e in repetitions:
	print("{}\t{}".format(e, str(deleted_map[e])))

print("> Deletion edge count:\t{}".format(len(deletion_edges)))
print("> Unique deletion edges:\t{}".format(len(set(deletion_edges))))