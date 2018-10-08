#!/usr/bin/env python

import argparse
import os
import sys

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################

#TODO: write description text.
#TODO: change arguments to be equal to sample_edges.py

# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
DESCRIPTION_TEXT = "GraphBolt TODO"
parser = argparse.ArgumentParser(description=DESCRIPTION_TEXT, formatter_class=argparse.RawTextHelpFormatter)
#parser.add_argument("-plot-single-figures", help="skip parameter-specific figure generation. Only plot groups of combinations of parameters", required=False, action="store_true") # if ommited default value is false
#parser.add_argument("-png", help="output .png files too", required=False, action="store_true") # if ommited default value is false
#parser.add_argument("-pdf", help="output .pdf files too", required=False, action="store_true") # if ommited default value is false

#parser.add_argument("-dataset-name", help="dataset name.", required=True, type=str, default="")
parser.add_argument("-data-dir", help="dataset directory name.", required=True, type=str, default="")
parser.add_argument('-deletion_chunk', help='size of each deletion chunk.', required=True, type=int)
parser.add_argument('-stream_chunk', help='size of each stream chunk.', required=True, type=int)

args = parser.parse_args()

if args.data_dir.startswith('~'):
    args.data_dir = os.path.expanduser(args.data_dir).replace('\\', '/')


for file in os.listdir(args.data_dir):
    if file.endswith("-start.tsv"):
        graph_path = os.path.join(args.data_dir, file)
    elif file.endswith("-stream.tsv"):
        stream_path = os.path.join(args.data_dir, file)
    elif file.endswith("-deletions.tsv"):
        deletion_path = os.path.join(args.data_dir, file)

print("> Start path:\t\t{}".format(graph_path))
print("> Stream path:\t\t{}".format(stream_path))
print("> Deletions path:\t{}".format(deletion_path))


with open(graph_path, 'r') as graph_fd, open(stream_path, 'r') as stream_fd, open(deletion_path, 'r') as deletion_fd:
    base_edges = graph_fd.readlines()
    stream_edges = stream_fd.readlines()
    deletion_edges = deletion_fd.readlines()

base_edge_dic = {}
edge_ctr = 0
v_set = set()
for e in base_edges:
    edge_ctr = edge_ctr + 1
    s, t = e.strip().split('\t')
    if not s in base_edge_dic:
        base_edge_dic[s] = [t]
    else:
        base_edge_dic[s].append(t)

    v_set.add(s)
    v_set.add(t)

print("> #Vertices:\t\t{}".format(len(v_set)))
print("> #Edges:\t\t{}".format(edge_ctr))
print("> #Sources:\t\t{}".format(len(base_edge_dic)))

stream_chunk_count = int(len(stream_edges) / args.stream_chunk)
prev_i = 0
prev_del_i = 0
for i in range(0, stream_chunk_count):

    print("> Execution #{}".format(i))

    addition_left_bound = i * args.stream_chunk
    addition_right_bound = addition_left_bound + args.stream_chunk

    print("> Adding chunk:{}-{}".format(addition_left_bound, addition_right_bound))
    additions = stream_edges[addition_left_bound:addition_right_bound]

    #base_edges = base_edges + additions

    for e in additions:
        
        s, t = e.strip().split('\t')
        if not s in base_edge_dic:
            base_edge_dic[s] = [t]
        else:
            base_edge_dic[s].append(t)

        v_set.add(s)
        v_set.add(t)

    print("> +:\t{}".format(len(additions)))


    deletion_left_bound = i * args.deletion_chunk
    deletion_right_bound = deletion_left_bound + args.deletion_chunk

    deletions = deletion_edges[deletion_left_bound:deletion_right_bound]

    print("> -:\t{}".format(len(deletions)))

    

    for e in deletions:
        s, t = e.strip().split('\t')
        #print("\n\n> s: {}\n{}\nDEL t: {}".format(s, base_edge_dic[s], t))
        if not t in base_edge_dic[s]:
            print("> Deletion of inexistant edge:\t{} {}".format(s, t))
            sys.exit(1)

        d_i = base_edge_dic[s].index(t)
        del base_edge_dic[s][d_i]

        if len(base_edge_dic[s]) == 0:
            del base_edge_dic[s]

    

    e_ctr = 0
    v_ctr = 0
    current_set = set()
    for s, targets in base_edge_dic.items():
        v_ctr = v_ctr + 1
        e_ctr = e_ctr + len(targets)

        current_set.add(s)
        for t in targets:
            current_set.add(t)

    print("> |V|: {}\t|E|: {}".format(len(current_set), e_ctr))

    prev_i = i * args.stream_chunk
    