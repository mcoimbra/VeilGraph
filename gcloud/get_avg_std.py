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

import os
import sys
import pandas as pd
import numpy as np

# TODO
# 0- this script should be called from decompress.sh
# 1- get path to *_data.tsv file in an execution_number dir.
# 2- ingest columns
#### (order is 4x complete and then 4x summarized), every 8 columns the parallelism level changes.
#### P=1,2,4,8,16
# 3- for each parallelism level P > 1:
# 3.1- calculate mean and std speedup (each summarized P level versus summarized P=1 level) and store in .tsv file
# 3.2- calculate mean and std speedup (each summarized P level versus summarized P=1 level) and store in .tsv file

# make a dummy .tsv file, save it to disk

data_path = sys.argv[1]

out_dir = os.path.dirname(data_path)
out_file = data_path[data_path.rfind(os.path.sep)+1:].replace('.tsv', '_stats.tsv')
out_path = os.path.join(out_dir, out_file)



df = pd.read_csv(data_path, sep="\t")   # read dummy .tsv file into memory

#print(df['P1_complete_total_update_time'].mean())

p1_complete_graph_update_time_mean = df['P1_complete_graph_update_time'].mean()
p1_complete_total_update_time_mean = df['P1_complete_total_update_time'].mean()
p1_complete_computation_time_mean = df['P1_complete_computation_time'].mean()


with open(out_path, 'w') as out:

    val_order = ["P",
        "complete_graph_update_time_ratio_avg",
        "complete_graph_update_time_ratio_std",
        "complete_total_update_time_ratio_avg",
        "complete_total_update_time_ratio_std",
        "summarized_graph_update_time_ratio_avg",
        "summarized_graph_update_time_ratio_std",
        "summarized_total_update_time_ratio_avg",
        "summarized_total_update_time_ratio_std",
        "comparative_graph_update_time_ratio_avg",
        "comparative_graph_update_time_ratio_std",
        "comparative_total_update_time_ratio_avg",
        "comparative_total_update_time_ratio_std"]

    val_dict = {}
    for val_name in val_order:
        val_dict[val_name] = []

    out.write("{}".format('\t'.join(val_order)))

    for parallelism in [2, 4, 8, 16]:

        p = str(parallelism)
        val_dict['P'].append(p)

        # Complete version's parallelism scalability and time increases.
        complete_graph_update_time_ratio_df = df['P1_complete_graph_update_time'] / df['P' + p + '_complete_graph_update_time']
        val_dict['complete_graph_update_time_ratio_avg'].append(complete_graph_update_time_ratio_df.mean())
        val_dict['complete_graph_update_time_ratio_std'].append(complete_graph_update_time_ratio_df.std())

        complete_total_update_time_ratio_df = df['P1_complete_total_update_time'] / df['P' + p + '_complete_total_update_time']
        val_dict['complete_total_update_time_ratio_avg'].append(complete_total_update_time_ratio_df.mean())
        val_dict['complete_total_update_time_ratio_std'].append(complete_total_update_time_ratio_df.std())

        # Summarized version's parallelism scalability and time increases.
        summarized_graph_update_time_ratio_df = df['P1_summarized_graph_update_time'] / df['P' + p + '_summarized_graph_update_time']
        val_dict['summarized_graph_update_time_ratio_avg'].append(summarized_graph_update_time_ratio_df.mean())
        val_dict['summarized_graph_update_time_ratio_std'].append(summarized_graph_update_time_ratio_df.std())

        summarized_total_update_time_ratio_df = df['P1_summarized_total_update_time'] / df['P' + p + '_summarized_total_update_time']
        val_dict['summarized_total_update_time_ratio_avg'].append(summarized_total_update_time_ratio_df.mean())
        val_dict['summarized_total_update_time_ratio_std'].append(summarized_total_update_time_ratio_df.std())

        # Summarized vs complete version comparative speedup and time increases.
        comparative_graph_update_time_ratio_df = df['P' + p + '_complete_graph_update_time'] / df['P' + p + '_summarized_graph_update_time']
        val_dict['comparative_graph_update_time_ratio_avg'].append(comparative_graph_update_time_ratio_df.mean())
        val_dict['comparative_graph_update_time_ratio_std'].append(comparative_graph_update_time_ratio_df.std())

        comparative_total_update_time_ratio_df = df['P' + p + '_complete_total_update_time'] / df['P' + p + '_summarized_total_update_time']
        val_dict['comparative_total_update_time_ratio_avg'].append(comparative_total_update_time_ratio_df.mean())
        val_dict['comparative_total_update_time_ratio_std'].append(comparative_total_update_time_ratio_df.std())

        

    #a = df.values  # access the numpy array containing values
    #print(a)

    print(val_dict)