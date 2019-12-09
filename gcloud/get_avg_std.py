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
# 3.1- calculate average and std speedup (each summarized P level versus summarized P=1 level) and store in .tsv file
# 3.2- calculate average and std speedup (each summarized P level versus summarized P=1 level) and store in .tsv file

# make a dummy .tsv file, save it to disk

data_path = sys.argv[1]

df = pd.read_csv(data_path, sep="\t")   # read dummy .tsv file into memory

#print(df['P1_complete_total_update_time'].mean())

p1_complete_graph_update_time_mean = df['P1_complete_graph_update_time'].mean()
p1_complete_total_update_time_mean = df['P1_complete_total_update_time'].mean()
p1_complete_computation_time_mean = df['P1_complete_computation_time'].mean()

for p in [2, 4, 8, 16]:

    # Complete version's parallelism scalability and time increases.
    complete_graph_update_time_ratio_df = df['P1_complete_graph_update_time'] / df['P' + p + '_complete_graph_update_time']
    complete_total_update_time_ratio_df = df['P1_complete_total_update_time'] / df['P' + p + '_complete_total_update_time']

    # Summarized version's parallelism scalability and time increases.
    summarized_graph_update_time_ratio_df = df['P1_summarized_graph_update_time'] / df['P' + p + '_summarized_graph_update_time']
    summarized_graph_update_time_ratio_df = df['P1_summarized_total_update_time'] / df['P' + p + '_summarized_total_update_time']

    # Summarized vs complete version comparative speedup and time increases.
    comparative_graph_update_time_ratio_df = df['P1_complete_graph_update_time'] / df['P' + p + '_summarized_graph_update_time']
    comparative_graph_update_time_ratio_df = df['P1_complete_total_update_time'] / df['P' + p + '_summarized_total_update_time']

    pass

#a = df.values  # access the numpy array containing values
#print(a)