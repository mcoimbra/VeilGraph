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



data_path = sys.argv[1]

out_dir = os.path.dirname(data_path)
out_file = data_path[data_path.rfind(os.path.sep)+1:].replace('.tsv', '_stats.tsv')
out_path = os.path.join(out_dir, out_file)

complete_out_file = data_path[data_path.rfind(os.path.sep)+1:].replace('.tsv', '_complete_stats.tsv')
complete_out_path = os.path.join(out_dir, complete_out_file)

summarized_out_file = data_path[data_path.rfind(os.path.sep)+1:].replace('.tsv', '_summarized_stats.tsv')
summarized_out_path = os.path.join(out_dir, summarized_out_file)

comparative_out_file = data_path[data_path.rfind(os.path.sep)+1:].replace('.tsv', '_comparative_stats.tsv')
comparative_out_path = os.path.join(out_dir, comparative_out_file)

times_out_file = data_path[data_path.rfind(os.path.sep)+1:].replace('.tsv', '_time_stats.tsv')
times_out_path = os.path.join(out_dir, times_out_file)

df = pd.read_csv(data_path, sep="\t")   # read dummy .tsv file into memory

#print(df['P1_complete_total_update_time'].mean())

p1_complete_graph_update_time_mean = df['P1_complete_graph_update_time'].mean()
p1_complete_total_update_time_mean = df['P1_complete_total_update_time'].mean()
p1_complete_computation_time_mean = df['P1_complete_computation_time'].mean()


with open(out_path, 'w') as out, open(complete_out_path, 'w') as complete_out, open(summarized_out_path, 'w') as summarized_out, open(comparative_out_path, 'w') as comparative_out, open(times_out_path, 'w') as times_out:

    val_order = ["P",
        "complete_graph_update_time_ratio_avg",
        "complete_graph_update_time_ratio_std",
        'complete_graph_update_time_avg',
        'complete_graph_update_time_std',
        "complete_total_update_time_ratio_avg",
        "complete_total_update_time_ratio_std",
        'complete_total_update_time_avg',
        'complete_total_update_time_std',
        "complete_computation_time_ratio_avg",
        "complete_computation_time_ratio_std",
        "complete_computation_time_avg",
        "complete_computation_time_std",
        "summarized_graph_update_time_ratio_avg",
        "summarized_graph_update_time_ratio_std",
        "summarized_graph_update_time_avg",
        "summarized_graph_update_time_std",
        "summarized_total_update_time_ratio_avg",
        "summarized_total_update_time_ratio_std",
        "summarized_total_update_time_avg",
        "summarized_total_update_time_std",
        "summarized_computation_time_ratio_avg",
        "summarized_computation_time_ratio_std",
        "summarized_computation_time_avg",
        "summarized_computation_time_std",
        "comparative_graph_update_time_ratio_avg",
        "comparative_graph_update_time_ratio_std",
        "comparative_total_update_time_ratio_avg",
        "comparative_total_update_time_ratio_std",
        "comparative_computation_time_ratio_avg",
        "comparative_computation_time_ratio_std"]

    complete_order = ["P"]
    summarized_order = ["P"]
    comparative_order = ["P"]
    times_order = ["P"]

    val_dict = {}
    
    for val_name in val_order:
        val_dict[val_name] = []

        if val_name.startswith("complete"):
            complete_order.append(val_name)

        if val_name.startswith("summarized"):
            summarized_order.append(val_name)

        if val_name.startswith("comparative"):
            comparative_order.append(val_name)

        if (not 'ratio' in val_name) and (not val_name.startswith('P')):
            times_order.append(val_name)


    out.write("{}\n".format('\t'.join(val_order)))
    complete_out.write("{}\n".format('\t'.join(complete_order)))
    summarized_out.write("{}\n".format('\t'.join(summarized_order)))
    comparative_out.write("{}\n".format('\t'.join(comparative_order)))
    times_out.write("{}\n".format('\t'.join(times_order)))

    p_list = [2, 4, 8, 16]

    for parallelism in p_list:

        p = str(parallelism)
        val_dict['P'].append(p)

        # Complete version's parallelism scalability and time increases.
        complete_graph_update_time_ratio_df = df['P1_complete_graph_update_time'] / df['P' + p + '_complete_graph_update_time']
        val_dict['complete_graph_update_time_ratio_avg'].append(complete_graph_update_time_ratio_df.mean())
        val_dict['complete_graph_update_time_ratio_std'].append(complete_graph_update_time_ratio_df.std())

        val_dict['complete_graph_update_time_avg'].append(df['P' + p + '_complete_graph_update_time'].mean())
        val_dict['complete_graph_update_time_std'].append(df['P' + p + '_complete_graph_update_time'].std())

        complete_total_update_time_ratio_df = df['P1_complete_total_update_time'] / df['P' + p + '_complete_total_update_time']
        val_dict['complete_total_update_time_ratio_avg'].append(complete_total_update_time_ratio_df.mean())
        val_dict['complete_total_update_time_ratio_std'].append(complete_total_update_time_ratio_df.std())

        val_dict['complete_total_update_time_avg'].append(df['P' + p + '_complete_total_update_time'].mean())
        val_dict['complete_total_update_time_std'].append(df['P' + p + '_complete_total_update_time'].std())

        complete_computation_time_ratio_df = df['P1_complete_computation_time'] / df['P' + p + '_complete_computation_time']
        val_dict['complete_computation_time_ratio_avg'].append(complete_computation_time_ratio_df.mean())
        val_dict['complete_computation_time_ratio_std'].append(complete_computation_time_ratio_df.std())

        val_dict['complete_computation_time_avg'].append(df['P' + p + '_complete_computation_time'].mean())
        val_dict['complete_computation_time_std'].append(df['P' + p + '_complete_computation_time'].std())

        # Summarized version's parallelism scalability and time increases.
        summarized_graph_update_time_ratio_df = df['P1_summarized_graph_update_time'] / df['P' + p + '_summarized_graph_update_time']
        val_dict['summarized_graph_update_time_ratio_avg'].append(summarized_graph_update_time_ratio_df.mean())
        val_dict['summarized_graph_update_time_ratio_std'].append(summarized_graph_update_time_ratio_df.std())

        val_dict['summarized_graph_update_time_avg'].append(df['P' + p + '_summarized_graph_update_time'].mean())
        val_dict['summarized_graph_update_time_std'].append(df['P' + p + '_summarized_graph_update_time'].std())

        summarized_total_update_time_ratio_df = df['P1_summarized_total_update_time'] / df['P' + p + '_summarized_total_update_time']
        val_dict['summarized_total_update_time_ratio_avg'].append(summarized_total_update_time_ratio_df.mean())
        val_dict['summarized_total_update_time_ratio_std'].append(summarized_total_update_time_ratio_df.std())

        val_dict['summarized_total_update_time_avg'].append(df['P' + p + '_summarized_total_update_time'].mean())
        val_dict['summarized_total_update_time_std'].append(df['P' + p + '_summarized_total_update_time'].std())

        summarized_computation_time_ratio_df = df['P1_summarized_computation_time'] / df['P' + p + '_summarized_computation_time']
        val_dict['summarized_computation_time_ratio_avg'].append(summarized_computation_time_ratio_df.mean())
        val_dict['summarized_computation_time_ratio_std'].append(summarized_computation_time_ratio_df.std())

        val_dict['summarized_computation_time_avg'].append(df['P' + p + '_summarized_computation_time'].mean())
        val_dict['summarized_computation_time_std'].append(df['P' + p + '_summarized_computation_time'].std())

        # Summarized vs complete version comparative speedup and time increases.
        comparative_graph_update_time_ratio_df = df['P' + p + '_complete_graph_update_time'] / df['P' + p + '_summarized_graph_update_time']
        val_dict['comparative_graph_update_time_ratio_avg'].append(comparative_graph_update_time_ratio_df.mean())
        val_dict['comparative_graph_update_time_ratio_std'].append(comparative_graph_update_time_ratio_df.std())

        comparative_total_update_time_ratio_df = df['P' + p + '_complete_total_update_time'] / df['P' + p + '_summarized_total_update_time']
        val_dict['comparative_total_update_time_ratio_avg'].append(comparative_total_update_time_ratio_df.mean())
        val_dict['comparative_total_update_time_ratio_std'].append(comparative_total_update_time_ratio_df.std())

        comparative_computation_time_ratio_df = df['P' + p + '_complete_computation_time'] / df['P' + p + '_summarized_computation_time']
        val_dict['comparative_computation_time_ratio_avg'].append(comparative_computation_time_ratio_df.mean())
        val_dict['comparative_computation_time_ratio_std'].append(comparative_computation_time_ratio_df.std())


    
    
    
    for parallelism_index in range(0, len(p_list)):
        curr_str = ""
        complete_str = ""
        summarized_str = ""
        comparative_str = ""
        times_str = ""

        for val in val_order:
            insertion_value = str(val_dict[val][parallelism_index])
            if len(curr_str) == 0:
                curr_str = insertion_value
            else:
                curr_str = curr_str + "\t" + insertion_value

            if val.startswith("complete") or val == 'P':
                if len(complete_str) == 0:
                    complete_str = insertion_value
                else:
                    complete_str = complete_str + "\t" + insertion_value

            if val.startswith("summarized") or val == 'P':
                if len(summarized_str) == 0:
                    summarized_str = insertion_value
                else:
                    summarized_str = summarized_str + "\t" + insertion_value

            if val.startswith("comparative") or val == 'P':
                if len(comparative_str) == 0:
                    comparative_str = insertion_value
                else:
                    comparative_str = comparative_str + "\t" + insertion_value

            if not 'ratio' in val:
                if len(times_str) == 0:
                    times_str = insertion_value
                else:
                    times_str = times_str + "\t" + insertion_value
        
        out.write("{}\n".format(curr_str))
        complete_out.write("{}\n".format(complete_str))
        summarized_out.write("{}\n".format(summarized_str))
        comparative_out.write("{}\n".format(comparative_str))
        times_out.write("{}\n".format(times_str))

    #a = df.values  # access the numpy array containing values
    #print(a)

    print(val_dict)