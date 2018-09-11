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
import sys

# 2. related third party imports
# 3. custom local imports
import RBO

def evaluate(file1: str, file2: str, p_value: float = 0.98) -> float:
    try:
        with open(file1) as f1:
            try:
                with open(file2) as f2:
                    list_to_evaluate = f1.readlines()
                    gold_list = f2.readlines()

                    # In case the ranking files have semi-colon-separated values, assume the first value is the node id. Discard the rest for each line.
                    list_to_evaluate_sans_rank = []
                    for l in list_to_evaluate:
                        node_id = l[0:l.rfind(";")]
                        list_to_evaluate_sans_rank.append(node_id)

                    gold_list_sans_rank = []
                    for l in gold_list:
                        node_id = l[0:l.rfind(";")]
                        gold_list_sans_rank.append(node_id)

                    return RBO.score(list_to_evaluate_sans_rank, gold_list_sans_rank, p=p_value)

            except FileNotFoundError:
                print("File not found:", file2, file=sys.stderr)

    except FileNotFoundError:
        print("File not found:", file1, file=sys.stderr)


if __name__ == "__main__":
    name = sys.argv[1]
    name2 = sys.argv[2]
    p = float(sys.argv[3])
    print(evaluate(name, name2, p))
