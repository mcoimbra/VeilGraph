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
from pathlib import Path

eval_dir = sys.argv[1]

iterations = 30
base_threshold = 0.0
top_threshold = 0.25
step_threshold = 0.05
base_neigh = 0
top_neigh = 3
output_size = 1000

p = Path(eval_dir)
lines_dict = dict()
evals_dict = dict()
evals_header = list()

for file in p.glob("*.csv"):
    try:
        with file.open() as e_file:
            evals_header.append(file.name)
            for l in e_file:
                (n, ev) = l.split(";")
                it = int(n)
                evals_dict.setdefault(it, list()).append(ev.rstrip().replace(".", ","))
    except FileNotFoundError:
        print("File not found:", eval_dir + file.name, file=sys.stderr)
        continue

print(";".join(evals_header))
for it in sorted(evals_dict):
    print(";".join(evals_dict[it]))
