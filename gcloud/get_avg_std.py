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

import random

# TODO
# 0- this script should be called from decompress.sh
# 1- get path to *_data.tsv file in an execution_number dir.
# 2- ingest columns
#### (order is 4x complete and then 4x summarized), every 8 columns the parallelism level changes.
#### P=1,2,4,8,16
# 3- for each parallelism level P > 1:
# 3.1- calculate average and std speedup (each summarized P level versus summarized P=1 level) and store in .tsv file
# 3.2- calculate average and std speedup (each summarized P level versus summarized P=1 level) and store in .tsv file