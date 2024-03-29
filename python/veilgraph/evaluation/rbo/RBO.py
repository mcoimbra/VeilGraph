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

# This is a modified implementation of  https://github.com/maslinych/linis-scripts/blob/master/rbo_calc.py
# It is a linear implementation of the RBO and assumes there are no duplicates and doesn't handle for ties. 

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
from typing import List

# 2. related third party imports
# 3. custom local imports


###########################################################################
############################### FUNCTIONS #################################
###########################################################################

def score(l1: List, l2: List, p: float = 0.98, debug: bool = False) -> float:
    """
        Calculates Ranked Biased Overlap (RBO) score. 
        l1 -- Ranked List 1
        l2 -- Ranked List 2
    """
    if l1 is None:
        l1 = []
    if l2 is None:
        l2 = []


    sorted_arg = [(len(l1), l1), (len(l2), l2)]

    sl, ll = sorted(sorted_arg)
    s, S = sl
    l, L = ll
    if s == 0:
        return 0

    # Calculate the overlaps at ranks 1 through l 
    # (the longer of the two lists)
    ss = set([])  # contains elements from the smaller list till depth i
    ls = set([])  # contains elements from the longer list till depth i
    x_d = {0: 0}
    sum1 = 0.0

    if debug:
        print(S)
        print(L)

    for i in range(l):
        x = L[i]
        y = S[i] if i < s else None
        d = i + 1

        if debug:
            print('i: {2}\tx: {0}\ty: {1}'.format(str(x), str(y), str(i)))

        # if two elements are same then 
        # we don't need to add to either of the set
        if x == y:
            x_d[d] = x_d[d - 1] + 1.0
        # else add items to respective list
        # and calculate overlap
        else:
            ls.add(x)
            if y is not None:
                ss.add(y)
            x_d[d] = x_d[d - 1] + (1.0 if x in ss else 0.0) + (1.0 if y in ls else 0.0)
            # calculate average overlap
        sum1 += x_d[d] / d * pow(p, d)

    sum2 = 0.0
    for i in range(l - s):
        d = s + i + 1
        sum2 += x_d[d] * (d - s) / (d * s) * pow(p, d)

    sum3 = ((x_d[l] - x_d[s]) / l + x_d[s] / s) * pow(p, l)

    # Equation 32
    rbo_ext = (1 - p) / p * (sum1 + sum2) + sum3

    if debug:
        print(sorted_arg)
        print(S)
        print(L)
        print("sum 1 " + str(sum1) + " sum2 " + str(sum2) + " sum3 " + str(sum3))
        print('SET ls')
        print(ls)
        print('SET ss')
        print(ss)

    return rbo_ext


if __name__ == "__main__":
    list1 = ['0', '1', '2', '3', '4', '5', '6']
    list2 = ['1', '0', '2', '3', '4', '5', '7']
    print(score(list1, list2, p=0.98, debug=True))

    list1 = ['012']
    list2 = []
    print(score(list1, list2, p=0.98))
