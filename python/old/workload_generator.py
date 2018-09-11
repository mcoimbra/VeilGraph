import random
import sys

# This module sends the contents of the file in sys.argv[1] to the output file in sys.argv[2].
# For every copied line, there is a probability sys.argv[3] that a query "Q " will be inserted as well.

prob = float(sys.argv[3])

with open(sys.argv[1]) as original:
    with open(sys.argv[2], 'w') as dest:
        for l in original:
            print(l, file=dest, end='')
            if random.random() <= prob:
                print("Q ", file=dest)
