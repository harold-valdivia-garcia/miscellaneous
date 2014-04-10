#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    #print line
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        if "," in word:
            pass
        else:
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            #
            # tab-delimited; the trivial word count is 1
            print '%s, %s' % (word, 1)
            #pass

