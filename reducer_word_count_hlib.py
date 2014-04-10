#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys
from hlib import *


def to_int(v):
    try:
        return int(v)
    except ValueError:
        return 0

def reducer(key, iterator_values, writer):
    values = [to_int(v) for v in iterator_values]
    writer.emit(key, sum(values))


if __name__ == "__main__":
    MainMapReduce().run_reducer(reducer, separator="\t")

