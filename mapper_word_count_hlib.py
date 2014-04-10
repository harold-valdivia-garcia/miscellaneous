#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

import sys
from hlib import *

def mapper(key, value, writer):
    line = value
    for word in line.split():
        writer.emit(word, 1)


if __name__ == "__main__":
    MainMapReduce().run_mapper(mapper, separator="\t")

