#!/usr/bin/env /usr/local/bin/python
"""A more advanced Mapper, using Python iterators and generators."""

import sys
from hlib import *

def mapper(key, value, writer):
    if is_data(value):
        instance = value.strip().split(",")        
        # Count the number of intances with feature Xi with value "vj" in class "clazz"
        clazz = instance[-1]    
        features = instance[:-1]
        writer.emit(clazz, ",".join(features) )


if __name__ == "__main__":
    MainMapReduce().run_mapper(mapper, output_separator="\t")
