#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys
from hlib import *


def to_instance_without_class(v):
    return v.strip().split(",")

def reducer(key, iterator_values, writer):
    clazz = key
    list_instances = [to_instance_without_class(v) for v in iterator_values]
    
    counter_clazz_feature_value = ndict()
    for instance in list_instances:       
        num_features = len(instance)
        # Count the number of intances with feature Xi 
        # with value "vj" in class "clazz"        
        for attr in range(num_features):
            cfv_key = (clazz, attr, instance[attr])
            counter_clazz_feature_value[ cfv_key ] = 1.0 + counter_clazz_feature_value.get(cfv_key, 0)
    
    # Emit the frequencies
    for cfv_key, count in counter_clazz_feature_value.items():
        cfv_key_as_str = ",".join(map(str, cfv_key))
        writer.emit( cfv_key_as_str, count )
    
        
if __name__ == "__main__":
    #print sys.stdin.readlines()
    
    MainMapReduce().run_reducer(reducer, input_separator="\t")
