#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

import sys
from hlib import *
from math import log
    
def bayes_predict(instance, counter_clazz_feature_value, counter_classes, N):
    # Find priorities
    Pk = { clazz : Nk/N for clazz, Nk in counter_classes.items()}
    for clazz in Pk: print "Prior[{0}] = {1}".format(clazz, Pk[clazz])    
    
    # Find the conditional probabilities P(Xi|C=c)
    list_clazz = counter_classes.keys()
    num_features = len( counter_clazz_feature_value.values()[0] )
    Prob_kiv = ndict()
    for clazz in list_clazz:
        for attr in range(num_features):
            # N(class,attr,val)
            Nkiv = counter_clazz_feature_value[clazz][attr].get( instance[attr], 0 )
            Nk = counter_classes[clazz]
    
            # Use laplacian smoothing for P(Xi = value| c=clazz)
            s = num_features
            Prob_kiv[clazz][attr][instance[attr]] = ( Nkiv + 1.0 )/ (Nk + s)
            #print "P[{0}] = {1}".format((clazz,attr,instance[attr]), Prob_kiv[clazz][attr][instance[attr]])
                    
    # Estimate the class               
    logP = dict()
    for clazz in list_clazz:
        logP[clazz] = log(Pk[clazz])
        for attr in range(num_features):
            logP[clazz] += log( Prob_kiv[clazz][attr][instance[attr]] )
        #print "logP[{0}] = {1} ".format(clazz, logP[clazz])
    
    # Predict       
    #predicted_clazz = max(logP, key = lambda clazz: logP[clazz])
    predicted_clazz = max(logP, key = logP.get)
    #print predicted_clazz
    return predicted_clazz
    

def mapper(key, value, writer):
    input_from_training = value

    # Calculate the frequencies
    counter_clazz_feature_value = ndict()
    counter_classes = ndict()
    
    for line in input_from_training:
        # line = clazz,attr,value	count
        cfv_key, count = line.split("\t",1)
        clazz, attr, attr_val = cfv_key.split(",")
        counter_clazz_feature_value[clazz][int(attr)][attr_val] = float(count)
    
    # Number of N per clazz
    counter_classes = {key :sum(counter_clazz_feature_value[key][0].values()) for key in counter_clazz_feature_value.keys()}

    # Get the total number of 
    N = sum( counter_classes.values() )

    testing_set = load_testing_set()
    
    hits = 0
    for test in testing_set:
        test_instance = test[:-1]
        test_clazz = test[-1]
        print ""
        print test_instance
        pred_clazz = bayes_predict(test_instance, counter_clazz_feature_value, counter_classes, N)
        hits += int(test_clazz == pred_clazz)
    
    print "\nAccuracy:\t{0}".format( len(testing_set)/(hits + 0.0) )
    print "N-test:\t{0}".format( len(testing_set) )
    print "Hits:\t{0}".format(hits)

def load_testing_set():
    raw_data = """rainy,68,80,FALSE,yes
rainy,65,70,TRUE,no
overcast,64,65,TRUE,yes
sunny,72,95,FALSE,no
sunny,69,70,FALSE,yes
rainy,75,80,FALSE,yes
sunny,75,70,TRUE,yes"""
    return [ line.strip().split(",") for line in raw_data.splitlines() ]

        


if __name__ == "__main__":
#    MainMapReduce().run_mapper(mapper, output_separator="\t")
    MainMapReduce().run_mapper_whole_input(mapper, output_separator="\t")
