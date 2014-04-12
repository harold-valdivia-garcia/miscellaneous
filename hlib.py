import sys
import operator
from collections import defaultdict

def is_data(strline):
    if strline.strip() == "" or strline.strip()[0] in "@%": 
        return False
    return True

def ndict():
    """Create a multidimensional dictionary"""
    return defaultdict(lambda : defaultdict(dict))

class Writer:
    hash_emit = dict()
    def __init__(self):
        self.has_emit = dict()

    def emit(self, key,value):
        if key not in self.hash_emit:
            self.hash_emit[key] = list()
        list_for_key = self.hash_emit[key]
        list_for_key.append(value)
    
    def write(self, separator="\t"):
        sorted_hash_emit = sorted( self.hash_emit.items(), key=operator.itemgetter(0) )
        for key, value in sorted_hash_emit:
            if isinstance(value, list):
                for v in value:
                    print "%s%s%s" % (key, separator, v)
            else:
                print "%s%s%s" % (key, separator, value)

class MainMapReduce:

    def read_input_with_key_separator(self, file, separator=None):
        if separator is None or separator == "":
            # Split the data into lines
            i = 0 
            for line in file:
                i = i + 1
                yield i, line
        else:
            for line in file:
                yield line.rstrip().split(separator, 1)
                
#    @staticmethod
    def read_input_mapper(self, file):
        # Split the data into lines
        return self.read_input_with_key_separator(file)

#    @staticmethod
    def read_mapper_output(self, file, separator='\t'):
        return self.read_input_with_key_separator(file,separator)

#    @staticmethod
    def read_reducer_input(self, file, separator="\t"):
        # Get the output of the mapper
        #mapper_output = self.read_mapper_output(file, separator) 
        mapper_output = self.read_input_with_key_separator(file,separator)
        
        # Get the input for the reducer
        reducer_input = self.group_by( mapper_output )
        return list(reducer_input)


#    @staticmethod
    def group_by(self, list_of_pairs):
        table = dict()
        for k,v in list_of_pairs:
            if k not in table:
                table[k] = list()
            table[k].append(v)
        return table.items()


#    @staticmethod
    def run_reducer(self, reducer, input_separator="\t", output_separator="\t"):
        # Get the input for the reducer
        reducer_input = self.read_reducer_input(sys.stdin, input_separator )

        #print type(reducer_input)
        #print reducer_input

        # Create the writer
        writer = Writer()
        
        # Traverse the key: <values>
        for key, iterator_values in reducer_input:
            reducer(key, iterator_values, writer)

        # Write the results
        writer.write(output_separator)

#    @staticmethod
    def run_mapper(self, mapper, input_separator=None, output_separator="\t"):
        # Get the input of the mapper
        #mapper_input = self.read_input_mapper(sys.stdin)
        mapper_input = self.read_input_with_key_separator(sys.stdin, input_separator)

        # Create the writer
        writer = Writer()
        
        # Traverse the key: values
        for key, value in mapper_input:
            mapper(key, value, writer)

        # Write the results
        writer.write(output_separator)
        # print '%s%s%d' % (word, separator, 1)
        
    def run_mapper_whole_input(self, mapper, output_separator="\t"):
        # Create the writer
        writer = Writer()
        
        # Traverse the key: values
        mapper(key=None, value=sys.stdin, writer= writer)

        # Write the results
        writer.write(output_separator)
        # print '%s%s%d' % (word, separator, 1)


