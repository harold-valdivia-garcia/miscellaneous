import sys

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
        for key, value in self.hash_emit.items():
            if isinstance(value, list):
                for v in value:
                    print "%s%s%d" % (key, separator, v)
            else:
                print "%s%s%d" % (key, separator, value)

class MainMapReduce:

#    @staticmethod
    def read_input_mapper(self, file):
        # Split the data into lines
        i = 0 
        for line in file:
            i = i + 1
            yield i, line

#    @staticmethod
    def read_mapper_output(self, file, separator='\t'):
        for line in file:
            yield line.rstrip().split(separator, 1)

#    @staticmethod
    def read_reducer_input(self, file, separator="\t"):
        # Get the output of the mapper
        mapper_output = self.read_mapper_output( file, separator) 

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
    def run_reducer(self, reducer,separator="\t"):
        # Get the input for the reducer
        reducer_input = self.read_reducer_input(sys.stdin, separator )

        #print type(reducer_input)
        #print reducer_input

        # Create the writer
        writer = Writer()
        
        # Traverse the key: <values>
        for key, iterator_values in reducer_input:
            reducer(key, iterator_values, writer)

        # Write the results
        writer.write(separator)

#    @staticmethod
    def run_mapper(self, mapper,separator="\t"):
        # Get the input of the mapper
        mapper_input = self.read_input_mapper(sys.stdin) 

        # Create the writer
        writer = Writer()
        
        # Traverse the key: values
        for key, value in mapper_input:
            mapper(key, value, writer)

        # Write the results
        writer.write(separator)
        # print '%s%s%d' % (word, separator, 1)


