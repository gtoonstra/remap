import remap
from operator import itemgetter

# --- create file i/o objects to be used ----
def create_mapper_reader( filename ):
    return remap.TextFileReader( filename )

def create_mapper_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid, combiner=None, customkey=itemgetter(3) )

def create_reducer_reader( inputdir ):
    return remap.TextPartFileReader( inputdir )

def create_reducer_writer( outputdir, partition ):
    return remap.TextReduceWriter( outputdir, partition )

# ---- map and reduce implementations ----

# map just creates one record of the word and a '1' to count it,
# it also directs the mapped value to a specific partition
def map( key, value ):
    print( key, value )
    words = value.split(',')
    part = words[2].replace(" ", "_")
    yield part, tuple(words), ""

# The reduce operation sums all the values in the sequence and outputs.
def reduce( key, value ):
    yield (key, value)

